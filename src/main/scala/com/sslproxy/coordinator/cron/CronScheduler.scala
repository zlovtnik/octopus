package com.sslproxy.coordinator.cron

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import com.sslproxy.coordinator.config.{CronConfig, IngestConfig}
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.dispatch.BatchDispatchService.DispatchResult
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.persistence.{
  IngestionStore,
  MaintenanceStore,
  OutboxStore,
  ProjectionStore
}
import com.sslproxy.coordinator.tidb.TidbErrorClass
import com.sslproxy.coordinator.processor.{FencedWorkRunner, ProcessorId}
import fs2.Stream
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

final class CronScheduler private (
    cfg: CronConfig,
    ingestConfig: IngestConfig,
    ingestionStore: IngestionStore[IO],
    outboxStore: OutboxStore[IO],
    projectionStore: ProjectionStore[IO],
    maintenanceStore: MaintenanceStore[IO],
    backpressureService: BackpressureService,
    batchDispatchService: BatchDispatchService,
    metrics: CoordinatorMetrics,
    verifyCanonicalManifest: IO[Unit],
    loopCounter: Ref[IO, Long],
    lastShadowAuditMs: Ref[IO, Long],
    workRunner: FencedWorkRunner[IO]
):
  import CronScheduler.log

  val schemaRefresher: Stream[IO, Unit] =
    Stream.awakeEvery[IO](cfg.schemaRefreshIntervalSeconds.seconds).evalMap { _ =>
      CronScheduler.verifyCanonicalManifestWithRetry(
        verifyCanonicalManifest,
        cfg.scanRetryBackoffSeconds.max(1).seconds
      )
    }

  val jobPlanningStream: Stream[IO, Unit] =
    fencedPeriodicStream(ProcessorId.SyncJobPlanner, cfg.idleSleepMs.millis)(processIngest())

  val backlogRecoveryStream: Stream[IO, Unit] =
    fencedPeriodicStream(ProcessorId.SyncBacklogRecovery, cfg.idleSleepMs.millis)(recoverStaleBatches())

  val loadDispatchStream: Stream[IO, Unit] =
    fencedPeriodicStream(ProcessorId.SyncLoadDispatch, cfg.idleSleepMs.millis) {
      ingestionStore.prepareLoadDispatch(
        ingestConfig.loadStreamNames,
        cfg.batchMaxAttempts,
        cfg.ingestBatchSize
      ).value.flatMap {
        case Right(_) => IO.unit
        case Left(error) => IO.raiseError(databaseFailure(error))
      }
    }

  val outboxPublisherStream: Stream[IO, Unit] =
    Stream.awakeEvery[IO](cfg.idleSleepMs.millis).evalMap { _ =>
      dispatchBatches()
    }

  val rfAlertStream: Stream[IO, Unit] =
    fencedPeriodicStream(ProcessorId.RfAlertProjector, 10.seconds)(shadowAudit())

  val wirelessFrameNormalizerStream: Stream[IO, Unit] =
    fencedPeriodicStream(ProcessorId.WirelessFrameNormalizer,cfg.idleSleepMs.millis) {
        projectionStore.normalizeWirelessFrames(cfg.ingestBatchSize).value.flatMap {
          case Right(_) => IO.unit
          case Left(error) => IO.raiseError(databaseFailure(error))
      }
    }

  val wirelessInventoryProjectorStream: Stream[IO, Unit] =
    periodicDatabaseStream(ProcessorId.WirelessInventoryProjector, 10.seconds) {
      projectionStore.projectWirelessInventory(cfg.ingestBatchSize).value
    }

  def searchDocumentBuilderStream(
      batchSize: Int,
      interval: FiniteDuration
  ): Stream[IO, Unit] =
    periodicDatabaseStream(ProcessorId.EmbeddingTextBuilder, interval) {
      projectionStore.buildSearchDocuments(batchSize).value
    }

  def embeddingJobPreparerStream(
      batchSize: Int,
      interval: FiniteDuration,
      embeddingModel: String
  ): Stream[IO, Unit] =
    periodicDatabaseStream(ProcessorId.EmbeddingPreparer, interval) {
      projectionStore.prepareEmbeddingJobs(batchSize, embeddingModel).value
    }

  def staleWorkerCleanupStream(
      batchSize: Int,
      interval: FiniteDuration
  ): Stream[IO, Unit] =
    periodicDatabaseStream(ProcessorId.StaleWorkerCleanup, interval) {
      maintenanceStore.cleanupStaleWorkers(batchSize).value
    }

  def scheduledReconciliationStream(
      batchSize: Int,
      interval: FiniteDuration
  ): Stream[IO, Unit] =
    periodicDatabaseStream(ProcessorId.ScheduledReconciliation, interval) {
      maintenanceStore.reconcileWirelessProjections(batchSize).value
    }

  def behaviorProjectorStream(batchSize: Int, interval: FiniteDuration): Stream[IO, Unit] =
    projectionStream(ProcessorId.BehaviorProjector, interval, projectionStore.projectBehavior(batchSize).value)

  def timingProjectorStream(batchSize: Int, interval: FiniteDuration): Stream[IO, Unit] =
    projectionStream(ProcessorId.TimingProjector, interval, projectionStore.projectTiming(batchSize).value)

  def baselineProjectorStream(batchSize: Int, interval: FiniteDuration): Stream[IO, Unit] =
    projectionStream(ProcessorId.BaselineProjector, interval, projectionStore.projectBaselines(batchSize).value)

  def sequenceProjectorStream(batchSize: Int, interval: FiniteDuration): Stream[IO, Unit] =
    projectionStream(ProcessorId.SequenceProjector, interval, projectionStore.projectSequences(batchSize).value)

  def similarityProjectorStream(
      batchSize: Int,
      interval: FiniteDuration,
      eventDuplicateDistance: Double,
      behaviorSimilarityThreshold: Double,
      sequenceDistanceThreshold: Double
  ): Stream[IO, Unit] =
    projectionStream(
      ProcessorId.SimilarityProjector,
      interval,
      projectionStore.projectSimilarities(
        batchSize,
        eventDuplicateDistance,
        behaviorSimilarityThreshold,
        sequenceDistanceThreshold
      ).value
    )

  def clusteringProjectorStream(
      batchSize: Int,
      interval: FiniteDuration,
      minimumSimilarity: Double
  ): Stream[IO, Unit] =
    projectionStream(
      ProcessorId.ClusteringProjector,
      interval,
      projectionStore.projectClusterCandidates(batchSize, minimumSimilarity).value
    )

  def identityProjectorStream(batchSize: Int, interval: FiniteDuration): Stream[IO, Unit] =
    projectionStream(ProcessorId.WirelessIdentityProjector, interval, projectionStore.projectApprovedIdentities(batchSize).value)

  def graphProjectorStream(batchSize: Int, interval: FiniteDuration): Stream[IO, Unit] =
    projectionStream(ProcessorId.GraphProjector, interval, projectionStore.projectInfrastructureGraph(batchSize).value)

  def dnsAlertProjectorStream(batchSize: Int, interval: FiniteDuration): Stream[IO, Unit] =
    projectionStream(ProcessorId.DnsAlertProjector, interval, projectionStore.projectDnsThreats(batchSize).value)

  def riskProjectorStream(batchSize: Int, interval: FiniteDuration): Stream[IO, Unit] =
    projectionStream(ProcessorId.RiskProjector, interval, projectionStore.projectRisk(batchSize).value)

  private def projectionStream(
      processorId: ProcessorId,
      interval: FiniteDuration,
      operation: => IO[Either[com.sslproxy.coordinator.domain.DatabaseError, Int]]
  ): Stream[IO, Unit] =
    periodicDatabaseStream(processorId, interval)(operation)

  private def periodicDatabaseStream(
      processorId: ProcessorId,
      interval: FiniteDuration
  )(
      operation: => IO[Either[com.sslproxy.coordinator.domain.DatabaseError, Int]]
  ): Stream[IO, Unit] =
    fencedPeriodicStream(processorId, interval) {
      operation.flatMap {
        case Right(_) => IO.unit
        case Left(error) => IO.raiseError(databaseFailure(error))
      }
    }

  private def fencedPeriodicStream(
      processorId: ProcessorId,
      interval: FiniteDuration
  )(
      operation: => IO[Unit]
  ): Stream[IO, Unit] =
    Stream.awakeEvery[IO](interval).evalMap { _ =>
        val leaseTtl = (interval * 2L).max(30.seconds)
        workRunner.runOnce(processorId, leaseTtl)(_ => operation).void
    }

  val supportStream: Stream[IO, Unit] =
    val backpressureStream = Stream
      .awakeEvery[IO](cfg.idleSleepMs.millis)
      .evalMap { _ =>
          backpressureService.checkAndAct.void.handleErrorWith { err =>
          IO(log.error("cron_backpressure", err, "status" -> "failed"))
        }
      }

    val metricsStream = Stream
      .awakeEvery[IO](cfg.heartbeatLogIntervalMs.millis)
      .evalMap { _ =>
        (metrics.heartbeat() >> IO(metrics.incrementLoopCounter()) >> loopCounter.update(_ + 1))
          .handleErrorWith { err =>
            IO(log.error("cron_metrics", err, "status" -> "failed"))
          }
      }

    backpressureStream.merge(metricsStream)

  val mainLoop: Stream[IO, Unit] =
    supportStream
      .merge(resilient(jobPlanningStream, "cron_ingest"))
      .merge(resilient(backlogRecoveryStream, "cron_recovery"))
      .merge(resilient(loadDispatchStream, "cron_load_dispatch"))
      .merge(resilient(outboxPublisherStream, "cron_dispatch"))
      .merge(resilient(rfAlertStream, "cron_shadow_audit"))

  private def resilient(stream: Stream[IO, Unit], event: String): Stream[IO, Unit] =
    Stream
      .unfoldEval(()) { state =>
    stream.compile.drain.attempt.flatMap {
          case Right(_) => IO.pure(None)
          case Left( error) =>IO(log.error(event, error, "status" -> "failed"))
              .as(Some(((), state)))
    }
      }
      .drain

  private def processIngest(): IO[Unit] =
    val budget = backpressureService.budget

    ingestionStore.pendingCount.value.flatMap {
      case Left(err) =>
        IO(log.warn("ingest_ledger", "status" -> "pending_count_failed",
          "operation" -> err.operation, "error" -> err.message)) *>
          IO(metrics.recordIngestInvocation(false)) *>
          IO.raiseError(databaseFailure(err))

      case Right(pendingCount) =>
        val logPending = IO(log.info("ingest_ledger", "status" -> "pending", "count" -> pendingCount.toString))
        val throttleCheck = if pendingCount >= budget then
          IO(log.info("backpressure", "status" -> "throttled",
            "pending_count" -> pendingCount.toString, "budget" -> budget.toString,
            "ingest_batch_size" -> cfg.ingestBatchSize.toString))
        else IO.unit

        logPending *> throttleCheck *>
          ingestionStore.processPending(
            ingestConfig.streamNames,
            cfg.scanMaxAttempts,
            cfg.scanRetryBackoffSeconds,
            cfg.ingestBatchSize
          ).value.flatMap {
            case Left(err) =>
              IO(log.error("ingest_ledger", "status" -> "failed",
                "operation" -> err.operation, "error" -> err.message)) *>
                IO(metrics.recordIngestInvocation(false)) *>
                IO.raiseError(databaseFailure(err))

            case Right(processed) =>
              IO(metrics.recordIngestInvocation(true)) *>
                IO(metrics.recordIngestProcessed(processed)) *>
                IO.whenA(processed > 0)(
                  IO(log.info("ingest_ledger", "status" -> "processed", "count" -> processed.toString))
                )
          }
    }

  private def recoverStaleBatches(): IO[Unit] =
    outboxStore.recoverExpired.value.flatMap {
      case Left(err) =>
        IO(log.error("outbox_lease_recovery", "status" -> "failed",
          "operation" -> err.operation, "error" -> err.message)) *>
          IO.raiseError(databaseFailure(err))
      case Right(count) =>
        IO.whenA(count > 0)(
          IO(log.info("outbox_lease_recovery", "status" -> "recovered", "count" -> count.toString))
        )
    }

  private def dispatchBatches(): IO[Unit] =
    // Ordered outbox UPDATE claims contend on the same leading row/range.
    // Drain sequentially so claim transactions cannot occupy the whole pool.
    CronScheduler.drainBatch(cfg.dispatchBatchSize) { () =>
      batchDispatchService.dispatchNext()
    }.void

  private def shadowAudit(): IO[Unit] =
    val intervalMs = 10_000L

    lastShadowAuditMs.get.flatMap { lastMs =>
      val now = System.currentTimeMillis()
      if now - lastMs < intervalMs then IO.unit
      else
        projectionStore.generateRfAlerts.value.flatMap {
          case Left(err) =>
            IO(log.error("shadow_audit", "status" -> "failed",
              "operation" -> err.operation, "error" -> err.message)) *>
              lastShadowAuditMs.set(now) *>
              IO.raiseError(databaseFailure(err))

          case Right(alerts) =>
            IO.whenA(alerts.nonEmpty)(
              IO(log.info("shadow_audit", "status" -> "alerts_generated",
                "count" -> alerts.size.toString))
            ) *> lastShadowAuditMs.set(now)
        }
    }

  private def databaseFailure(error: com.sslproxy.coordinator.domain.DatabaseError): RuntimeException =
    RuntimeException(s"${error.operation}: ${error.message}", error.cause)

object CronScheduler:
  private val log = StructuredLogger(getClass)

  def create(
      cfg: CronConfig,
      ingestConfig: IngestConfig,
      ingestionStore: IngestionStore[IO],
      outboxStore: OutboxStore[IO],
      projectionStore: ProjectionStore[IO],
      maintenanceStore: MaintenanceStore[IO],
      backpressureService: BackpressureService,
      batchDispatchService: BatchDispatchService,
      metrics: CoordinatorMetrics,
      verifyCanonicalManifest: IO[Unit]
  ): IO[CronScheduler] =
    for
      loopCounter <- Ref.of[IO, Long](0L)
      lastShadowAuditMs <- Ref.of[IO, Long](0L)
      ownerId <- IO(java.util.UUID.randomUUID().toString)
    yield new CronScheduler(
      cfg,
      ingestConfig,
      ingestionStore,
      outboxStore,
      projectionStore,
      maintenanceStore,
      backpressureService,
      batchDispatchService,
      metrics,
      verifyCanonicalManifest,
      loopCounter,
      lastShadowAuditMs,
      new FencedWorkRunner(maintenanceStore, ownerId)
    )

  private[cron] def drainBatch(
      maxDispatches: Int
  )(dispatchNext: () => IO[DispatchResult]): IO[Int] =
    def loop(remaining: Int, dispatched: Int): IO[Int] =
      if remaining <= 0 then IO.pure(dispatched)
      else
        dispatchNext().flatMap {
          case DispatchResult.Dispatched =>
            loop(remaining - 1, dispatched + 1)
          case DispatchResult.ContinueDraining =>
            loop(remaining - 1, dispatched)
          case DispatchResult.NoWork | DispatchResult.StopDraining =>
            IO.pure(dispatched)
        }

    loop(maxDispatches.max(0), 0)

  private[cron] def verifyCanonicalManifestWithRetry(
      verify: IO[Unit],
      retryDelay: FiniteDuration
  ): IO[Unit] =
    verify.handleErrorWith { error =>
      TidbErrorClass.classify(error) match
        case TidbErrorClass.Retryable =>
          IO(log.warn(
            "canonical_manifest_verification",
            "status" -> "retrying",
            "delay_ms" -> retryDelay.toMillis.toString,
            "error" -> Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
          )) *> IO.sleep(retryDelay) *> verifyCanonicalManifestWithRetry(verify, retryDelay)
        case TidbErrorClass.Permanent =>
          IO(log.error("canonical_manifest_verification", error, "status" -> "failed")) *>
            IO.raiseError(error)
    }
