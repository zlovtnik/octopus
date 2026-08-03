package com.sslproxy.coordinator.cron

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.{CronConfig, IngestConfig}
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.dispatch.BatchDispatchService.DispatchResult
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.tidb.TidbRepository
import fs2.Stream
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

final class CronScheduler private (
    cfg: CronConfig,
    ingestConfig: IngestConfig,
    repo: TidbRepository,
    backpressureService: BackpressureService,
    batchDispatchService: BatchDispatchService,
    metrics: CoordinatorMetrics,
    verifyCanonicalManifest: IO[Unit],
    dbSemaphore: Semaphore[IO],
    loopCounter: Ref[IO, Long],
    lastShadowAuditMs: Ref[IO, Long]
):
  import CronScheduler.log

  val schemaRefresher: Stream[IO, Unit] =
    Stream.awakeEvery[IO](cfg.schemaRefreshIntervalSeconds.seconds).evalMap { _ =>
      verifyCanonicalManifest.handleErrorWith { error =>
        IO(log.error("canonical_manifest_verification", error, "status" -> "failed")) *>
          IO.raiseError(error)
      }
    }

  val jobPlanningStream: Stream[IO, Unit] =
    Stream.awakeEvery[IO](cfg.idleSleepMs.millis).evalMap { _ =>
      dbSemaphore.permit.use(_ => processIngest())
    }

  val backlogRecoveryStream: Stream[IO, Unit] =
    Stream.awakeEvery[IO](cfg.idleSleepMs.millis).evalMap { _ =>
      dbSemaphore.permit.use(_ => recoverStaleBatches())
    }

  val loadDispatchStream: Stream[IO, Unit] =
    Stream.awakeEvery[IO](cfg.idleSleepMs.millis).evalMap { _ =>
      dbSemaphore.permit.use { _ =>
        repo.prepareLoadDispatch(
          ingestConfig.loadStreamNames,
          cfg.batchMaxAttempts,
          cfg.ingestBatchSize
        ).flatMap {
          case Right(_) => IO.unit
          case Left(error) => IO.raiseError(databaseFailure(error))
        }
      }
    }

  val outboxPublisherStream: Stream[IO, Unit] =
    Stream.awakeEvery[IO](cfg.idleSleepMs.millis).evalMap { _ =>
      dispatchBatches()
    }

  val rfAlertStream: Stream[IO, Unit] =
    Stream.awakeEvery[IO](10.seconds).evalMap { _ =>
      dbSemaphore.permit.use(_ => shadowAudit())
    }

  val supportStream: Stream[IO, Unit] =
    val backpressureStream = Stream
      .awakeEvery[IO](cfg.idleSleepMs.millis)
      .evalMap { _ =>
        dbSemaphore.permit.use { _ =>
          backpressureService.checkAndAct.void
        }.handleErrorWith { err =>
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
    stream.handleErrorWith { error =>
      Stream.eval(IO(log.error(event, error, "status" -> "failed"))) ++ resilient(stream, event)
    }

  private def processIngest(): IO[Unit] =
    val budget = backpressureService.budget

    repo.pendingLedgerCount().flatMap {
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
          repo.processIngestLedger(
            ingestConfig.streamNames,
            ingestConfig.loadStreamNames,
            cfg.scanMaxAttempts,
            cfg.scanRetryBackoffSeconds,
            cfg.ingestBatchSize
          ).flatMap {
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
    repo.recoverExpiredOutboxLeases().flatMap {
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
        repo.generateShadowAlerts().flatMap {
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
      repo: TidbRepository,
      backpressureService: BackpressureService,
      batchDispatchService: BatchDispatchService,
      metrics: CoordinatorMetrics,
      verifyCanonicalManifest: IO[Unit],
      dbSemaphore: Semaphore[IO]
  ): IO[CronScheduler] =
    for
      loopCounter <- Ref.of[IO, Long](0L)
      lastShadowAuditMs <- Ref.of[IO, Long](0L)
    yield new CronScheduler(
      cfg,
      ingestConfig,
      repo,
      backpressureService,
      batchDispatchService,
      metrics,
      verifyCanonicalManifest,
      dbSemaphore,
      loopCounter,
      lastShadowAuditMs
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
