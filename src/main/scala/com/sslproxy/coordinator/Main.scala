package com.sslproxy.coordinator

import cats.effect.{IO, IOApp, Resource}
import cats.effect.kernel.Fiber
import cats.effect.std.Semaphore
import com.comcast.ip4s.*
import fs2.Stream
import com.sslproxy.coordinator.archive.MinioPayloadArchive
import com.sslproxy.coordinator.config.{AppConfig, RuntimeConfig}
import com.sslproxy.coordinator.cron.CronScheduler
import com.sslproxy.coordinator.cutover.CutoverArtifactLoader
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.http.HealthRoutes
import com.sslproxy.coordinator.ingest.{PayloadAuditConsumer, SyncEventHydrationService}
import com.sslproxy.coordinator.kafka.{
  KafkaComponents,
  ScanRequestStream,
  TidbLoadStream,
  TidbResultStream,
  WirelessConsumerService
}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, CoordinatorTracing}
import com.sslproxy.coordinator.processor.{
  EventRetentionProcessor,
  PayloadArchiver,
  ProcessorId,
  ProcessorSupervisor,
  ProcessorWorkload,
  SearchRetentionProcessor
}
import com.sslproxy.coordinator.tidb.*
import doobie.Transactor
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.http4s.ember.server.EmberServerBuilder
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

object Main extends IOApp.Simple:
  private val log = StructuredLogger(getClass)

  override def run: IO[Unit] =
    val cfg = AppConfig.load

    val blockingEc: scala.concurrent.ExecutionContext =
      scala.concurrent.ExecutionContext.fromExecutor(
        java.util.concurrent.Executors.newFixedThreadPool(
          cfg.tidb.poolSize,
          new java.util.concurrent.ThreadFactory:
            def newThread(r: Runnable): Thread =
              val t = new Thread(r, "doobie-tidb-pool")
              t.setDaemon(true)
              t
        )
      )
    val meterRegistry = new SimpleMeterRegistry()
    val metrics = new CoordinatorMetrics(meterRegistry)

    if !cfg.tidb.enabled then
      log.warn("startup", "status" -> "disabled", "tidb_sink" -> "disabled")
      IO.println("TiDB sink disabled (set TIDB_ENABLED=true to enable)").void
    else
      val appResource: Resource[IO, Fiber[IO, Throwable, Unit]] =
        CoordinatorTracing.resource.flatMap { _ =>
          TidbTransactor.resource(cfg.tidb).flatMap { oldTx =>
            val tiDbDs = oldTx.dataSource
            val tiDbDoobieTx = Transactor.fromDataSource[IO](tiDbDs, blockingEc)
            val preflight = new TidbSchemaPreflight(oldTx, cfg.tidb)
            val tiDbRepo = new TidbRepository(tiDbDoobieTx)

            Resource.eval(preflight.validate()).flatMap { _ =>
              val enabledProcessorIds = cfg.processors.enabled.flatMap(ProcessorId.fromString(_).toOption).toSet
              val lockedConsumerProcessorIds = Set(
                ProcessorId.SyncScanIngestion,
                ProcessorId.SyncLoadConsumer,
                ProcessorId.SyncResultConsumer
              )
              val cutoverRequired =
                cfg.runtime.consumersEnabled || enabledProcessorIds.exists(lockedConsumerProcessorIds.contains)
              val artifactIO =
                if cutoverRequired then
                  if cfg.cutover.devBypass then
                    IO(log.warn("startup", "status" -> "cutover_dev_bypass")) *>
                      cats.effect
                        .Clock[IO]
                        .realTimeInstant
                        .map(t =>
                          Some(
                            com.sslproxy.coordinator.cutover.VerifiedCutoverArtifact
                              .devBypass(cfg.cutover.expectedClusterId, t)
                          )
                        )
                  else CutoverArtifactLoader.loadAndVerify[IO](cfg.cutover).map(Some(_))
                else IO.pure(None)

              Resource.eval(artifactIO).flatMap { artifactOpt =>
                Resource
                  .eval(
                    Semaphore[IO](
                      dbWorkerPermits(
                        cfg.tidb.poolSize,
                        cfg.tidb.healthcheckReserve
                      )
                    )
                  )
                  .flatMap { dbSemaphore =>
                    KafkaComponents.resource(cfg.kafka).flatMap { kafka =>
                      val payloadResolver = new TidbPayloadResolver(cfg.sync.outboxDir)
                      val handler = new TidbLoadHandler(payloadResolver, TidbTransformService, oldTx, TidbClock)
                      val ingestionStore = new TidbIngestionStore(tiDbRepo)
                      val outboxStore = new TidbOutboxStore(tiDbRepo)
                      val projectionStore = new TidbProjectionStore(tiDbRepo)
                      val maintenanceStore = new TidbMaintenanceStore(tiDbRepo)
                      val resultStore = new TidbResultStore(tiDbRepo)
                      val wirelessStore = new TidbWirelessStore(tiDbRepo)
                      val hydrationService = new SyncEventHydrationService(
                        ingestionStore,
                        payloadResolver,
                        metrics,
                        cfg.cron.scanFetchCount,
                        dbSemaphore
                      )

                      val batchDispatchService = new BatchDispatchService(
                        outboxStore,
                        kafka.producer,
                        metrics,
                        java.util.UUID.randomUUID().toString,
                        List(cfg.kafka.loadTopic, cfg.kafka.resultTopic),
                        cfg.cron.batchDispatchLeaseSeconds,
                        cfg.cron.scanRetryBackoffSeconds,
                        cfg.cron.batchDispatchRetryMaxSeconds,
                        dbSemaphore
                      )

                      val scheduler = for
                        backpressureService <- BackpressureService.create(
                          cfg.backpressure,
                          cfg.cron.ingestBatchSize,
                          ingestionStore.pendingCount.value,
                          metrics
                        )
                        cronScheduler <- CronScheduler.create(
                          cfg.cron,
                          cfg.ingest,
                          ingestionStore,
                          outboxStore,
                          projectionStore,
                          maintenanceStore,
                          backpressureService,
                          batchDispatchService,
                          metrics,
                          preflight.validate(),
                          dbSemaphore
                        )
                      yield cronScheduler

                      Resource.eval(scheduler).flatMap { cronScheduler =>
                        val processorStateStore = new TidbProcessorStateStore(tiDbDoobieTx)
                        val maintenanceOwnerId = java.util.UUID.randomUUID().toString
                        val leaseTtlSeconds = ((cfg.archive.maintenanceIntervalMs / 1000L) * 2L)
                          .max(60L)
                          .min(Int.MaxValue.toLong)
                          .toInt
                        val searchRetentionProcessor = new SearchRetentionProcessor(
                          maintenanceStore,
                          maintenanceOwnerId,
                          cfg.archive.searchRetentionDays,
                          cfg.archive.batchSize,
                          leaseTtlSeconds
                        )
                        val eventRetentionResource =
                          if enabledProcessorIds.contains(ProcessorId.EventRetention) then
                            MinioPayloadArchive.resource(cfg.archive).map { archive =>
                              val archiver = new PayloadArchiver(
                                maintenanceStore,
                                archive,
                                cfg.archive.hotDays,
                                cfg.archive.batchSize
                              )
                              Some(
                                new EventRetentionProcessor(
                                  maintenanceStore,
                                  archiver,
                                  maintenanceOwnerId,
                                  cfg.archive.eventRetentionDays,
                                  cfg.archive.tombstoneRetentionDays,
                                  cfg.archive.batchSize,
                                  leaseTtlSeconds
                                )
                              )
                            }
                          else Resource.pure[IO, Option[EventRetentionProcessor[IO]]](None)

                        eventRetentionResource.flatMap { eventRetentionProcessor =>
                          Resource.eval(ProcessorSupervisor.create(
                            cfg.processors,
                            processorStateStore,
                            Some(metrics)
                          )).flatMap {
                            supervisor =>
                              val healthRoutes = new HealthRoutes(
                                oldTx,
                                metrics,
                                Some(supervisor.readiness),
                                cfg.tidb.connectionTimeoutMs.millis
                              )

                              val httpPort = Port
                                .fromInt(cfg.http.port)
                                .getOrElse(
                                  sys.error(s"Port ${cfg.http.port} validated by config but IP4s rejected it")
                                )
                              val serverResource = EmberServerBuilder
                                .default[IO]
                                .withPort(httpPort)
                                .withHost(host"0.0.0.0")
                                .withHttpApp(healthRoutes.routes.orNotFound)
                                .build

                              serverResource.flatMap { _ =>
                                val payloadAuditStream = PayloadAuditConsumer.stream(
                                  cfg.kafka,
                                  ingestionStore,
                                  metrics,
                                  kafka.producer,
                                  dbSemaphore
                                )
                                val wirelessStreams = WirelessConsumerService.allStreams(
                                  cfg.wireless,
                                  cfg.kafka,
                                  wirelessStore,
                                  kafka.producer,
                                  dbSemaphore
                                )

                                val (scanStream, loadStream, resultStream) = artifactOpt match
                                  case Some(artifact) =>
                                    (
                                      ScanRequestStream.run(
                                        cfg.kafka,
                                        artifact,
                                        ingestionStore,
                                        payloadResolver,
                                        metrics,
                                        kafka.producer,
                                        dbSemaphore
                                      ),
                                      TidbLoadStream.run(
                                        cfg.kafka,
                                        artifact,
                                        ingestionStore,
                                        resultStore,
                                        handler,
                                        kafka.producer,
                                        dbSemaphore
                                      ),
                                      TidbResultStream.run(
                                        cfg.kafka,
                                        artifact,
                                        ingestionStore,
                                        resultStore,
                                        kafka.producer,
                                        dbSemaphore
                                      )
                                    )
                                  case None =>
                                    (Stream.empty, Stream.empty, Stream.empty)

                                val workloads = List(
                                  ProcessorWorkload(ProcessorId.SyncScanIngestion, scanStream),
                                  ProcessorWorkload(
                                    ProcessorId.SyncJobPlanner,
                                    cronScheduler.jobPlanningStream,
                                    startup = hydrationService.runOnce.compile.drain
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.SyncBacklogRecovery,
                                    cronScheduler.backlogRecoveryStream
                                  ),
                                  ProcessorWorkload(ProcessorId.SyncLoadDispatch, cronScheduler.loadDispatchStream),
                                  ProcessorWorkload(ProcessorId.SyncLoadConsumer, loadStream),
                                  ProcessorWorkload(ProcessorId.SyncResultConsumer, resultStream),
                                  ProcessorWorkload(
                                    ProcessorId.SyncOutboxPublisher,
                                    cronScheduler.outboxPublisherStream
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.WirelessFrameNormalizer,
                                    cronScheduler.wirelessFrameNormalizerStream
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.WirelessInventoryProjector,
                                    cronScheduler.wirelessInventoryProjectorStream
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.WirelessIdentityProjector,
                                    cronScheduler.identityProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.EmbeddingTextBuilder,
                                    cronScheduler.searchDocumentBuilderStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.EmbeddingPreparer,
                                    cronScheduler.embeddingJobPreparerStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds,
                                      cfg.processors.embeddingModel
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.BehaviorProjector,
                                    cronScheduler.behaviorProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.TimingProjector,
                                    cronScheduler.timingProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.BaselineProjector,
                                    cronScheduler.baselineProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.SequenceProjector,
                                    cronScheduler.sequenceProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.SimilarityProjector,
                                    cronScheduler.similarityProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds,
                                      cfg.processors.eventDuplicateDistance,
                                      cfg.processors.behaviorSimilarityThreshold,
                                      cfg.processors.sequenceDistanceThreshold
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.ClusteringProjector,
                                    cronScheduler.clusteringProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds,
                                      cfg.processors.behaviorSimilarityThreshold
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.GraphProjector,
                                    cronScheduler.graphProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.DnsAlertProjector,
                                    cronScheduler.dnsAlertProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.RiskProjector,
                                    cronScheduler.riskProjectorStream(
                                      cfg.processors.batchSize,
                                      cfg.processors.intervalSeconds.seconds
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.SearchRetention,
                                    Stream
                                      .awakeEvery[IO](cfg.archive.maintenanceIntervalMs.millis)
                                      .evalMap(_ => searchRetentionProcessor.runOnce)
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.StaleWorkerCleanup,
                                    cronScheduler.staleWorkerCleanupStream(
                                      cfg.processors.batchSize,
                                      cfg.archive.maintenanceIntervalMs.millis
                                    )
                                  ),
                                  ProcessorWorkload(
                                    ProcessorId.ScheduledReconciliation,
                                    cronScheduler.scheduledReconciliationStream(
                                      cfg.processors.batchSize,
                                      cfg.archive.maintenanceIntervalMs.millis
                                    )
                                  ),
                                  ProcessorWorkload(ProcessorId.RfAlertProjector, cronScheduler.rfAlertStream)
                                ) ++ eventRetentionProcessor.toList.map { processor =>
                                  ProcessorWorkload(
                                    ProcessorId.EventRetention,
                                    Stream
                                      .awakeEvery[IO](cfg.archive.maintenanceIntervalMs.millis)
                                      .evalMap(_ => processor.runOnce)
                                  )
                                }

                                val legacyProcessorStreams =
                                  cronScheduler.mainLoop
                                    .merge(cronScheduler.schemaRefresher)
                                    .merge(hydrationService.runOnce.handleErrorWith { error =>
                                      Stream.eval(
                                        IO(
                                          log.error(
                                            "sync_event_hydration_backfill",
                                            error,
                                            "status" -> "failed"
                                          )
                                        )
                                      )
                                    })
                                val processorStreams =
                                  if cfg.processors.enabled.isEmpty then legacyProcessorStreams
                                  else
                                    cronScheduler.supportStream
                                      .merge(cronScheduler.schemaRefresher)
                                      .merge(supervisor.run(workloads))

                                artifactOpt match
                                  case Some(artifact) =>
                                    log.info(
                                      "startup",
                                      "status" -> "consumers_enabled",
                                      "artifact_version" -> artifact.artifact.schemaVersion.toString,
                                      "cluster_id" -> artifact.artifact.clusterId,
                                      "scan_topic" -> cfg.kafka.scanTopic,
                                      "scan_group" -> cfg.kafka.scanConsumer,
                                      "load_topic" -> cfg.kafka.loadTopic,
                                      "load_group" -> cfg.kafka.loadConsumer,
                                      "result_topic" -> cfg.kafka.resultTopic,
                                      "result_group" -> cfg.kafka.resultConsumer
                                    )
                                  case None =>
                                    log.info("startup", "status" -> "consumers_disabled")

                                val auxiliaryConsumers =
                                  if cfg.runtime.consumersEnabled then payloadAuditStream.merge(wirelessStreams)
                                  else Stream.empty
                                val legacyLockedConsumers =
                                  if cfg.runtime.consumersEnabled && cfg.processors.enabled.isEmpty then
                                    scanStream.merge(loadStream).merge(resultStream)
                                  else Stream.empty
                                val consumerStreams = auxiliaryConsumers.merge(legacyLockedConsumers)

                                val streams = enabledRuntimeStreams(
                                  cfg.runtime,
                                  processorStreams,
                                  consumerStreams
                                )

                                Resource.make(
                                  tiDbRepo.ensureAllCursors(cfg.ingest.streamNames, dbSemaphore) *>
                                    streams.compile.drain.start
                                )(_.cancel)
                              }
                          }
                        }
                      }
                    }
                  }
              }
            }
          }
        }

      log.info(
        "startup",
        "status" -> "starting",
        "tidb_host" -> cfg.tidb.host,
        "tidb_port" -> cfg.tidb.port.toString,
        "tidb_database" -> cfg.tidb.database
      )

      appResource.use(_.joinWithNever)

  private[coordinator] def dbWorkerPermits(poolSize: Int, healthcheckReserve: Int): Long =
    // Schema introspection and health checks use the transactor directly.
    // Keep the configured capacity available when admitted worker traffic is busy.
    (poolSize - healthcheckReserve).max(1).toLong

  private[coordinator] def enabledRuntimeStreams[A](
    runtime: RuntimeConfig,
    processorStreams: Stream[IO, A],
    consumerStreams: Stream[IO, A]
  ): Stream[IO, A] =
    (runtime.processorsEnabled, runtime.consumersEnabled) match
      case (true, true) => processorStreams.merge(consumerStreams)
      case (true, false) => processorStreams
      case (false, true) => consumerStreams
      case (false, false) => Stream.never[IO]
