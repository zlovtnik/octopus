package com.sslproxy.coordinator

import cats.effect.{IO, IOApp, Resource}
import cats.effect.kernel.Fiber
import cats.effect.std.Semaphore
import com.comcast.ip4s.*
import fs2.Stream
import com.sslproxy.coordinator.archive.MinioPayloadArchive
import com.sslproxy.coordinator.config.{AppConfig, RuntimeConfig}
import com.sslproxy.coordinator.cron.CronScheduler
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.http.HealthRoutes
import com.sslproxy.coordinator.ingest.{PayloadAuditConsumer, SyncEventHydrationService}
import com.sslproxy.coordinator.kafka.{
  KafkaComponents,
  ScanRequestStream,
  PostgresLoadStream,
  PostgresResultStream,
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
import com.sslproxy.coordinator.postgres.*
import com.sslproxy.coordinator.postgres.sql.IngestionSql
import doobie.Transactor
import doobie.implicits.*
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
          cfg.postgres.poolSize,
          new java.util.concurrent.ThreadFactory:
            def newThread(r: Runnable): Thread =
              val t = new Thread(r, "doobie-postgres-pool")
              t.setDaemon(true)
              t
        )
      )
    val meterRegistry = new SimpleMeterRegistry()
    val metrics = new CoordinatorMetrics(meterRegistry)

    if !cfg.postgres.enabled then
      log.warn("startup", "status" -> "disabled", "postgres_sink" -> "disabled")
      IO.println("PostgreSQL sink disabled (set POSTGRES_ENABLED=true to enable)").void
    else
      val appResource: Resource[IO, Fiber[IO, Throwable, Unit]] =
        CoordinatorTracing.resource.flatMap { _ =>
          PostgresTransactor.resource(cfg.postgres).flatMap { oldTx =>
            val postgresDs = oldTx.dataSource
            val postgresDoobieTx = Transactor.fromDataSource[IO](postgresDs, blockingEc)
            val preflight = new PostgresSchemaPreflight(oldTx, cfg.postgres)

            Resource.eval(preflight.validate()).flatMap { _ =>
              val enabledProcessorIds = cfg.processors.enabled.flatMap(ProcessorId.fromString(_).toOption).toSet
              Resource
                .eval(
                  Semaphore[IO](
                    dbWorkerPermits(
                      cfg.postgres.poolSize,
                      cfg.postgres.healthcheckReserve
                    )
                  )
                )
                .flatMap { dbSemaphore =>
                  val postgresRepo = new PostgresRepository(postgresDoobieTx, Some(dbSemaphore))
                  KafkaComponents.resource(cfg.kafka).flatMap { kafka =>
                    val payloadResolver = new PostgresPayloadResolver(cfg.sync.outboxDir)
                    def payloadLookup(sha: String): IO[Option[String]] =
                      IngestionSql.payloadBySha256(sha).unique.transact(postgresDoobieTx).attempt.map(_.toOption)
                    val handler =
                      new PostgresLoadHandler(payloadResolver, PostgresTransformService, oldTx, PostgresClock, payloadLookup)
                    val ingestionStore = new PostgresIngestionStore(postgresRepo)
                    val outboxStore = new PostgresOutboxStore(postgresRepo)
                    val projectionStore = new PostgresProjectionStore(postgresRepo)
                    val maintenanceStore = new PostgresMaintenanceStore(postgresRepo)
                    val resultStore = new PostgresResultStore(postgresRepo)
                    val wirelessStore = new PostgresWirelessStore(postgresRepo)
                    val hydrationService = new SyncEventHydrationService(
                      ingestionStore,
                      payloadResolver,
                      metrics,
                      cfg.cron.scanFetchCount,
                      cfg.cron.scanMaxAttempts,
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
                      cfg.cron.batchDispatchRetryMaxSeconds
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
                        preflight.validate()
                      )
                    yield cronScheduler

                    Resource.eval(scheduler).flatMap { cronScheduler =>
                      val processorStateStore = new PostgresProcessorStateStore(postgresDoobieTx, Some(dbSemaphore))
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
                        Resource
                          .eval(
                            ProcessorSupervisor.create(
                              cfg.processors,
                              processorStateStore,
                              Some(metrics)
                            )
                          )
                          .flatMap { supervisor =>
                            val healthRoutes = new HealthRoutes(
                              oldTx,
                              metrics,
                              Some(supervisor.readiness),
                              cfg.postgres.connectionTimeoutMs.millis
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
                              val scanStream = ScanRequestStream.run(
                                cfg.kafka,
                                cfg.ingest,
                                ingestionStore,
                                payloadResolver,
                                metrics,
                                kafka.producer
                              )
                              val loadStream = PostgresLoadStream.run(
                                cfg.kafka,
                                resultStore,
                                handler,
                                kafka.producer,
                                dbSemaphore
                              )
                              val resultStream = PostgresResultStream.run(
                                cfg.kafka,
                                resultStore,
                                kafka.producer
                              )

                              val lockedConsumerWorkloads =
                                if cfg.runtime.consumersEnabled then
                                  List(
                                    ProcessorWorkload(ProcessorId.SyncScanIngestion, scanStream),
                                    ProcessorWorkload(ProcessorId.SyncLoadConsumer, loadStream),
                                    ProcessorWorkload(ProcessorId.SyncResultConsumer, resultStream)
                                  )
                                else Nil

                              val workloads = lockedConsumerWorkloads ++ List(
                                ProcessorWorkload(
                                  ProcessorId.SyncJobPlanner,
                                  cronScheduler.jobPlanningStream
                                ),
                                ProcessorWorkload(
                                  ProcessorId.SyncBacklogRecovery,
                                  cronScheduler.backlogRecoveryStream
                                ),
                                ProcessorWorkload(ProcessorId.SyncLoadDispatch, cronScheduler.loadDispatchStream),
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

                              val requiredRuntimeStreams =
                                cronScheduler.schemaRefresher
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
                                if cfg.processors.enabled.isEmpty then Stream.empty
                                else
                                  cronScheduler.supportStream
                                    .merge(supervisor.run(workloads))

                              val consumerWorkloads =
                                if cfg.runtime.consumersEnabled then
                                  List(
                                    ProcessorWorkload(ProcessorId.WirelessBacklogSave,
                                      WirelessConsumerService.backlogSaveStream(cfg.wireless, cfg.kafka, wirelessStore, kafka.producer)),
                                    ProcessorWorkload(ProcessorId.WirelessBacklogList,
                                      WirelessConsumerService.backlogListStream(cfg.wireless, cfg.kafka, wirelessStore, kafka.producer)),
                                    ProcessorWorkload(ProcessorId.WirelessBacklogSynced,
                                      WirelessConsumerService.backlogSyncedStream(cfg.wireless, cfg.kafka, wirelessStore, kafka.producer)),
                                    ProcessorWorkload(ProcessorId.WirelessBacklogPrune,
                                      WirelessConsumerService.backlogPruneStream(cfg.wireless, cfg.kafka, wirelessStore, kafka.producer)),
                                    ProcessorWorkload(ProcessorId.WirelessMacLookup,
                                      WirelessConsumerService.macLookupStream(cfg.wireless, cfg.kafka, wirelessStore, kafka.producer)),
                                    ProcessorWorkload(ProcessorId.WirelessNetworksAuthorized,
                                      WirelessConsumerService.networksAuthorizedStream(cfg.wireless, cfg.kafka, wirelessStore, kafka.producer)),
                                    ProcessorWorkload(ProcessorId.WirelessProbeFlush,
                                      WirelessConsumerService.probeFlushStream(cfg.wireless, cfg.kafka, wirelessStore, kafka.producer)),
                                    ProcessorWorkload(ProcessorId.PayloadAuditIngestion,
                                      PayloadAuditConsumer.stream(cfg.kafka, ingestionStore, metrics, kafka.producer))
                                  )
                                else Nil

                              val consumerSupervisorEnabled: Set[ProcessorId] =
                                if cfg.runtime.consumersEnabled then
                                  Set(
                                    ProcessorId.WirelessBacklogSave,
                                    ProcessorId.WirelessBacklogList,
                                    ProcessorId.WirelessBacklogSynced,
                                    ProcessorId.WirelessBacklogPrune,
                                    ProcessorId.WirelessMacLookup,
                                    ProcessorId.WirelessNetworksAuthorized,
                                    ProcessorId.WirelessProbeFlush,
                                    ProcessorId.PayloadAuditIngestion
                                  )
                                else Set.empty

                              val consumerStreams =
                                if consumerSupervisorEnabled.isEmpty then Stream.empty
                                else
                                  Stream.eval(
                                    ProcessorSupervisor.create(
                                      cfg.processors.copy(enabled = consumerSupervisorEnabled.map(_.value).toList),
                                      processorStateStore
                                    )
                                  ).flatMap(_.run(consumerWorkloads))

                              log.info(
                                "startup",
                                "status" -> (if cfg.runtime.consumersEnabled then "consumers_enabled"
                                             else "consumers_disabled"),
                                "offset_source" -> "kafka_committed_offsets",
                                "scan_topic" -> cfg.kafka.scanTopic,
                                "scan_group" -> cfg.kafka.scanConsumer,
                                "load_topic" -> cfg.kafka.loadTopic,
                                "load_group" -> cfg.kafka.loadConsumer,
                                "result_topic" -> cfg.kafka.resultTopic,
                                "result_group" -> cfg.kafka.resultConsumer
                              )

                              val streams = enabledRuntimeStreams(
                                cfg.runtime,
                                processorStreams,
                                consumerStreams,
                                requiredRuntimeStreams
                              )

                              Resource.make(
                                postgresRepo.ensureAllCursors(cfg.ingest.streamNames, dbSemaphore) *>
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

      log.info(
        "startup",
        "status" -> "starting",
        "postgres_host" -> cfg.postgres.host,
        "postgres_port" -> cfg.postgres.port.toString,
        "postgres_database" -> cfg.postgres.database
      )

      appResource.use(_.joinWithNever)

  private[coordinator] def dbWorkerPermits(poolSize: Int, healthcheckReserve: Int): Long =
    // Schema introspection and health checks use the transactor directly.
    // Keep the configured capacity available when admitted worker traffic is busy.
    (poolSize - healthcheckReserve).max(1).toLong

  private[coordinator] def enabledRuntimeStreams[A](
    runtime: RuntimeConfig,
    processorStreams: Stream[IO, A],
    consumerStreams: Stream[IO, A],
    requiredRuntimeStreams: Stream[IO, A]
  ): Stream[IO, A] =
    (runtime.processorsEnabled, runtime.consumersEnabled) match
      case (true, true) => processorStreams.merge(consumerStreams).merge(requiredRuntimeStreams)
      case (true, false) => processorStreams.merge(requiredRuntimeStreams) ++ Stream.never[IO]
      case (false, true) => consumerStreams.merge(requiredRuntimeStreams) ++ Stream.never[IO]
      case (false, false) => Stream.never[IO]
