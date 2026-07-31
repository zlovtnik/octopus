package com.sslproxy.coordinator

import cats.effect.{IO, IOApp, Resource}
import cats.effect.kernel.Fiber
import cats.effect.std.Semaphore
import com.comcast.ip4s.*
import fs2.Stream
import com.sslproxy.coordinator.config.{AppConfig, RuntimeConfig}
import com.sslproxy.coordinator.cron.CronScheduler
import com.sslproxy.coordinator.cutover.CutoverArtifactLoader
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.http.HealthRoutes
import com.sslproxy.coordinator.ingest.{PayloadAuditConsumer, SyncEventHydrationService}
import com.sslproxy.coordinator.kafka.{KafkaComponents, ScanRequestStream, TidbLoadStream, TidbResultStream, WirelessConsumerService}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.sink.*
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
        java.util.concurrent.Executors.newFixedThreadPool(cfg.tidb.poolSize, new java.util.concurrent.ThreadFactory:
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
        TidbTransactor.resource(cfg.tidb).flatMap { oldTx =>
          val tiDbDs = oldTx.dataSource
          val tiDbDoobieTx = Transactor.fromDataSource[IO](tiDbDs, blockingEc)
          val preflight = new TidbSchemaPreflight(oldTx, cfg.tidb)
          val tiDbRepo = new TidbRepository(tiDbDoobieTx)

          Resource.eval(preflight.validate()).flatMap { _ =>
            val artifactIO =
              if cfg.runtime.consumersEnabled then
                if cfg.cutover.devBypass then
                  IO(log.warn("startup", "status" -> "cutover_dev_bypass")) *>
                    cats.effect.Clock[IO].realTimeInstant.map(t =>
                      Some(com.sslproxy.coordinator.cutover.VerifiedCutoverArtifact.devBypass(cfg.cutover.expectedClusterId, t))
                    )
                else
                  CutoverArtifactLoader.loadAndVerify[IO](cfg.cutover).map(Some(_))
              else
                IO.pure(None)

            Resource.eval(artifactIO).flatMap { artifactOpt =>
              Resource.eval(Semaphore[IO](dbWorkerPermits(cfg.tidb.poolSize))).flatMap { dbSemaphore =>
                KafkaComponents.resource(cfg.kafka).flatMap { kafka =>
                  Resource.eval(SchemaIntrospector(tiDbDoobieTx, cfg.tidb.database, cfg.cron.schemaRefreshIntervalSeconds.seconds)).flatMap { schemaIntrospector =>
                    val payloadResolver = new TidbPayloadResolver(cfg.sync.outboxDir)
                    val handler = new TidbLoadHandler(payloadResolver, TidbTransformService, oldTx, TidbClock)
                    val hydrationService = new SyncEventHydrationService(
                      tiDbRepo,
                      payloadResolver,
                      metrics,
                      cfg.cron.scanFetchCount,
                      dbSemaphore
                    )

                    val backpressureService = new BackpressureService(
                      cfg.backpressure, cfg.cron.ingestBatchSize,
                      tiDbRepo.pendingLedgerCount(), metrics
                    )

                    val batchDispatchService = new BatchDispatchService(
                      tiDbRepo,
                      kafka.producer,
                      metrics,
                      java.util.UUID.randomUUID().toString,
                      List(cfg.kafka.loadTopic, cfg.kafka.resultTopic),
                      cfg.cron.batchDispatchLeaseSeconds,
                      cfg.cron.scanRetryBackoffSeconds,
                      cfg.cron.batchDispatchRetryMaxSeconds,
                      dbSemaphore
                    )

                    val cronScheduler = new CronScheduler(
                      cfg.cron, cfg.ingest, tiDbRepo,
                      backpressureService, batchDispatchService, metrics,
                      schemaIntrospector, dbSemaphore
                    )

                    val healthRoutes = new HealthRoutes(oldTx)

                    val httpPort = Port.fromInt(cfg.http.port).getOrElse(
                      sys.error(s"Port ${cfg.http.port} validated by config but IP4s rejected it")
                    )
                    val serverResource = EmberServerBuilder.default[IO]
                      .withPort(httpPort)
                      .withHost(host"0.0.0.0")
                      .withHttpApp(healthRoutes.routes.orNotFound)
                      .build

                    serverResource.flatMap { _ =>
                      val payloadAuditStream = PayloadAuditConsumer.stream(
                        cfg.kafka, tiDbRepo, metrics, kafka.producer, dbSemaphore
                      )

                      val wirelessStreams = WirelessConsumerService.allStreams(
                        cfg.wireless, cfg.kafka, tiDbRepo, kafka.producer, dbSemaphore
                      )

                      val processorStreams =
                        cronScheduler.mainLoop
                          .merge(cronScheduler.schemaRefresher)
                          .merge(hydrationService.runOnce.handleErrorWith { error =>
                            Stream.eval(IO(log.error(
                              "sync_event_hydration_backfill",
                              error,
                              "status" -> "failed"
                            )))
                          })

                      artifactOpt match
                        case Some(artifact) =>
                          log.info("startup", "status" -> "consumers_enabled",
                            "artifact_version" -> artifact.artifact.schemaVersion.toString,
                            "cluster_id" -> artifact.artifact.clusterId,
                            "scan_topic" -> cfg.kafka.scanTopic,
                            "scan_group" -> cfg.kafka.scanConsumer,
                            "load_topic" -> cfg.kafka.loadTopic,
                            "load_group" -> cfg.kafka.loadConsumer,
                            "result_topic" -> cfg.kafka.resultTopic,
                            "result_group" -> cfg.kafka.resultConsumer)
                        case None =>
                          log.info("startup", "status" -> "consumers_disabled")

                      val consumerStreams = artifactOpt match
                        case Some(artifact) =>
                          payloadAuditStream
                            .merge(wirelessStreams)
                            .merge(ScanRequestStream.run(
                              cfg.kafka,
                              artifact,
                              tiDbRepo,
                              payloadResolver,
                              metrics,
                              kafka.producer,
                              dbSemaphore
                            ))
                            .merge(TidbLoadStream.run(cfg.kafka, artifact, tiDbRepo, handler, kafka.producer, dbSemaphore))
                            .merge(TidbResultStream.run(cfg.kafka, artifact, tiDbRepo, kafka.producer, dbSemaphore))
                        case None =>
                          Stream.empty

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

      log.info("startup", "status" -> "starting",
        "tidb_host" -> cfg.tidb.host, "tidb_port" -> cfg.tidb.port.toString, "tidb_database" -> cfg.tidb.database)

      appResource.use(_.joinWithNever)

  private[coordinator] def dbWorkerPermits(poolSize: Int): Long =
    // Schema introspection and health checks use the transactor directly.
    // Keep capacity available for them when admitted worker traffic is busy.
    (poolSize - 2).max(1).toLong

  private[coordinator] def enabledRuntimeStreams[A](
      runtime: RuntimeConfig,
      processorStreams: Stream[IO, A],
      consumerStreams: Stream[IO, A]
  ): Stream[IO, A] =
    (runtime.processorsEnabled, runtime.consumersEnabled) match
      case (true, true)   => processorStreams.merge(consumerStreams)
      case (true, false)  => processorStreams
      case (false, true)  => consumerStreams
      case (false, false) => Stream.never[IO]
