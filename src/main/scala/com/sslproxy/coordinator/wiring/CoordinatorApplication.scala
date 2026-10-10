package com.sslproxy.coordinator.wiring

import cats.effect.{IO, Resource}
import cats.effect.kernel.Fiber
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.kafka.KafkaComponents
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.processor.ProcessorId
import com.sslproxy.coordinator.wiring.workloads.{ConsumerWorkloads, RetentionWorkloads, ScheduledWorkloads}

import scala.concurrent.duration.*

private[coordinator] object CoordinatorApplication:
  private val log = StructuredLogger(getClass)

  def resource(
    cfg: AppConfig,
    metrics: CoordinatorMetrics
  ): Resource[IO, Fiber[IO, Throwable, Unit]] =
    for
      _ <- ObservabilityModule.tracingResource
      db <- DatabaseModule.acquire(cfg.postgres, cfg.wireless.projection, cfg.sync.outboxDir, Some(metrics))
      enabledProcessorIds = cfg.processors.enabled.flatMap(ProcessorId.fromString(_).toOption).toSet
      runtimeConsumerIds = RuntimeStreams.runtimeConsumerProcessorIds(cfg.runtime)
      kafka <- KafkaComponents.resource(cfg.kafka)
      services <- Resource.eval(
        ServicesModule.build(
          cfg,
          db,
          metrics,
          runtimeConsumerIds,
          kafka.producer
        )
      )
      retentionWorkloads <- RetentionWorkloads.resource(
        cfg,
        db.maintenanceStore,
        services.maintenanceOwnerId,
        services.leaseTtlSeconds,
        enabledProcessorIds
      )
      _ <- ServerModule.resource(
        cfg.http,
        cfg.publicStats,
        db.transactor,
        metrics,
        services.supervisor.readiness,
        services.statsService,
        cfg.postgres.connectionTimeoutMs.millis
      )
      workloads = ConsumerWorkloads.build(
        cfg,
        db,
        metrics,
        services.backpressureService,
        kafka.producer
      ) ++ ScheduledWorkloads.build(
        cfg,
        services.cronScheduler,
        services.searchRetentionProcessor,
        cfg.archive.maintenanceIntervalMs.millis
      ) ++ retentionWorkloads
      _ <- Resource.eval(IO {
        log.info(
          "startup",
          "status" -> (if cfg.runtime.consumersEnabled then "consumers_enabled" else "consumers_disabled"),
          "offset_source" -> "kafka_committed_offsets",
          "scan_topic" -> cfg.kafka.scanTopic,
          "scan_group" -> cfg.kafka.scanConsumer,
          "load_topic" -> cfg.kafka.loadTopic,
          "load_group" -> cfg.kafka.loadConsumer,
          "result_topic" -> cfg.kafka.resultTopic,
          "result_group" -> cfg.kafka.resultConsumer
        )
      })
      streams = RuntimeStreams.assemble(
        cfg,
        services,
        workloads,
        enabledProcessorIds,
        runtimeConsumerIds
      )
      fiber <- Resource.make(
        db.repository.ensureAllCursors(cfg.ingest.streamNames, db.dbSemaphore) *>
          streams.compile.drain.start
      )(_.cancel)
    yield fiber
