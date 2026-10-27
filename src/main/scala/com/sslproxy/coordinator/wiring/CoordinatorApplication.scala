package com.sslproxy.coordinator.wiring

import cats.effect.kernel.Fiber
import cats.effect.{IO, Ref, Resource}
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.dq.{
  DqArtifacts,
  DqRetention,
  DqRuntime,
  DqStore
}
import com.sslproxy.coordinator.http.DqRoutes
import com.sslproxy.coordinator.kafka.KafkaComponents
import com.sslproxy.coordinator.observability.{
  CoordinatorMetrics,
  StructuredLogger
}
import com.sslproxy.coordinator.processor.ProcessorId
import com.sslproxy.coordinator.wiring.workloads.{
  ConsumerWorkloads,
  RetentionWorkloads,
  ScheduledWorkloads
}
import fs2.Stream

import scala.concurrent.duration.*

private[coordinator] object CoordinatorApplication:
  private val log = StructuredLogger(getClass)

  def resource(
      cfg: AppConfig,
      metrics: CoordinatorMetrics
  ): Resource[IO, Fiber[IO, Throwable, Unit]] =
    for
      _ <- ObservabilityModule.tracingResource
      db <- DatabaseModule.acquire(
        cfg.postgres,
        cfg.wireless.projection,
        cfg.sync.outboxDir,
        Some(metrics)
      )
      enabledProcessorIds = cfg.processors.enabled
        .flatMap(ProcessorId.fromString(_).toOption)
        .toSet
      runtimeConsumerIds = RuntimeStreams.runtimeConsumerProcessorIds(
        cfg.runtime
      )
      kafka <- KafkaComponents.resource(cfg.kafka)
      dqStore = new DqStore(db.doobieTx, cfg.dq.settings)
      // Always reconcile persisted policies first, including rollback startup.
      // The transaction fences old attempts before legacy projection can resume.
      _ <- Resource.eval(
        db.dbSemaphore.permit.use(_ => dqStore.reconcilePolicies)
      )
      dqReady <- Resource.eval(Ref.of[IO, Boolean](!cfg.dq.enabled))
      dqArtifacts <-
        if cfg.dq.enabled then
          DqArtifacts.resource(cfg.dq, cfg.archive).map(Some(_))
        else Resource.pure[IO, Option[DqArtifacts]](None)
      dqRetention <-
        if cfg.dq.enabled then
          DqRetention
            .resource(cfg.dq, cfg.archive, dqStore, db.dbSemaphore)
            .map(Some(_))
        else Resource.pure[IO, Option[DqRetention]](None)
      dqRuntime = dqArtifacts.map(artifacts =>
        new DqRuntime(
          cfg.dq,
          cfg.kafka,
          dqStore,
          artifacts,
          db.dbSemaphore,
          kafka.producer,
          cfg.processors.behaviorSimilarityThreshold,
          dqReady,
          metrics
        )
      )
      _ <- Resource.eval(dqRuntime.fold(IO.unit)(_.initialize))
      dqRoutes = dqArtifacts.fold(org.http4s.HttpRoutes.empty[IO])(artifacts =>
        new DqRoutes(
          dqStore,
          cfg.dq.controlToken,
          artifacts.authorizedAccess
        ).routes
      )
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
        cfg.postgres.connectionTimeoutMs.millis,
        dqRoutes,
        dqReady.get
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
      })
      streams = RuntimeStreams
        .assemble(
          cfg,
          services,
          workloads,
          enabledProcessorIds,
          runtimeConsumerIds
        )
        .merge(dqRuntime.fold(Stream.empty[IO].covaryOutput[Unit])(_.stream))
        .merge(dqRetention.fold(Stream.empty[IO].covaryOutput[Unit])(_.stream))
      fiber <- Resource.make(
        db.repository
          .ensureAllCursors(cfg.ingest.streamNames, db.dbSemaphore) *>
          streams.compile.drain.start
      )(_.cancel)
    yield fiber
