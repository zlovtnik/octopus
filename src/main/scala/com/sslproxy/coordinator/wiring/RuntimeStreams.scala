package com.sslproxy.coordinator.wiring

import cats.effect.IO
import com.sslproxy.coordinator.config.{AppConfig, RuntimeConfig}
import com.sslproxy.coordinator.metrics.StatsMaterializerStream
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.postgres.PostgresMetricsRepository
import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorWorkload}
import fs2.Stream

private[coordinator] object RuntimeStreams:
  private val log = StructuredLogger(getClass)

  def enabledRuntimeStreams[A](
      runtime: RuntimeConfig,
      supervisedStreams: Stream[IO, A],
      processorSupportStreams: Stream[IO, A],
      requiredRuntimeStreams: Stream[IO, A]
  ): Stream[IO, A] =
    if !runtime.anyEnabled then Stream.never[IO]
    else
      val processorSupport =
        if runtime.processorsEnabled then processorSupportStreams
        else Stream.empty
      supervisedStreams
        .merge(processorSupport)
        .merge(requiredRuntimeStreams) ++ Stream.never[IO]

  def runtimeConsumerProcessorIds(
      runtime: RuntimeConfig
  ): Set[ProcessorId] =
    if runtime.consumersEnabled then ProcessorId.kafkaConsumers
    else Set.empty

  def assemble(
      cfg: AppConfig,
      db: DatabaseRuntime,
      metrics: CoordinatorMetrics,
      services: CoordinatorServices,
      workloads: List[ProcessorWorkload],
      enabledProcessorIds: Set[ProcessorId],
      runtimeConsumerIds: Set[ProcessorId]
  ): Stream[IO, Unit] =
    val hydrationService = services.hydrationService
    val requiredRuntimeStreams = services.cronScheduler.schemaRefresher
      .merge(hydrationService.runOnce.handleErrorWith { error =>
        Stream.eval(IO(log.error("sync_event_hydration_backfill", error, "status" -> "failed")))
      })
    val supervisedStreams =
      if enabledProcessorIds.isEmpty && runtimeConsumerIds.isEmpty then Stream.empty
      else services.supervisor.run(workloads)
    val processorSupportStreams =
      if enabledProcessorIds.isEmpty then Stream.empty
      else services.cronScheduler.supportStream
    val statsMaterializerStream = StatsMaterializerStream.run(
      new PostgresMetricsRepository(db.transactor, Some(db.dbSemaphore)),
      metrics, cfg.statsStore, cfg.statsMaterializer
    )
    // Stats snapshots keep running even when both runtime gates are disabled.
    enabledRuntimeStreams(
      cfg.runtime, supervisedStreams, processorSupportStreams, requiredRuntimeStreams
    ).merge(statsMaterializerStream)
