package com.sslproxy.coordinator.wiring.workloads

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.archive.MinioPayloadArchive
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.postgres.PostgresMaintenanceStore
import com.sslproxy.coordinator.processor.{EventRetentionProcessor, PayloadArchiver, ProcessorId, ProcessorWorkload}
import fs2.Stream

import scala.concurrent.duration.*

private[coordinator] object RetentionWorkloads:
  private final case class EventRetentionRuntime(
      processor: EventRetentionProcessor[IO],
      initialize: IO[Unit]
  )

  def resource(
      cfg: AppConfig,
      maintenanceStore: PostgresMaintenanceStore,
      maintenanceOwnerId: String,
      leaseTtl: Int,
      enabledProcessorIds: Set[ProcessorId]
  ): Resource[IO, List[ProcessorWorkload]] =
    if !enabledProcessorIds.contains(ProcessorId.EventRetention) then
      Resource.pure[IO, List[ProcessorWorkload]](Nil)
    else
      MinioPayloadArchive.resource(cfg.archive).map { archiveRuntime =>
        val archiver = new PayloadArchiver(
          maintenanceStore,
          archiveRuntime.archive,
          cfg.archive.hotDays,
          cfg.archive.batchSize
        )
        val runtime = EventRetentionRuntime(
          new EventRetentionProcessor(
            maintenanceStore,
            archiver,
            maintenanceOwnerId,
            cfg.archive.eventRetentionDays,
            cfg.archive.tombstoneRetentionDays,
            cfg.archive.batchSize,
            leaseTtl
          ),
          archiveRuntime.initialize
        )
        List(
          ProcessorWorkload(
            ProcessorId.EventRetention,
            Stream.awakeEvery[IO](cfg.archive.maintenanceIntervalMs.millis)
              .evalMap(_ => runtime.processor.runOnce),
            startup = runtime.initialize
          )
        )
      }
