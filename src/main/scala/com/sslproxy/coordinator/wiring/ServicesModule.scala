package com.sslproxy.coordinator.wiring

import cats.effect.IO
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.cron.CronScheduler
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.ingest.SyncEventHydrationService
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, OperationalStatsService, PostgresPeaksSource}
import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorSupervisor, SearchRetentionProcessor}
import fs2.kafka.KafkaProducer

import scala.concurrent.duration.*

private[coordinator] final case class CoordinatorServices(
    hydrationService: SyncEventHydrationService,
    batchDispatchService: BatchDispatchService,
    backpressureService: BackpressureService,
    cronScheduler: CronScheduler,
    supervisor: ProcessorSupervisor,
    statsService: OperationalStatsService,
    searchRetentionProcessor: SearchRetentionProcessor[IO],
    maintenanceOwnerId: String,
    leaseTtlSeconds: Int
)

private[coordinator] object ServicesModule:
  def build(
      cfg: AppConfig,
      db: DatabaseRuntime,
      metrics: CoordinatorMetrics,
      runtimeConsumerIds: Set[ProcessorId],
      producer: KafkaProducer[IO, String, String]
  ): IO[CoordinatorServices] =
    for
      hydrationService <- IO(new SyncEventHydrationService(
        db.ingestionStore,
        db.payloadResolver,
        metrics,
        cfg.cron.scanFetchCount,
        cfg.cron.scanMaxAttempts,
        db.dbSemaphore
      ))

      batchDispatchService <- IO(new BatchDispatchService(
        db.outboxStore,
        producer,
        metrics,
        java.util.UUID.randomUUID().toString,
        List(cfg.kafka.loadTopic, cfg.kafka.resultTopic),
        cfg.cron.batchDispatchLeaseSeconds,
        cfg.cron.scanRetryBackoffSeconds,
        cfg.cron.batchDispatchRetryMaxSeconds
      ))
      backpressureService <- BackpressureService.create(
        cfg.backpressure,
        cfg.cron.ingestBatchSize,
        db.ingestionStore.pendingCount.value,
        metrics
      )
      cronScheduler <- CronScheduler.create(
        cfg.cron,
        cfg.ingest,
        db.ingestionStore,
        db.outboxStore,
        db.projectionStore,
        db.maintenanceStore,
        backpressureService,
        batchDispatchService,
        metrics,
        db.preflight.validate()
      )

      maintenanceOwnerId <- IO(java.util.UUID.randomUUID().toString)
      leaseTtlSeconds = ((cfg.archive.maintenanceIntervalMs / 1000L) * 2L)
        .max(60L).min(Int.MaxValue.toLong).toInt
      searchRetentionProcessor = new SearchRetentionProcessor(
        db.maintenanceStore, maintenanceOwnerId, cfg.archive.searchRetentionDays,
        cfg.archive.batchSize, leaseTtlSeconds
      )
      supervisor <- ProcessorSupervisor.create(
        cfg.processors, db.processorStateStore, Some(metrics), runtimeConsumerIds
      )
      statsService <- OperationalStatsService.create(
        PostgresPeaksSource(db.repository), metrics, cfg.publicStats.peaksRefreshSeconds.seconds
      )
    yield CoordinatorServices(
      hydrationService, batchDispatchService, backpressureService, cronScheduler,
      supervisor, statsService, searchRetentionProcessor, maintenanceOwnerId, leaseTtlSeconds
    )
