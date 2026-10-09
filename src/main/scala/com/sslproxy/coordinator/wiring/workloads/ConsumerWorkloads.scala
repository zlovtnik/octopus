package com.sslproxy.coordinator.wiring.workloads

import cats.effect.IO
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.dispatch.BackpressureService
import com.sslproxy.coordinator.kafka.{PostgresLoadStream, PostgresResultStream, ScanRequestStream, WirelessHeartbeatStream}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.wiring.DatabaseRuntime
import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorWorkload}
import fs2.kafka.KafkaProducer

private[coordinator] object ConsumerWorkloads:
  def build(
      cfg: AppConfig,
      db: DatabaseRuntime,
      metrics: CoordinatorMetrics,
      backpressureService: BackpressureService,
      producer: KafkaProducer[IO, String, String]
  ): List[ProcessorWorkload] =
    val scanStream = ScanRequestStream.run(
      cfg.kafka,
      cfg.ingest,
      db.ingestionStore,
      db.payloadResolver,
      metrics,
      backpressureService,
      producer,
      cfg.wireless.projection.projectionOnly
    )
    val loadStream = PostgresLoadStream.run(
      cfg.kafka,
      db.resultStore,
      db.loadHandler,
      producer,
      db.dbSemaphore,
      metrics
    )
    val resultStream = PostgresResultStream.run(
      cfg.kafka,
      db.resultStore,
      metrics,
      producer
    )
    val heartbeatStream =
      WirelessHeartbeatStream.run(
        cfg.kafka,
        db.ingestionStore,
        metrics,
        producer
      )

    List(
      ProcessorWorkload(
        ProcessorId.WirelessAuditProjection,
        com.sslproxy.coordinator.kafka.WirelessAuditStream.run(
          cfg.kafka, cfg.wireless.projection, db.repository, metrics, backpressureService, producer)
      ),
      ProcessorWorkload(
        ProcessorId.SyncScanIngestion,
        scanStream
      ),
      ProcessorWorkload(
        ProcessorId.SyncLoadConsumer,
        loadStream
      ),
      ProcessorWorkload(
        ProcessorId.SyncResultConsumer,
        resultStream
      ),
      ProcessorWorkload(
        ProcessorId.WirelessHeartbeatIngestion,
        heartbeatStream
      )
    )
