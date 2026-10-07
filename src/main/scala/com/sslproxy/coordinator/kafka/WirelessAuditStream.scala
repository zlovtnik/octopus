package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.{KafkaCfg, WirelessProjectionConfig}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.postgres.PostgresRepository
import com.sslproxy.coordinator.processor.WirelessObservation
import fs2.Stream
import fs2.kafka.KafkaProducer
import scala.concurrent.duration.*

object WirelessAuditStream:
  def run(cfg: KafkaCfg, projection: WirelessProjectionConfig, repository: PostgresRepository,
    metrics: CoordinatorMetrics, producer: KafkaProducer[IO, String, String]): Stream[IO, Unit] =
    if !projection.enabled then Stream.never[IO]
    else
      val consumer = LockedTopicConsumer.stream(cfg, "wireless-audit-projection-v1", "wireless.audit", 1,
        IO.unit, metrics, producer, WirelessObservation.decode) { records =>
        records.traverse_(record => repository.projectWirelessRecord(record.decoded, record.metadata).flatMap {
          case Right(_) => IO.unit
          // Decode failures are parked by LockedTopicConsumer. Storage failures,
          // including coordinate conflicts, fail closed without committing.
          case Left(error) => IO.raiseError(RuntimeException(error.message, error.cause))
        })
      }
      // Expiry claims rows with SKIP LOCKED; safe across coordinator instances.
      val retention = Stream.repeatEval(repository.expireWirelessProjections(2000).flatMap {
        case Right(_) => IO.unit
        case Left(error) => IO.raiseError(RuntimeException(error.message, error.cause))
      }).metered(1.minute)
      consumer.concurrently(retention)
