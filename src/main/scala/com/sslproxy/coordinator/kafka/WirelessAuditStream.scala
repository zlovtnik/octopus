package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.{KafkaCfg, WirelessProjectionConfig}
import com.sslproxy.coordinator.dispatch.BackpressureService
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.postgres.{PostgresErrorClass, PostgresRepository}
import com.sslproxy.coordinator.processor.WirelessObservation
import fs2.Stream
import fs2.kafka.KafkaProducer
import scala.concurrent.duration.*

object WirelessAuditStream:
  def run(cfg: KafkaCfg, projection: WirelessProjectionConfig, repository: PostgresRepository,
    metrics: CoordinatorMetrics, backpressure: BackpressureService,
    producer: KafkaProducer[IO, String, String]): Stream[IO, Unit] =
    if !projection.enabled then Stream.never[IO]
    else
      val consumer = LockedTopicConsumer.stream(
        cfg,
        cfg.wirelessAuditConsumer,
        cfg.wirelessAuditTopic,
        cfg.wirelessAuditConsumersCount,
        backpressure.awaitConsumerPermit,
        metrics,
        producer,
        WirelessObservation.decode
      ) { records =>
        records.traverse_(record => repository.projectWirelessRecord(record.decoded, record.metadata).flatMap {
          case Right(_) => IO.unit
          // Decode failures are parked by LockedTopicConsumer. Permanent storage
          // failures are parked to the DLQ so the processor does not dead-end.
          // Coordinate conflicts and retryable storage errors fail closed without
          // committing: a contradictory receipt must not be skipped.
          case Left(error) if isCoordinateConflict(error) =>
            IO.raiseError(RuntimeException(error.message, error.cause))
          case Left(error) if PostgresErrorClass.classify(error.cause) == PostgresErrorClass.Permanent =>
            LockedTopicConsumer.parkNonRetriable(
              producer,
              cfg.wirelessAuditTopic + cfg.dlqSuffix,
              cfg.wirelessAuditConsumer,
              record.record,
              error.cause
            )
          case Left(error) => IO.raiseError(RuntimeException(error.message, error.cause))
        })
      }
      // Expiry claims rows with SKIP LOCKED; safe across coordinator instances.
      val retention = Stream.repeatEval(repository.expireWirelessProjections(2000).flatMap {
        case Right(_) => IO.unit
        case Left(error) => IO.raiseError(RuntimeException(error.message, error.cause))
      }).metered(1.minute)
      consumer.concurrently(retention)

  private[kafka] def isCoordinateConflict(error: DatabaseError): Boolean =
    PostgresErrorClass.exceptions(error.cause).exists {
      case e: IllegalStateException =>
        e.getMessage == "wireless broker coordinate payload hash conflict"
      case _ => false
    }
