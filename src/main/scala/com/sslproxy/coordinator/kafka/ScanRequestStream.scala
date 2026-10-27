package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.dispatch.BackpressureService
import com.sslproxy.coordinator.domain.ScanRequestRecord
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.persistence.IngestionStore
import com.sslproxy.coordinator.postgres.{PostgresPayloadReadException, PostgresPayloadResolver}
import fs2.Stream
import fs2.kafka.KafkaProducer

import java.nio.file.NoSuchFileException

object ScanRequestStream:
  def run(
    cfg: KafkaCfg,
    store: IngestionStore[IO],
    payloadResolver: PostgresPayloadResolver,
    metrics: CoordinatorMetrics,
    backpressure: BackpressureService,
    producer: KafkaProducer[IO, String, String],
    wirelessProjectionOnly: Boolean = false
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(
      cfg,
      cfg.scanConsumer,
      cfg.scanTopic,
      cfg.scanConsumersCount,
      backpressure.awaitConsumerPermit,
      metrics,
      producer,
      ScanRequestRecord.decodeWire
    ) { lockedRecords =>
      lockedRecords
        // In projection-only mode wireless.audit owns durable processing. Its
        // companion discovery request must not create legacy frame-load work.
        .filter(locked => shouldIngest(locked.decoded.streamName, wirelessProjectionOnly))
        .traverse_ { locked =>
          def park(error: Throwable): IO[Unit] =
            LockedTopicConsumer.parkNonRetriable(
              producer,
              cfg.scanTopic + cfg.dlqSuffix,
              cfg.scanConsumer,
              locked.record,
              error
            )
          IO.blocking(payloadResolver.resolve(locked.decoded)).attempt.flatMap {
            case Right(record) =>
              DurableIngestion.persist(
                record,
                locked.metadata,
                store.recordScanRequestWithEvidence(record, locked.metadata).value,
                metrics,
                park
              )
            case Left(error) if isNonRetriableResolutionError(error) => park(error)
            case Left(error) => IO.raiseError(error)
          }
        }
    }

  private[kafka] def shouldIngest(streamName: String, wirelessProjectionOnly: Boolean): Boolean =
    !(wirelessProjectionOnly && streamName == "wireless.audit")

  private[kafka] def isNonRetriableResolutionError(error: Throwable): Boolean =
    error match
      case _: IllegalArgumentException => true
      case payloadError: PostgresPayloadReadException =>
        Option(payloadError.getCause).exists(_.isInstanceOf[NoSuchFileException])
      case _ => false
