package com.sslproxy.coordinator.kafka

import cats.effect.IO
import com.sslproxy.coordinator.domain.{
  BrokerRecordMetadata,
  DatabaseError,
  IngestionDecision,
  IngestionDisposition,
  IngestionInvariantViolation,
  ResolvedScanRequestRecord
}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.postgres.PostgresErrorClass

/** All intake records finish their durable write or acknowledged poison parking
  * before the consumer is allowed to commit their broker offsets.
  */
private[kafka] object DurableIngestion:
  private val log = StructuredLogger(getClass)

  def persist(
    record: ResolvedScanRequestRecord,
    metadata: BrokerRecordMetadata,
    write: IO[Either[DatabaseError, IngestionDecision]],
    metrics: CoordinatorMetrics,
    park: Throwable => IO[Unit]
  ): IO[Unit] =
    write.flatMap {
      case Right(decision) =>
        IO {
          decision.disposition match
            case IngestionDisposition.Processed => metrics.recordSyncEventHydrated()
            case IngestionDisposition.Deduplicated => metrics.recordIngestDeduplicated()
            case _ => ()
          log.info(
            "durable_ingestion",
            "status" -> decision.disposition.databaseValue,
            "stream_name" -> record.streamName,
            "group" -> metadata.consumerGroup,
            "partition" -> metadata.partition.toString,
            "offset" -> metadata.offset.toString
          )
        }
      case Left(error) if isParkable(error) => park(error.cause)
      case Left(error) => KafkaDatabaseResult.require[Unit](IO.pure(Left(error)))
    }

  def isInvariantViolation(error: DatabaseError): Boolean =
    PostgresErrorClass.exceptions(error.cause).exists(_.isInstanceOf[IngestionInvariantViolation])

  def isParkable(error: DatabaseError): Boolean =
    error.isInstanceOf[DatabaseError.Permanent] && !isInvariantViolation(error)
