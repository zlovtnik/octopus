package com.sslproxy.coordinator.dispatch

import cats.effect.IO
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, CoordinatorTracing}
import com.sslproxy.coordinator.persistence.OutboxStore
import com.sslproxy.coordinator.postgres.{OutboxFailureDisposition, OutboxRecord}
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords}
import com.sslproxy.coordinator.observability.StructuredLogger
import io.opentelemetry.api.trace.SpanKind

import scala.concurrent.duration.*

/** Publishes the transactional PostgreSQL outbox. A broker acknowledgement followed
  * by a process crash can produce the same stable message key again; the
  * receiving transaction is therefore required to deduplicate that key.
  */
final class BatchDispatchService(
    store: OutboxStore[IO],
    producer: KafkaProducer[IO, String, String],
    metrics: CoordinatorMetrics,
    ownerId: String,
    destinationTopics: List[String],
    leaseSeconds: Int,
    retryBaseSeconds: Int,
    retryMaxSeconds: Int
):
  import BatchDispatchService.{DispatchResult, log}

  def dispatchNext(): IO[DispatchResult] = store.claim(ownerId, destinationTopics, leaseSeconds).value.flatMap {
      case Left(error) =>
        val sanitized = com.sslproxy.coordinator.util.ErrorSanitizer.sanitize(error.message)
        IO(log.error("outbox_claim", "status" -> "db_error",
          "operation" -> error.operation, "error" -> sanitized))
          .as(DispatchResult.StopDraining)
      case Right(None) => IO.pure(DispatchResult.NoWork)
      case Right(Some(record)) => publish(record)
    }

  private def publish(record: OutboxRecord): IO[DispatchResult] =
    val brokerRecord = ProducerRecord(
      record.destinationTopic,
      record.messageKey,
      record.payload
    )

    CoordinatorTracing.span(
      "kafka.publish.outbox",
      SpanKind.PRODUCER,
      "messaging.system" -> "kafka",
      "messaging.destination.name" -> record.destinationTopic,
      "messaging.message.id" -> record.messageKey,
      "outbox.fence" -> record.lease.fence.toString
    ) {
      producer.produce(ProducerRecords.one(brokerRecord)).flatten
    }.timeout((leaseSeconds.toLong * 900L).max(1L).millis).attempt.flatMap {
      case Right(_) => acknowledge(record)
      case Left(error) => fail(record, error)
    }

  private def acknowledge(record: OutboxRecord): IO[DispatchResult] = store.acknowledge(record).value.flatMap {
      case Right(true) =>
        IO(metrics.recordBatchDispatched()) *>
          IO(log.info("outbox_publish", "status" -> "published",
            "outbox_id" -> record.outboxId, "topic" -> record.destinationTopic,
            "message_key" -> record.messageKey, "fence" -> record.lease.fence.toString))
            .as(DispatchResult.Dispatched)
      case Right(false) =>
        IO(log.warn("outbox_publish", "status" -> "lease_lost_after_publish",
          "outbox_id" -> record.outboxId, "fence" -> record.lease.fence.toString))
          .as(DispatchResult.ContinueDraining)
      case Left(error) =>
        val sanitized = com.sslproxy.coordinator.util.ErrorSanitizer.sanitize(error.message)
        IO(log.error("outbox_publish", "status" -> "ack_failed",
          "outbox_id" -> record.outboxId, "operation" -> error.operation,
          "error" -> sanitized)).as(DispatchResult.StopDraining)
    }

  private def fail(record: OutboxRecord, cause: Throwable): IO[DispatchResult] =
    val message = Option(cause.getMessage).getOrElse(cause.getClass.getSimpleName)
    store.fail(record, message, retryBaseSeconds, retryMaxSeconds).value.flatMap {
      case Right(disposition) =>
        val status = disposition match
          case OutboxFailureDisposition.RetryScheduled => "retry_scheduled"
          case OutboxFailureDisposition.Parked         => "parked"
        IO(log.warn("outbox_publish", "status" -> status,
          "outbox_id" -> record.outboxId, "attempt" -> record.attemptCount.toString,
          "error" -> message)).as(DispatchResult.StopDraining)
      case Left(error) =>
        val sanitized = com.sslproxy.coordinator.util.ErrorSanitizer.sanitize(error.message)
        IO(log.error("outbox_publish", "status" -> "fail_transition_failed",
          "outbox_id" -> record.outboxId, "operation" -> error.operation,
          "error" -> sanitized)).as(DispatchResult.StopDraining)
    }

object BatchDispatchService:
  private val log = StructuredLogger(getClass)

  enum DispatchResult:
    case NoWork
    case Dispatched
    case ContinueDraining
    case StopDraining
