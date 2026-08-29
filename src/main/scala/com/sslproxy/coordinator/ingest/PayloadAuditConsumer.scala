package com.sslproxy.coordinator.ingest

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.{
  DatabaseError,
  PayloadAudit,
  ResolvedScanRequestRecord,
  ScanRequestRecord
}
import com.sslproxy.coordinator.kafka.KafkaComponents
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.persistence.{DatabaseOperationException, IngestionStore}
import com.sslproxy.coordinator.util.{ErrorSanitizer, Sha256Utils}
import fs2.Stream
import fs2.kafka.*
import io.circe.Json
import io.circe.parser as circeParser
import com.sslproxy.coordinator.observability.StructuredLogger

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

object PayloadAuditConsumer:
  private val log = StructuredLogger(getClass)
  private val StreamName = "proxy.payload_audit"
  private val MaxRetries = 3
  private val RetryDelay = 500.millis

  def stream(
      cfg: KafkaCfg,
      store: IngestionStore[IO],
      metrics: CoordinatorMetrics,
      dlqProducer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val consumerSettings = ConsumerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withGroupId(cfg.payloadAuditConsumer)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withMaxPollRecords(cfg.maxPollRecords)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "session.timeout.ms" -> "30000",
        "heartbeat.interval.ms" -> "3000"
      )

    Stream
      .eval(
        KafkaComponents.waitForTopic(cfg, cfg.payloadAuditTopic) *>
          KafkaComponents
            .waitForTopic(cfg, cfg.payloadAuditTopic + cfg.dlqSuffix)
      )
      .flatMap(_ => Stream.resource(KafkaConsumer.resource(consumerSettings)))
      .flatMap { consumer =>
        Stream.eval(consumer.subscribeTo(cfg.payloadAuditTopic)) >>
          consumer.stream
            .map(committable =>
              (
                committable.record.value,
                translateRecord(committable.record),
                committable.offset
              )
            )
            .through(batchWrite(store, dlqProducer, cfg, metrics))
            .through(commitBatch)
      }

  private[ingest] def translateRecord(
      record: ConsumerRecord[String, String]
  ): Either[PayloadAuditError, ResolvedScanRequestRecord] =
    val rawJson = record.value
    if rawJson == null || rawJson.isEmpty then
      Left(PayloadAuditError.EmptyMessage)
    else
      PayloadAudit.parse(rawJson) match
        case Left(err) =>
          Left(PayloadAuditError.InvalidPayload(rawJson, errorMessage(err)))
        case Right(audit) =>
          val payloadBytes = rawJson.getBytes(StandardCharsets.UTF_8)
          val payloadSha256 = Sha256Utils.sha256Hex(payloadBytes)
          val dedupeKey = Sha256Utils.sha256Hex(s"$StreamName:$payloadSha256")
          val payloadRef = s"sha256://$payloadSha256"

          import io.circe.Json
          val requestJson = Json
            .obj(
              "stream_name" -> Json.fromString(StreamName),
              "dedupe_key" -> Json.fromString(dedupeKey),
              "payload_ref" -> Json.fromString(payloadRef),
              "observed_at" -> Json.fromString(audit.observedAt)
            )
            .noSpaces

          log.trace(
            "payload_audit_ingest",
            "status" -> "received",
            "payload_bytes" -> payloadBytes.length.toString
          )

          val source = ScanRequestRecord(
            requestJson = requestJson,
            sourceRecordSha256 = payloadSha256,
            streamName = StreamName,
            dedupeKey = dedupeKey,
            observedAt = audit.observedAt,
            payloadRef = payloadRef
          )
          ResolvedScanRequestRecord
            .from(source, rawJson)
            .leftMap(error =>
              PayloadAuditError.InvalidPayload(rawJson, errorMessage(error))
            )

  private def batchWrite(
      store: IngestionStore[IO],
      dlqProducer: KafkaProducer[IO, String, String],
      cfg: KafkaCfg,
      metrics: CoordinatorMetrics
  ): fs2.Pipe[
    IO,
    (
        String,
        Either[PayloadAuditError, ResolvedScanRequestRecord],
        CommittableOffset[IO]
    ),
    CommittableOffset[IO]
  ] =
    _.groupWithin(cfg.maxPollRecords, 1.second)
      .evalTap { chunk =>
        val validRecords = chunk.collect { case (_, Right(r), _) => r }.toList
        val invalidBatch = chunk.collect { case (_, Left(e), _) => e }.toList
        val validWithRaw = chunk.collect { case (raw, Right(r), _) =>
          (raw, r)
        }.toList

        val dlqAction = invalidBatch.traverse_ { err =>
          publishDlq(dlqProducer, cfg, err)
        }

        val writeAction = if validRecords.nonEmpty then
          retryBatchWrite(
            store,
            dlqProducer,
            cfg,
            metrics,
            validRecords,
            validWithRaw
          )
        else IO.unit

        dlqAction *> writeAction
      }
      .flatMap { chunk =>
        Stream.emits(chunk.map(_._3).toList)
      }

  private def retryBatchWrite(
      store: IngestionStore[IO],
      dlqProducer: KafkaProducer[IO, String, String],
      cfg: KafkaCfg,
      metrics: CoordinatorMetrics,
      validRecords: List[ResolvedScanRequestRecord],
      validWithRaw: List[(String, ResolvedScanRequestRecord)]
  ): IO[Unit] =
    retryDatabaseWrite(store.recordScanRequests(validRecords).value).flatMap {
      case Right(count) =>
        IO(metrics.recordPayloadAuditIngested(count))
      case Left(dbErr) =>
        val sanitized = ErrorSanitizer.sanitize(dbErr.message)
        IO(
          log.error(
            "payload_audit_ingest",
            "status" -> "dlq",
            "operation" -> dbErr.operation,
            "error" -> sanitized
          )
        ) *>
          validWithRaw
            .traverse_ { (raw, _) =>
              publishRecordDlq(
                dlqProducer,
                cfg,
                raw,
                dbErr.operation,
                sanitized
              )
            }
            .handleErrorWith { publishError =>
              IO(
                log.error(
                  "payload_audit_ingest",
                  "status" -> "dlq_publish_failed",
                  "error" -> ErrorSanitizer
                    .sanitize(ErrorSanitizer.message(publishError))
                )
              ) *>
                IO.raiseError(publishError)
            }
    }

  private[ingest] def retryDatabaseWrite(
      write: IO[Either[DatabaseError, Int]],
      maxAttempts: Int = MaxRetries,
      initialDelay: FiniteDuration = RetryDelay
  ): IO[Either[DatabaseError.Permanent, Int]] =
    cats.Monad[IO].tailRecM[(Int, FiniteDuration), Either[DatabaseError.Permanent, Int]](
      (maxAttempts.max(1), initialDelay)
    ) { case (remaining, delay) =>
      write.flatMap {
        case Right(count) =>
          IO.pure(Right(Right(count)))
        case Left(dbErr: DatabaseError.Permanent) =>
          IO.pure(Right(Left(dbErr)))
        case Left(dbErr: DatabaseError.Retryable) if remaining > 1 =>
          IO(
            log.warn(
              "payload_audit_ingest",
              "status" -> "retry",
              "attempts_remaining" -> (remaining - 1).toString,
              "operation" -> dbErr.operation,
              "error" -> ErrorSanitizer.sanitize(dbErr.message)
            )
          ) *>
            IO.sleep(delay) *>
            IO.pure(Left((remaining - 1, delay * 2L)))
        case Left(dbErr: DatabaseError.Retryable) =>
          IO(
            log.error(
              "payload_audit_ingest",
              "status" -> "retry_exhausted",
              "operation" -> dbErr.operation,
              "error" -> ErrorSanitizer.sanitize(dbErr.message)
            )
          ) *>
            IO.raiseError(DatabaseOperationException(dbErr))
      }
    }

  private def publishRecordDlq(
      dlqProducer: KafkaProducer[IO, String, String],
      cfg: KafkaCfg,
      rawJson: String,
      operation: String,
      errorMsg: String
  ): IO[Unit] =
    val dlqTopic = cfg.payloadAuditTopic + cfg.dlqSuffix
    val original =
      circeParser.parse(rawJson).getOrElse(Json.fromString(rawJson))
    val dlqValue = Json
      .obj(
        "original" -> original,
        "error" -> Json.fromString(s"db_error[$operation]: $errorMsg"),
        "operation" -> Json.fromString(operation)
      )
      .noSpaces
    val record = ProducerRecord(dlqTopic, null: String, dlqValue)
    dlqProducer.produce(ProducerRecords.one(record)).flatten.void

  private def commitBatch: fs2.Pipe[IO, CommittableOffset[IO], Unit] =
    _.groupWithin(500, 15.seconds)
      .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  private def errorMessage(error: Throwable): String =
    Option(error.getMessage)
      .filter(_.nonEmpty)
      .getOrElse(error.getClass.getSimpleName)

  private def publishDlq(
      dlqProducer: KafkaProducer[IO, String, String],
      cfg: KafkaCfg,
      err: PayloadAuditError
  ): IO[Unit] =
    err match
      case PayloadAuditError.EmptyMessage                      => IO.unit
      case PayloadAuditError.InvalidPayload(rawJson, errorMsg) =>
        val dlqTopic = cfg.payloadAuditTopic + cfg.dlqSuffix
        val original =
          circeParser.parse(rawJson).getOrElse(Json.fromString(rawJson))
        val dlqValue = Json
          .obj(
            "original" -> original,
            "error" -> Json.fromString(
              errorMsg.replace('\n', ' ').replace('\r', ' ')
            )
          )
          .noSpaces
        val record = ProducerRecord(dlqTopic, null, dlqValue)
        dlqProducer.produce(ProducerRecords.one(record)).flatten.void

sealed trait PayloadAuditError
object PayloadAuditError:
  case object EmptyMessage extends PayloadAuditError
  final case class InvalidPayload(rawJson: String, error: String)
      extends PayloadAuditError
