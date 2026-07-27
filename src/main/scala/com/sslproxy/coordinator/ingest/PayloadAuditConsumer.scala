package com.sslproxy.coordinator.ingest

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.{PayloadAudit, ResolvedScanRequestRecord, ScanRequestRecord}
import com.sslproxy.coordinator.kafka.KafkaComponents
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.tidb.TidbRepository
import com.sslproxy.coordinator.util.Sha256Utils
import fs2.Stream
import fs2.kafka.*
import io.circe.Json
import io.circe.parser as circeParser
import com.sslproxy.coordinator.observability.StructuredLogger

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.concurrent.duration.*

object PayloadAuditConsumer:
  private val log = StructuredLogger(getClass)
  private val StreamName = "proxy.payload_audit"

  def stream(
      cfg: KafkaCfg,
      repo: TidbRepository,
      metrics: CoordinatorMetrics,
      dlqProducer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
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
      .eval(KafkaComponents.waitForTopic(cfg.bootstrapServers, cfg.payloadAuditTopic))
      .flatMap(_ =>
        Stream.resource(KafkaConsumer.resource(consumerSettings))
      )
      .flatMap { consumer =>
        Stream.eval(consumer.subscribeTo(cfg.payloadAuditTopic)) >>
          consumer.stream
            .map(committable => (translateRecord(committable.record), committable.offset))
            .through(batchWrite(repo, dlqProducer, cfg, metrics, dbSemaphore))
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
          Left(PayloadAuditError.InvalidPayload(rawJson, err.getMessage))
        case Right(audit) =>
          val payloadBytes = rawJson.getBytes(StandardCharsets.UTF_8)
          val payloadSha256 = Sha256Utils.sha256Hex(payloadBytes)
          val dedupeKey = Sha256Utils.sha256Hex(s"$StreamName:$payloadSha256")
          val payloadRef = s"inline://json/${Base64.getUrlEncoder.withoutPadding.encodeToString(payloadBytes)}"

          import io.circe.Json
          val requestJson = Json.obj(
            "stream_name" -> Json.fromString(StreamName),
            "dedupe_key" -> Json.fromString(dedupeKey),
            "payload_ref" -> Json.fromString(payloadRef),
            "observed_at" -> Json.fromString(audit.observedAt)
          ).noSpaces

          log.trace("payload_audit_ingest", "status" -> "received",
            "payload_bytes" -> payloadBytes.length.toString)

          val source = ScanRequestRecord(
            requestJson = requestJson,
            sourceRecordSha256 = payloadSha256,
            streamName = StreamName,
            dedupeKey = dedupeKey,
            observedAt = audit.observedAt,
            payloadRef = payloadRef
          )
          ResolvedScanRequestRecord.from(source, rawJson)
            .leftMap(error => PayloadAuditError.InvalidPayload(rawJson, error.getMessage))

  private def batchWrite(
      repo: TidbRepository,
      dlqProducer: KafkaProducer[IO, String, String],
      cfg: KafkaCfg,
      metrics: CoordinatorMetrics,
      dbSemaphore: Semaphore[IO]
  ): fs2.Pipe[IO, (Either[PayloadAuditError, ResolvedScanRequestRecord], CommittableOffset[IO]), CommittableOffset[IO]] =
    _.groupWithin(cfg.maxPollRecords, 1.second)
      .evalTap { chunk =>
        val validRecords = chunk.collect { case (Right(r), _) => r }.toList
        val invalidBatch = chunk.collect { case (Left(e), _) => e }.toList

        val dlqAction = invalidBatch.traverse_ { err =>
          publishDlq(dlqProducer, cfg, err)
        }

        val writeAction = if validRecords.nonEmpty then
          dbSemaphore.permit.use(_ => repo.recordScanRequests(validRecords)).flatMap {
            case Right(count) =>
              IO(metrics.recordPayloadAuditIngested(count)) *>
                IO(metrics.recordSyncEventHydrated(count.toLong))
            case Left(dbErr) =>
              IO(log.error("payload_audit_ingest", "status" -> "failed",
                "operation" -> dbErr.operation, "error" -> dbErr.message)) *>
                IO.raiseError(new RuntimeException(
                  s"${dbErr.operation}: ${dbErr.message}", dbErr.cause))
          }
        else IO.unit

        dlqAction *> writeAction
      }
      .flatMap { chunk =>
        Stream.emits(chunk.map(_._2).toList)
      }

  private def commitBatch: fs2.Pipe[IO, CommittableOffset[IO], Unit] =
    _.groupWithin(500, 15.seconds)
      .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)

  private def publishDlq(
      dlqProducer: KafkaProducer[IO, String, String],
      cfg: KafkaCfg,
      err: PayloadAuditError
  ): IO[Unit] =
    err match
      case PayloadAuditError.EmptyMessage => IO.unit
      case PayloadAuditError.InvalidPayload(rawJson, errorMsg) =>
        val dlqTopic = cfg.payloadAuditTopic + cfg.dlqSuffix
        val original = circeParser.parse(rawJson).getOrElse(Json.fromString(rawJson))
        val dlqValue = Json.obj(
          "original" -> original,
          "error" -> Json.fromString(errorMsg.replace('\n', ' ').replace('\r', ' '))
        ).noSpaces
        val record = ProducerRecord(dlqTopic, null, dlqValue)
        dlqProducer.produce(ProducerRecords.one(record)).flatten.void

sealed trait PayloadAuditError
object PayloadAuditError:
  case object EmptyMessage extends PayloadAuditError
  final case class InvalidPayload(rawJson: String, error: String) extends PayloadAuditError
