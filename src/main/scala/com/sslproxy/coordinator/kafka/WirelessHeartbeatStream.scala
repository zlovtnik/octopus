package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.{IngestionDisposition, ResolvedScanRequestRecord, ScanRequestRecord}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.persistence.IngestionStore
import com.sslproxy.coordinator.postgres.PostgresErrorClass
import com.sslproxy.coordinator.util.Sha256Utils
import fs2.Stream
import fs2.kafka.KafkaProducer
import io.circe.Json
import io.circe.parser.parse

import java.nio.charset.StandardCharsets

object WirelessHeartbeatStream:
  private val log = StructuredLogger(getClass)
  private val Topic = "wireless.sensor.heartbeat"
  private val ConsumerGroup = "wireless-sensor-heartbeat-postgres-v1"

  private final case class Decoded(record: ResolvedScanRequestRecord)

  def run(
    cfg: KafkaCfg,
    store: IngestionStore[IO],
    metrics: CoordinatorMetrics,
    producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, ConsumerGroup, Topic, producer, decode) { lockedRecords =>
      lockedRecords.traverse_ { locked =>
        store.recordScanRequestWithEvidence(locked.decoded.record, locked.metadata).value.flatMap {
          case Right(decision) =>
            IO.whenA(decision.disposition == IngestionDisposition.Processed)(
              IO(metrics.recordSyncEventHydrated())
            ) *> IO(
              log.info(
                "wireless_sensor_heartbeat",
                "status" -> decision.disposition.databaseValue,
                "group" -> locked.metadata.consumerGroup,
                "partition" -> locked.metadata.partition.toString,
                "offset" -> locked.metadata.offset.toString
              )
            )
          case Left(error) if PostgresErrorClass.classify(error.cause) == PostgresErrorClass.Permanent =>
            LockedTopicConsumer.parkNonRetriable(
              producer,
              Topic + cfg.dlqSuffix,
              ConsumerGroup,
              locked.record,
              error.cause
            )
          case Left(error) => IO.raiseError(RuntimeException(error.message, error.cause))
        }
      }
    }

  private def decode(raw: String): Either[Throwable, Decoded] =
    for
      json <- parse(raw).left.map(identity[Throwable])
      obj <- json.asObject.toRight(IllegalArgumentException("heartbeat payload must be a JSON object"))
      observedAt <- json.hcursor.get[String]("observed_at").left.map(identity[Throwable])
      sensorId <- json.hcursor.get[String]("sensor_id").left.map(identity[Throwable])
      _ <- Either
        .catchOnly[java.time.format.DateTimeParseException](java.time.OffsetDateTime.parse(observedAt))
        .leftMap(identity[Throwable])
      rawSha = Sha256Utils.sha256Hex(raw.getBytes(StandardCharsets.UTF_8))
      eventId = json.hcursor.get[String]("event_id").toOption.filter(_.nonEmpty).getOrElse(rawSha)
      enriched = Json.fromJsonObject(
        obj
          .add("event_id", Json.fromString(eventId))
          .add("schema_version", obj("schema_version").getOrElse(Json.fromInt(1)))
          .add("produced_at", obj("produced_at").getOrElse(Json.fromString(observedAt)))
          .add("source_identity", obj("source_identity").getOrElse(Json.fromString(sensorId)))
          .add("correlation_id", obj("correlation_id").getOrElse(Json.fromString(eventId)))
          .add("causation_id", obj("causation_id").getOrElse(Json.fromString(eventId)))
      ).noSpaces
      source = ScanRequestRecord(raw, rawSha, Topic, eventId, observedAt, s"inline://heartbeat/$eventId")
      resolved <- ResolvedScanRequestRecord.from(source, enriched)
    yield Decoded(resolved)
