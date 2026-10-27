package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.{ResolvedScanRequestRecord, ScanRequestRecord}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.persistence.IngestionStore
import com.sslproxy.coordinator.util.Sha256Utils
import fs2.Stream
import fs2.kafka.KafkaProducer
import io.circe.Json
import io.circe.parser.parse

import java.nio.charset.StandardCharsets

object WirelessHeartbeatStream:
  private val Topic = "wireless.sensor.heartbeat"

  private[kafka] final case class Decoded(record: ResolvedScanRequestRecord)

  def run(
    cfg: KafkaCfg,
    store: IngestionStore[IO],
    metrics: CoordinatorMetrics,
    producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.wirelessHeartbeatConsumer, Topic, 1, IO.unit, metrics, producer, decode) {
      lockedRecords =>
        lockedRecords.traverse_ { locked =>
          DurableIngestion.persist(
            locked.decoded.record,
            locked.metadata,
            store.recordScanRequestWithEvidence(locked.decoded.record, locked.metadata).value,
            metrics,
            error =>
              LockedTopicConsumer.parkNonRetriable(
                producer,
                Topic + cfg.dlqSuffix,
                cfg.wirelessHeartbeatConsumer,
                locked.record,
                error
              )
          )
        }
    }

  private[kafka] def decode(raw: String): Either[Throwable, Decoded] =
    for
      json <- parse(raw).left.map(identity[Throwable])
      obj <- json.asObject.toRight(IllegalArgumentException("heartbeat payload must be a JSON object"))
      observedAt <- json.hcursor.get[String]("observed_at").left.map(identity[Throwable])
      sensorId <- json.hcursor.get[String]("sensor_id").left.map(identity[Throwable])
      _ <- Either.cond(
        !sensorId.isBlank && sensorId.length <= 64,
        (),
        IllegalArgumentException("heartbeat sensor_id must contain 1 to 64 characters")
      )
      producedAt <- json.hcursor.getOrElse[String]("produced_at")(observedAt).left.map(identity[Throwable])
      schemaVersion <- json.hcursor.getOrElse[Int]("schema_version")(1).left.map(identity[Throwable])
      _ <- Either.cond(schemaVersion > 0, (), IllegalArgumentException("heartbeat schema_version must be positive"))
      _ <- Either
        .catchOnly[java.time.format.DateTimeParseException](java.time.OffsetDateTime.parse(observedAt))
        .leftMap(identity[Throwable])
      _ <- Either
        .catchOnly[java.time.format.DateTimeParseException](java.time.OffsetDateTime.parse(producedAt))
        .leftMap(identity[Throwable])
      rawSha = Sha256Utils.sha256Hex(raw.getBytes(StandardCharsets.UTF_8))
      eventId <- json.hcursor.getOrElse[String]("event_id")(rawSha).left.map(identity[Throwable])
      _ <- Either.cond(
        !eventId.isBlank && eventId.length <= 255,
        (),
        IllegalArgumentException("heartbeat event_id must contain 1 to 255 characters")
      )
      enriched = Json
        .fromJsonObject(
          obj
            .add("event_id", Json.fromString(eventId))
            .add("schema_version", Json.fromInt(schemaVersion))
            .add("produced_at", Json.fromString(producedAt))
            .add("source_identity", obj("source_identity").getOrElse(Json.fromString(sensorId)))
            .add("correlation_id", obj("correlation_id").getOrElse(Json.fromString(eventId)))
            .add("causation_id", obj("causation_id").getOrElse(Json.fromString(eventId)))
        )
        .noSpaces
      source = ScanRequestRecord(raw, rawSha, Topic, eventId, observedAt, s"inline://heartbeat/$eventId")
      resolved <- ResolvedScanRequestRecord.from(source, enriched)
    yield Decoded(resolved)
