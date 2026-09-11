package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.domain.{BrokerRecordMetadata, ResolvedScanRequestRecord}
import com.sslproxy.coordinator.util.Sha256Utils
import doobie.{ConnectionIO, Update0}
import doobie.implicits.*
import doobie.postgres.implicits.*
import io.circe.{Json, JsonObject}
import io.circe.parser.parse

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.OffsetDateTime
import java.util.{Locale, UUID}

object CanonicalIngestionSql:
  final case class Applied(eventId: String, applied: Boolean)

  private final case class WirelessEvent(
    eventId: String,
    eventType: String,
    schemaVersion: Int,
    observedAt: Timestamp,
    producedAt: Timestamp,
    sensorId: String,
    locationId: Option[String],
    correlationId: String,
    causationId: String,
    sourceMac: Option[String],
    transmitterMac: Option[String],
    receiverMac: Option[String],
    bssid: Option[String],
    destinationBssid: Option[String],
    ssid: Option[String],
    signalDbm: Option[Int],
    frequencyMhz: Option[Int],
    channelNumber: Option[Int],
    securityFlags: Int,
    handshakeCaptured: Boolean,
    payload: Json,
    payloadSha256: String
  )

  private val MacPattern = "^[0-9a-f]{2}(:[0-9a-f]{2}){5}$".r
  private val BroadcastMac = "ff:ff:ff:ff:ff:ff"
  private val ProxyClassifications =
    Set("ads_tracker", "analytics", "cdn", "essential_api", "auth", "unknown")

  def applyRecord(
    record: ResolvedScanRequestRecord,
    metadata: BrokerRecordMetadata,
    embeddingModel: String
  ): ConnectionIO[Applied] =
    for
      payload <- parsePayload(record.payloadJson)
      eventId = text(payload, "event_id").getOrElse(record.dedupeKey)
      _ <- validateEventId(eventId)
      claimed <- claimReceipt(record, metadata, eventId)
      applied <- claimed match
        case None => verifyDuplicate(record, metadata, eventId).as(false)
        case Some(_) =>
          val persist =
            if isWireless(record.streamName) then
              parseWireless(record, payload, eventId).flatMap(persistWireless(_, metadata, embeddingModel))
            else persistProxy(record, payload, eventId)
          persist *> completeReceipt(metadata) *> advanceCheckpoint(metadata).as(true)
    yield Applied(eventId, applied)

  private def parsePayload(value: String): ConnectionIO[Json] =
    parse(value) match
      case Right(json) if json.isObject => json.pure[ConnectionIO]
      case Right(_) =>
        doobie.free.connection.raiseError(IllegalArgumentException("ingestion payload must be a JSON object"))
      case Left(error) => doobie.free.connection.raiseError(error)

  private def validateEventId(eventId: String): ConnectionIO[Unit] =
    if eventId.nonEmpty && eventId.length <= 255 then ().pure[ConnectionIO]
    else doobie.free.connection.raiseError(IllegalArgumentException("event_id must contain 1 to 255 characters"))

  private def claimReceipt(
    record: ResolvedScanRequestRecord,
    metadata: BrokerRecordMetadata,
    eventId: String
  ): ConnectionIO[Option[String]] =
    sql"""INSERT INTO ingestion_receipts (
             consumer_group, topic, partition_id, record_offset, event_id,
             payload_sha256, artifact_sha256, schema_version, disposition,
             received_at
           ) VALUES (
             ${metadata.consumerGroup}, ${metadata.topic}, ${metadata.partition}, ${metadata.offset},
             $eventId, ${record.eventPayloadSha256}, ${metadata.artifactSha256},
             ${schemaVersion(record.payloadJson)}, 'processing', CURRENT_TIMESTAMP
           ) ON CONFLICT DO NOTHING
           RETURNING event_id""".query[String].option

  private def verifyDuplicate(
    record: ResolvedScanRequestRecord,
    metadata: BrokerRecordMetadata,
    eventId: String
  ): ConnectionIO[Unit] =
    sql"""SELECT event_id, payload_sha256
           FROM ingestion_receipts
           WHERE (
             consumer_group = ${metadata.consumerGroup}
             AND topic = ${metadata.topic}
             AND partition_id = ${metadata.partition}
             AND record_offset = ${metadata.offset}
           ) OR (topic = ${metadata.topic} AND event_id = $eventId)"""
      .query[(String, String)]
      .to[List]
      .flatMap { rows =>
        if rows.nonEmpty && rows.forall(row => row._1 == eventId && row._2 == record.eventPayloadSha256) then
          ().pure[ConnectionIO]
        else
          doobie.free.connection.raiseError(
            IllegalStateException("ingestion receipt conflict changed event identity or payload hash")
          )
      }

  private def completeReceipt(metadata: BrokerRecordMetadata): ConnectionIO[Unit] =
    sql"""UPDATE ingestion_receipts
           SET disposition = 'processed', processed_at = CURRENT_TIMESTAMP
           WHERE consumer_group = ${metadata.consumerGroup}
             AND topic = ${metadata.topic}
             AND partition_id = ${metadata.partition}
             AND record_offset = ${metadata.offset}
             AND disposition = 'processing'""".update.run.flatMap {
      case 1 => ().pure[ConnectionIO]
      case count =>
        doobie.free.connection.raiseError(
          IllegalStateException(s"receipt completion updated $count rows")
        )
    }

  private def advanceCheckpoint(metadata: BrokerRecordMetadata): ConnectionIO[Unit] =
    val nextOffset = metadata.offset + 1L
    sql"""INSERT INTO consumer_checkpoints (
             consumer_group, topic, partition_id, next_offset, group_version,
             artifact_sha256, updated_at
           ) VALUES (
             ${metadata.consumerGroup}, ${metadata.topic}, ${metadata.partition}, $nextOffset,
             ${metadata.groupVersion.toString}, ${metadata.artifactSha256}, CURRENT_TIMESTAMP
           ) ON CONFLICT (consumer_group, topic, partition_id) DO UPDATE SET
             next_offset = GREATEST(consumer_checkpoints.next_offset, EXCLUDED.next_offset),
             group_version = CASE
               WHEN EXCLUDED.next_offset > consumer_checkpoints.next_offset THEN EXCLUDED.group_version
               ELSE consumer_checkpoints.group_version
             END,
             artifact_sha256 = CASE
               WHEN EXCLUDED.next_offset > consumer_checkpoints.next_offset THEN EXCLUDED.artifact_sha256
               ELSE consumer_checkpoints.artifact_sha256
             END,
             updated_at = CURRENT_TIMESTAMP""".update.run.void

  private def parseWireless(
    record: ResolvedScanRequestRecord,
    payload: Json,
    eventId: String
  ): ConnectionIO[WirelessEvent] =
    val eventType = text(payload, "event_type").orElse(text(payload, "type")).getOrElse(record.streamName)
    val observed = text(payload, "occurred_at").orElse(text(payload, "observed_at")).getOrElse(record.observedAt)
    val produced = text(payload, "produced_at").getOrElse(observed)
    val sensorId = text(payload, "source_identity").orElse(text(payload, "sensor_id"))
    val parsed = for
      observedAt <- timestamp(observed, "occurred_at")
      producedAt <- timestamp(produced, "produced_at")
      sensor <- sensorId.filter(_.nonEmpty).toRight("source_identity or sensor_id is required")
      version <- integer(payload, "schema_version").filter(_ > 0).toRight("schema_version must be positive")
      sourceMac <- normalizedMac(payload, "source_mac")
      transmitterMac <- normalizedMac(payload, "transmitter_mac")
      receiverMac <- normalizedMac(payload, "receiver_mac")
      bssid <- normalizedMac(payload, "bssid")
      destinationBssid <- normalizedMac(payload, "destination_bssid", "destination_mac")
    yield WirelessEvent(
      eventId,
      eventType,
      version,
      observedAt,
      producedAt,
      sensor,
      text(payload, "location_id"),
      text(payload, "correlation_id").getOrElse(eventId),
      text(payload, "causation_id").getOrElse(eventId),
      sourceMac,
      transmitterMac,
      receiverMac,
      bssid,
      destinationBssid,
      text(payload, "ssid"),
      integer(payload, "signal_dbm"),
      integer(payload, "frequency_mhz"),
      integer(payload, "channel_number").orElse(integer(payload, "channel")),
      integer(payload, "security_flags").getOrElse(0),
      boolean(payload, "handshake_captured").getOrElse(false),
      payload,
      record.eventPayloadSha256
    )

    parsed.fold(
      message => doobie.free.connection.raiseError(IllegalArgumentException(message)),
      _.pure[ConnectionIO]
    )

  private def persistWireless(
    event: WirelessEvent,
    metadata: BrokerRecordMetadata,
    embeddingModel: String
  ): ConnectionIO[Unit] =
    for
      _ <- insertObservation(event, metadata).run
      _ <- upsertSensor(event).run
      _ <- observedMacs(event).traverse_(mac => upsertInventoryDevice(event, mac).run.void)
      _ <- alert(event).traverse_(a => insertAlert(event, a))
      _ <- insertSearchDocumentAndJob(event, embeddingModel)
    yield ()

  private def insertObservation(event: WirelessEvent, metadata: BrokerRecordMetadata): Update0 =
    sql"""INSERT INTO wireless_observations (
             event_id, schema_version, observation_subtype, observed_at, produced_at,
             sensor_id, location_id, correlation_id, causation_id, source_mac,
             transmitter_mac, receiver_mac, bssid, destination_bssid, ssid,
             signal_dbm, frequency_mhz, channel_number, security_flags,
             handshake_captured, payload, payload_sha256,
             receipt_consumer_group, receipt_topic, receipt_partition_id,
             receipt_record_offset, created_at
           ) VALUES (
             ${event.eventId}, ${event.schemaVersion}, ${event.eventType}, ${event.observedAt},
             ${event.producedAt}, ${event.sensorId}, ${event.locationId}, ${event.correlationId},
             ${event.causationId}, ${event.sourceMac}, ${event.transmitterMac}, ${event.receiverMac},
             ${event.bssid}, ${event.destinationBssid}, ${event.ssid}, ${event.signalDbm},
             ${event.frequencyMhz}, ${event.channelNumber}, ${event.securityFlags},
             ${event.handshakeCaptured}, CAST(${event.payload.noSpaces} AS jsonb),
             ${event.payloadSha256}, ${metadata.consumerGroup}, ${metadata.topic},
             ${metadata.partition}, ${metadata.offset}, CURRENT_TIMESTAMP
           ) ON CONFLICT (event_id) DO NOTHING""".update

  private def upsertSensor(event: WirelessEvent): Update0 =
    val capabilities = event.payload.hcursor.downField("capabilities").focus.filter(_.isObject).getOrElse(Json.obj())
    sql"""INSERT INTO sensors (
             sensor_id, location_id, capabilities, metadata, first_seen_at,
             last_seen_at, last_heartbeat_at, status, updated_at
           ) VALUES (
             ${event.sensorId}, ${event.locationId}, CAST(${capabilities.noSpaces} AS jsonb),
             '{}'::jsonb, ${event.observedAt}, ${event.observedAt},
             ${Option.when(event.eventType == "sensor_heartbeat")(event.observedAt)},
             ${if event.eventType == "sensor_heartbeat" then "online" else "observed"},
             CURRENT_TIMESTAMP
           ) ON CONFLICT (sensor_id) DO UPDATE SET
             location_id = COALESCE(EXCLUDED.location_id, sensors.location_id),
             capabilities = CASE
               WHEN EXCLUDED.capabilities = '{}'::jsonb THEN sensors.capabilities
               ELSE EXCLUDED.capabilities
             END,
             first_seen_at = LEAST(sensors.first_seen_at, EXCLUDED.first_seen_at),
             last_seen_at = GREATEST(sensors.last_seen_at, EXCLUDED.last_seen_at),
             last_heartbeat_at = GREATEST(
               COALESCE(sensors.last_heartbeat_at, EXCLUDED.last_heartbeat_at),
               COALESCE(EXCLUDED.last_heartbeat_at, sensors.last_heartbeat_at)
             ),
             status = CASE
               WHEN EXCLUDED.last_heartbeat_at IS NOT NULL THEN 'online'
               ELSE sensors.status
             END,
             updated_at = CURRENT_TIMESTAMP""".update

  private def observedMacs(event: WirelessEvent): List[String] =
    List(event.sourceMac, event.transmitterMac, event.receiverMac, event.bssid, event.destinationBssid)
      .flatten
      .filterNot(_ == BroadcastMac)
      .distinct

  private def upsertInventoryDevice(event: WirelessEvent, mac: String): Update0 =
    val knownMacs = Json.arr(Json.fromString(mac)).noSpaces
    sql"""INSERT INTO atheros_search.devices (
             mac, location_id, first_seen, last_seen, active, registered,
             tags, known_macs, updated_at
           ) VALUES (
             $mac, ${event.locationId}, ${event.observedAt}, ${event.observedAt},
             true, false, '[]'::jsonb, CAST($knownMacs AS jsonb), CURRENT_TIMESTAMP
           ) ON CONFLICT (mac) DO UPDATE SET
             location_id = COALESCE(EXCLUDED.location_id, devices.location_id),
             first_seen = LEAST(devices.first_seen, EXCLUDED.first_seen),
             last_seen = GREATEST(devices.last_seen, EXCLUDED.last_seen),
             active = true,
             updated_at = CURRENT_TIMESTAMP""".update

  private final case class Alert(subjectKind: String, subjectId: String, severity: String, evidence: Json)

  private def alert(event: WirelessEvent): Option[Alert] =
    val normalized = event.eventType.toLowerCase(Locale.ROOT)
    val spec = normalized match
      case value if value.contains("rogue_ap") => Some(("access_point", event.bssid.orElse(event.sourceMac), "high"))
      case value if value.contains("deauth") => Some(("access_point", event.bssid.orElse(event.sourceMac), "high"))
      case value if value.contains("pmf_attack") => Some(("access_point", event.bssid.orElse(event.sourceMac), "critical"))
      case value if value.contains("signal_anomaly") => Some(("station", event.sourceMac.orElse(event.transmitterMac), "medium"))
      case value if value.contains("attack_sequence") => Some(("station", event.sourceMac.orElse(event.transmitterMac), "critical"))
      case value if value.contains("sequence_alert") => Some(("station", event.sourceMac.orElse(event.transmitterMac), "high"))
      case value if value.contains("handshake") => Some(("access_point", event.bssid.orElse(event.sourceMac), "medium"))
      case _ => None
    spec.flatMap { case (kind, subject, severity) =>
      subject.filter(_.nonEmpty).map { id =>
        Alert(
          kind,
          id,
          severity,
          Json.obj(
            "event_type" -> Json.fromString(event.eventType),
            "sensor_id" -> Json.fromString(event.sensorId),
            "location_id" -> event.locationId.fold(Json.Null)(Json.fromString),
            "signal_dbm" -> event.signalDbm.fold(Json.Null)(Json.fromInt),
            "bssid" -> event.bssid.fold(Json.Null)(Json.fromString),
            "source_mac" -> event.sourceMac.fold(Json.Null)(Json.fromString)
          )
        )
      }
    }

  private def insertAlert(event: WirelessEvent, alert: Alert): ConnectionIO[Unit] =
    val alertId = stableUuid(s"alert:${event.eventId}:${event.eventType}")
    sql"""INSERT INTO wireless_alerts (
             alert_id, alert_type, subject_kind, subject_id, severity,
             evidence, source_event_id, detected_at, created_at, updated_at
           ) VALUES (
             $alertId, ${event.eventType}, ${alert.subjectKind}, ${alert.subjectId},
             ${alert.severity}, CAST(${alert.evidence.noSpaces} AS jsonb),
             ${event.eventId}, ${event.observedAt}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           ) ON CONFLICT (source_event_id, alert_type) DO NOTHING""".update.run.void

  private def insertSearchDocumentAndJob(event: WirelessEvent, embeddingModel: String): ConnectionIO[Unit] =
    val textValue = List(
      Some("kind: event"),
      Some(event.eventType),
      event.sourceMac,
      event.bssid,
      event.ssid,
      event.locationId,
      Some(event.sensorId)
    ).flatten.map(_.trim.toLowerCase(Locale.ROOT)).filter(_.nonEmpty).mkString(" ")
    val checksum = Sha256Utils.sha256Hex(textValue.getBytes(StandardCharsets.UTF_8))
    val documentId = stableUuid(s"event:${event.eventId}:$checksum")
    val jobId = stableUuid(s"embedding:$documentId:event:$embeddingModel:$checksum")
    val filters = JsonObject.fromIterable(
      List(
        "source_mac" -> event.sourceMac.fold(Json.Null)(Json.fromString),
        "location_id" -> event.locationId.fold(Json.Null)(Json.fromString),
        "sensor_id" -> Json.fromString(event.sensorId),
        "bssid" -> event.bssid.fold(Json.Null)(Json.fromString),
        "ssid" -> event.ssid.fold(Json.Null)(Json.fromString),
        "frame_subtype" -> Json.fromString(event.eventType),
        "tags" -> event.payload.hcursor.downField("tags").focus.filter(_.isArray).getOrElse(Json.arr())
      )
    )
    for
      _ <- sql"""INSERT INTO atheros_search.search_documents (
                   document_id, source_kind, source_id, source_version, source_mac,
                   location_id, sensor_id, observed_at, bssid, ssid, frame_subtype,
                   security_flags, handshake_captured, title, normalized_text,
                   normalized_sha256, search_vector, filters, detail_json, status,
                   created_at, updated_at
                 ) VALUES (
                   $documentId, 'event', ${event.eventId}, 1, ${event.sourceMac},
                   ${event.locationId}, ${event.sensorId}, ${event.observedAt}, ${event.bssid},
                   ${event.ssid}, ${event.eventType}, ${event.securityFlags},
                   ${event.handshakeCaptured}, ${event.ssid.orElse(Some(event.eventType))},
                   $textValue, $checksum, to_tsvector('simple', $textValue),
                   CAST(${Json.fromJsonObject(filters).noSpaces} AS jsonb),
                   CAST(${event.payload.noSpaces} AS jsonb), 'active',
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                 ) ON CONFLICT (document_id) DO NOTHING""".update.run
      _ <- sql"""INSERT INTO atheros_search.embedding_jobs (
                   job_id, document_id, embedding_kind, embedding_model,
                   content_sha256, status, priority, attempt_count, max_attempts,
                   next_attempt_at, created_at, updated_at
                 ) VALUES (
                   $jobId, $documentId, 'event', $embeddingModel, $checksum,
                   'pending', 100, 0, 5, CURRENT_TIMESTAMP,
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                 ) ON CONFLICT (document_id, embedding_kind, embedding_model, content_sha256)
                   DO NOTHING""".update.run
    yield ()

  private def persistProxy(record: ResolvedScanRequestRecord, payload: Json, eventId: String): ConnectionIO[Unit] =
    val eventTimeValue = text(payload, "occurred_at").orElse(text(payload, "event_time")).orElse(text(payload, "observed_at")).getOrElse(record.observedAt)
    val eventTime = timestamp(eventTimeValue, "occurred_at")
    val host = text(payload, "host").filter(_.nonEmpty).toRight("proxy event host is required")
    val validated = (eventTime, host).mapN { (time, hostValue) =>
      val classification = text(payload, "classification").orElse(text(payload, "category"))
        .filter(ProxyClassifications.contains)
        .getOrElse("unknown")
      sql"""INSERT INTO proxy_events (
               event_id, event_time, event_type, host, peer_ip, wireguard_pubkey,
               bytes_up, bytes_down, status_code, blocked, classification,
               correlation_id, causation_id, payload, created_at
             ) VALUES (
               $eventId, $time, ${text(payload, "event_type").getOrElse(record.streamName)},
               $hostValue, ${text(payload, "peer_ip")}, ${text(payload, "wg_pubkey")},
               ${long(payload, "bytes_up").getOrElse(0L)}, ${long(payload, "bytes_down").getOrElse(0L)},
               ${integer(payload, "status_code")}, ${boolean(payload, "blocked").getOrElse(false)},
               $classification, ${text(payload, "correlation_id").orElse(Some(eventId))},
               ${text(payload, "causation_id").orElse(Some(eventId))},
               CAST(${payload.noSpaces} AS jsonb), CURRENT_TIMESTAMP
             ) ON CONFLICT (event_id) DO NOTHING""".update.run.void
    }
    validated.fold(
      message => doobie.free.connection.raiseError(IllegalArgumentException(message)),
      identity
    )

  private def isWireless(streamName: String): Boolean =
    streamName.startsWith("wireless.") || streamName.startsWith("audit.wireless.")

  private def schemaVersion(payloadJson: String): Option[Int] =
    parse(payloadJson).toOption.flatMap(integer(_, "schema_version"))

  private def text(json: Json, name: String): Option[String] =
    json.hcursor.get[String](name).toOption.map(_.trim).filter(_.nonEmpty)

  private def integer(json: Json, name: String): Option[Int] =
    json.hcursor.get[Int](name).toOption

  private def long(json: Json, name: String): Option[Long] =
    json.hcursor.get[Long](name).toOption

  private def boolean(json: Json, name: String): Option[Boolean] =
    json.hcursor.get[Boolean](name).toOption

  private def timestamp(value: String, field: String): Either[String, Timestamp] =
    Either
      .catchOnly[java.time.format.DateTimeParseException](Timestamp.from(OffsetDateTime.parse(value).toInstant))
      .leftMap(_ => s"$field must be RFC3339")

  private def normalizedMac(json: Json, names: String*): Either[String, Option[String]] =
    val value = names.iterator.flatMap(name => text(json, name)).toList.headOption.map(_.toLowerCase(Locale.ROOT))
    value match
      case Some(mac) if !MacPattern.matches(mac) => Left(s"${names.headOption.getOrElse("mac")} must be a canonical MAC address")
      case other => Right(other)

  private def stableUuid(value: String): UUID =
    UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8))
