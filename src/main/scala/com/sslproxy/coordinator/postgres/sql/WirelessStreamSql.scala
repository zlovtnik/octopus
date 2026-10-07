package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.config.{SensorCalibration, WirelessProjectionConfig}
import com.sslproxy.coordinator.domain.BrokerRecordMetadata
import com.sslproxy.coordinator.processor.{CalibratedRange, RadioSamples, WirelessObservation}
import doobie.{ConnectionIO, FC}
import doobie.implicits.*
import io.circe.{Json, parser}
import java.sql.Timestamp
import java.time.Instant
import java.nio.charset.StandardCharsets
import java.util.UUID

object WirelessStreamSql:
  private def id(parts: String*): String =
    UUID.nameUUIDFromBytes(parts.map(p => s"${p.length}:$p").mkString.getBytes(StandardCharsets.UTF_8)).toString
  private def ts(value: Instant): Timestamp = Timestamp.from(value)
  private def parse(value: String): ConnectionIO[Json] =
    parser.parse(value).fold(FC.raiseError, _.pure[ConnectionIO])

  /** Called inside one transaction; the consumer commits only after it returns. */
  def project(event: WirelessObservation, meta: BrokerRecordMetadata, config: WirelessProjectionConfig): ConnectionIO[Boolean] =
    for
      now <- sql"SELECT CURRENT_TIMESTAMP".query[Timestamp].unique.map(_.toInstant)
      inserted <- sql"""INSERT INTO octopus_core.wireless_projection_receipts
        (topic, partition_id, offset_id, payload_sha256, disposition, observed_at)
        VALUES (${meta.topic}, ${meta.partition}, ${meta.offset}, ${meta.payloadSha256}, 'received', ${ts(event.observedAt)})
        ON CONFLICT DO NOTHING""".update.run
      stored <- sql"""SELECT payload_sha256 FROM octopus_core.wireless_projection_receipts
        WHERE topic = ${meta.topic} AND partition_id = ${meta.partition} AND offset_id = ${meta.offset}""".query[String].unique
      _ <- if stored == meta.payloadSha256 then ().pure[ConnectionIO]
           else FC.raiseError[Unit](IllegalStateException("wireless broker coordinate payload hash conflict"))
      fresh <- if inserted == 0 then 0.pure[ConnectionIO] else
        sql"""INSERT INTO octopus_core.wireless_projection_hashes (payload_sha256)
          VALUES (${meta.payloadSha256}) ON CONFLICT DO NOTHING""".update.run
      eligible = fresh > 0 && !event.observedAt.isBefore(now.minusSeconds(7 * 86400)) && !event.observedAt.isAfter(now.plusSeconds(60))
      calibration = config.calibrations.toOption.toList.flatten.filter { candidate =>
        candidate.sensorId == event.sensorId &&
        !now.isBefore(candidate.validFrom) && !now.isAfter(candidate.validUntil)
      } match
        case current :: Nil => Some(current)
        // Overlapping configuration is ambiguous. Preserve observations but
        // withhold configured location and every meter claim until reviewed.
        case _ => None
      _ <- if eligible then updateProjection(event, calibration, config.rangesEnabled, now) else ().pure[ConnectionIO]
      disposition = if fresh == 0 then "duplicate" else if eligible then "projected" else "outside_window"
      _ <- if inserted == 0 then 0.pure[ConnectionIO] else sql"""UPDATE octopus_core.wireless_projection_receipts
        SET disposition = $disposition WHERE topic = ${meta.topic} AND partition_id = ${meta.partition} AND offset_id = ${meta.offset}""".update.run
    yield eligible

  private def node(nodeId: String, kind: String, label: String, payload: Json, location: Option[String],
    sensor: Option[String], mac: Option[String], ssid: Option[String], observed: Instant): ConnectionIO[Unit] =
    sql"""INSERT INTO atheros_search.wireless_topology_nodes
      (node_id, node_kind, label, node_payload, location_id, sensor_id, normalized_mac, normalized_ssid,
       observed_at, projection_run_id)
      VALUES ($nodeId, $kind, $label, ${payload.noSpaces}::jsonb, $location, $sensor, $mac, $ssid, ${ts(observed)}, 'wireless-stream-v1')
      ON CONFLICT (node_id) DO UPDATE SET label = EXCLUDED.label, node_payload = EXCLUDED.node_payload,
        location_id = EXCLUDED.location_id, sensor_id = EXCLUDED.sensor_id, normalized_ssid = EXCLUDED.normalized_ssid,
        observed_at = EXCLUDED.observed_at, updated_at = CURRENT_TIMESTAMP
      WHERE EXCLUDED.observed_at >= wireless_topology_nodes.observed_at""".update.run.void

  private def edge(edgeId: String, source: String, target: String, kind: String, label: String,
    evidence: Json, observed: Instant, expires: Option[Instant]): ConnectionIO[Unit] =
    sql"""INSERT INTO atheros_search.wireless_topology_edges
      (edge_id, source_node_id, target_node_id, edge_kind, label, evidence, observed_at, projection_run_id, expires_at)
      VALUES ($edgeId, $source, $target, $kind, $label, ${evidence.noSpaces}::jsonb, ${ts(observed)}, 'wireless-stream-v1', ${expires.map(ts)})
      ON CONFLICT (edge_id) DO UPDATE SET evidence = EXCLUDED.evidence, observed_at = EXCLUDED.observed_at,
        expires_at = EXCLUDED.expires_at, updated_at = CURRENT_TIMESTAMP""".update.run.void

  private def updateProjection(e: WirelessObservation, calibration: Option[SensorCalibration], ranges: Boolean,
    now: Instant): ConnectionIO[Unit] =
    val location = calibration.map(_.locationId)
    val sensorNode = "sensor:" + e.sensorId
    for
      _ <- node(sensorNode, "sensor", e.sensorId, Json.obj(), location, Some(e.sensorId), None, None, e.observedAt)
      _ <- location.traverse_ { loc =>
        node("location:" + loc, "location", loc, Json.obj(), Some(loc), None, None, None, e.observedAt) *>
          edge(id("contains", loc, sensorNode), "location:" + loc, sensorNode, "containment", "Configured sensor location",
            Json.obj(), e.observedAt, None)
      }
      _ <- e.bssid.traverse_(ap => node("ap:" + ap, "access_point", e.ssid.getOrElse(ap),
        Json.obj("bssid" -> Json.fromString(ap), "ssid" -> e.ssid.fold(Json.Null)(Json.fromString)),
        None, None, Some(ap), e.ssid, e.observedAt))
      _ <- calibration.traverse_ { c =>
        e.bssid.filter(c.accessPoints.contains).traverse_(ap =>
          edge(id("contains", c.locationId, "ap:" + ap), "location:" + c.locationId, "ap:" + ap,
            "containment", "Reviewed AP location", Json.obj(), e.observedAt, None))
      }
      _ <- e.sourceMac.filterNot(mac => e.bssid.contains(mac)).traverse_ { mac =>
        sql"""INSERT INTO atheros_search.devices (mac, first_seen, last_seen)
          VALUES ($mac, ${ts(e.observedAt)}, ${ts(e.observedAt)}) ON CONFLICT (mac) DO UPDATE SET
          first_seen = LEAST(devices.first_seen, EXCLUDED.first_seen), last_seen = GREATEST(devices.last_seen, EXCLUDED.last_seen),
          updated_at = CURRENT_TIMESTAMP""".update.run *>
        node("device:" + mac, "device", mac, Json.obj("mac" -> Json.fromString(mac)), None, None, Some(mac), None, e.observedAt) *>
        e.bssid.traverse_(ap => summarize(e, mac, ap, calibration, ranges, now))
      }
    yield ()

  private def summarize(e: WirelessObservation, mac: String, ap: String, calibration: Option[SensorCalibration],
    ranges: Boolean, now: Instant): ConnectionIO[Unit] =
    val start = Instant.ofEpochSecond(Math.floorDiv(e.observedAt.getEpochSecond, 300) * 300)
    val end = start.plusSeconds(300)
    val expires = end.plusSeconds(7 * 86400)
    val summaryId = id(e.sensorId, ap, mac, start.toString)
    for
      // Insert the lock target, then serialize read/modify/write for this window.
      inserted <- sql"""INSERT INTO atheros_search.wireless_observation_summaries
        (summary_id, sensor_id, bssid, source_mac, window_start, window_end, first_seen, last_seen,
         frame_count, counters, radio, location_id, ssid, expires_at)
        VALUES ($summaryId::uuid, ${e.sensorId}, $ap, $mac, ${ts(start)}, ${ts(end)}, ${ts(e.observedAt)}, ${ts(e.observedAt)},
          1, '{}'::jsonb, '{}'::jsonb, ${calibration.map(_.locationId)}, ${e.ssid}, ${ts(expires)})
        ON CONFLICT DO NOTHING""".update.run
      row <- sql"""SELECT frame_count, counters::text, radio::text, first_seen, last_seen
        FROM atheros_search.wireless_observation_summaries WHERE summary_id = $summaryId::uuid FOR UPDATE"""
        .query[(Long, String, String, Timestamp, Timestamp)].unique
      counters <- parse(row._2)
      radio <- parse(row._3)
      count = if inserted == 1 then 1L else row._1 + 1
      first = if e.observedAt.isBefore(row._4.toInstant) then e.observedAt else row._4.toInstant
      last = if e.observedAt.isAfter(row._5.toInstant) then e.observedAt else row._5.toInstant
      nextCounters = increment(counters, List("frames", "frame:" + boundedFrame(e.frameType), "protocol:" + boundedProtocol(e.protocol)) ++
        Option.when(e.retry)("retry") ++ Option.when(e.protectedFrame)("protected") ++ Option.when(e.handshake)("handshake"))
      nextRadio = addRadio(radio, e)
      _ <- sql"""UPDATE atheros_search.wireless_observation_summaries SET frame_count = $count,
        counters = ${nextCounters.noSpaces}::jsonb, radio = ${nextRadio.noSpaces}::jsonb,
        first_seen = ${ts(first)}, last_seen = ${ts(last)}, updated_at = CURRENT_TIMESTAMP
        WHERE summary_id = $summaryId::uuid""".update.run
      evidence = Json.obj("summary_id" -> Json.fromString(summaryId), "sensor_id" -> Json.fromString(e.sensorId),
        "window_start" -> Json.fromString(start.toString), "window_end" -> Json.fromString(end.toString),
        "expires_at" -> Json.fromString(expires.toString), "projection_watermark" -> Json.fromString(last.toString),
        "projected_at" -> Json.fromString(now.toString), "frame_count" -> Json.fromLong(count))
      _ <- edge("observation:" + summaryId, "device:" + mac, "ap:" + ap, "observed_association",
        "Observed association; connection and ownership unverified", evidence, last, Some(expires))
      _ <- sql"DELETE FROM atheros_search.wireless_topology_edges WHERE edge_id = ${"range:" + summaryId}".update.run
      _ <- Option.when(ranges)(calibration).flatten.traverse_ { c =>
        // Never blend channels or attribute receiver RSSI to the source device.
        val estimates = nextRadio.asObject.toList.flatMap(_.values).flatMap { bin =>
          val h = bin.hcursor
          for
            n <- h.get[Long]("sample_count").toOption
            sum <- h.get[Double]("sum").toOption
            squares <- h.get[Double]("squares").toOption
            freq <- h.get[Int]("frequency_mhz").toOption
            channel <- h.get[Int]("channel").toOption
            binFirst <- h.get[Instant]("first_seen").toOption
            binLast <- h.get[Instant]("last_seen").toOption
            range <- CalibratedRange.estimate(RadioSamples(n, sum, squares), c, freq, channel, binFirst, binLast, now)
          yield range
        }
        // More than one eligible radio context is ambiguous: withhold meters.
        estimates match
          case range :: Nil => edge("range:" + summaryId, "sensor:" + e.sensorId, "device:" + mac,
            "calibrated_range", "Estimated sensor-to-device range", evidence.deepMerge(Json.obj("range" -> range)), last, Some(expires))
          case _ => ().pure[ConnectionIO]
      }
    yield ()

  private def boundedFrame(value: String): String =
    if Set("management", "control", "data").contains(value) then value else "unknown"
  private def boundedProtocol(value: String): String =
    if Set("dns", "mdns", "dhcp", "ssdp", "http", "https", "tls", "eapol", "arp", "tcp", "udp").contains(value.toLowerCase) then value.toLowerCase else "other"
  private def increment(json: Json, keys: List[String]): Json =
    keys.foldLeft(json)((acc, key) => acc.mapObject(_.add(key, Json.fromLong(acc.hcursor.get[Long](key).getOrElse(0L) + 1))))

  private def addRadio(json: Json, e: WirelessObservation): Json =
    (e.signalDbm, e.frequencyMhz, e.channel) match
      case (Some(rssi), Some(frequency), Some(channel)) if e.transmitterMac == e.sourceMac && e.sourceMac.nonEmpty &&
          frequency >= 2300 && frequency <= 7200 && channel > 0 && channel <= 233 =>
        val key = s"$frequency/$channel"
        val old = json.hcursor.downField(key)
        val samples = RadioSamples(old.get[Long]("sample_count").getOrElse(0L), old.get[Double]("sum").getOrElse(0d),
          old.get[Double]("squares").getOrElse(0d)).add(rssi)
        val first = old.get[Instant]("first_seen").toOption.filter(_.isBefore(e.observedAt)).getOrElse(e.observedAt)
        val last = old.get[Instant]("last_seen").toOption.filter(_.isAfter(e.observedAt)).getOrElse(e.observedAt)
        val bin = Json.obj("sample_count" -> Json.fromLong(samples.count), "sum" -> Json.fromDoubleOrNull(samples.sum),
          "squares" -> Json.fromDoubleOrNull(samples.squares), "mean_dbm" -> Json.fromDoubleOrNull(samples.mean),
          "stddev_db" -> Json.fromDoubleOrNull(samples.stddev), "frequency_mhz" -> Json.fromInt(frequency),
          "channel" -> Json.fromInt(channel), "first_seen" -> Json.fromString(first.toString), "last_seen" -> Json.fromString(last.toString))
        // Bounded per-window radio material, even for corrupt or hostile input.
        if old.focus.nonEmpty || json.asObject.exists(_.size < 64) then json.mapObject(_.add(key, bin)) else json
      case _ => json

  def expire(limit: Int): ConnectionIO[Int] =
    for
      ids <- sql"""SELECT summary_id::text FROM atheros_search.wireless_observation_summaries
        WHERE expires_at <= CURRENT_TIMESTAMP ORDER BY expires_at, summary_id LIMIT ${limit.max(1)} FOR UPDATE SKIP LOCKED""".query[String].to[List]
      _ <- ids.traverse_ { summaryId =>
        for
          documents <- sql"""SELECT document_id FROM atheros_search.search_documents
            WHERE source_kind = 'observation_window' AND source_key = $summaryId FOR UPDATE""".query[String].to[List]
          _ <- documents.traverse_(deleteDocument)
          _ <- sql"""DELETE FROM atheros_search.wireless_topology_edges
            WHERE edge_id IN (${"observation:" + summaryId}, ${"range:" + summaryId})""".update.run
          _ <- sql"DELETE FROM atheros_search.wireless_observation_summaries WHERE summary_id = $summaryId::uuid".update.run
        yield ()
      }
      _ <- sql"""DELETE FROM octopus_core.wireless_projection_receipts WHERE (topic, partition_id, offset_id) IN
        (SELECT topic, partition_id, offset_id FROM octopus_core.wireless_projection_receipts WHERE expires_at <= CURRENT_TIMESTAMP LIMIT ${limit.max(1)})""".update.run
      _ <- sql"""DELETE FROM octopus_core.wireless_projection_hashes WHERE payload_sha256 IN
        (SELECT payload_sha256 FROM octopus_core.wireless_projection_hashes WHERE expires_at <= CURRENT_TIMESTAMP LIMIT ${limit.max(1)})""".update.run
    yield ids.size

  def deleteDocument(documentId: String): ConnectionIO[Unit] =
    for
      // Locking/deleting jobs fences an in-flight worker before removing vectors.
      _ <- sql"DELETE FROM atheros_search.embedding_jobs WHERE document_id = $documentId".update.run
      _ <- sql"DELETE FROM atheros_search.search_vectors_device WHERE document_id = $documentId".update.run
      _ <- sql"DELETE FROM atheros_search.search_vectors_behaviour WHERE document_id = $documentId".update.run
      _ <- sql"DELETE FROM atheros_search.embeddings WHERE document_id = $documentId".update.run
      _ <- sql"DELETE FROM atheros_search.search_document_tokens WHERE document_id = $documentId".update.run
      _ <- sql"DELETE FROM atheros_search.search_document_tags WHERE document_id = $documentId".update.run
      _ <- sql"DELETE FROM atheros_search.search_documents WHERE document_id = $documentId".update.run
    yield ()
