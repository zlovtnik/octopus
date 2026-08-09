package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.processor.{PreparedSearchDocument, SearchDocumentSource}
import doobie.{ConnectionIO, Query0, Update}
import doobie.implicits.*
import io.circe.Json
import io.circe.syntax.*

object SearchPreparationSql:
  def candidates(limit: Int): Query0[SearchDocumentSource] =
    sql"""SELECT frame.dedupe_key, frame.source_mac, frame.location_id, frame.sensor_id,
                  frame.observed_at, frame.bssid, frame.ssid, frame.frame_subtype,
                  security.security_flags, identity_row.handshake_captured,
                  COALESCE(NULLIF(identity_row.normalized_text, ''), CONCAT_WS(
                    ' ', frame.source_mac, frame.bssid, frame.ssid, frame.frame_subtype,
                    identity_row.wps_device_name, identity_row.wps_manufacturer,
                    identity_row.wps_model_name, network.app_protocol,
                    network.src_ip, network.dst_ip
                  )),
                  JSON_OBJECT(
                    'dedupe_key', frame.dedupe_key,
                    'event_type', identity_row.event_type,
                    'identity_source', identity_row.identity_source,
                    'security_flags', security.security_flags
                  )
           FROM wireless_frames frame
           JOIN wireless_frame_identity identity_row ON identity_row.dedupe_key = frame.dedupe_key
           JOIN wireless_frame_security security ON security.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_network network ON network.dedupe_key = frame.dedupe_key
           LEFT JOIN atheros_search.search_documents document
             ON document.source_table = 'wireless_frames'
            AND document.source_key = frame.dedupe_key
            AND document.status = 'active'
           WHERE document.document_id IS NULL
           ORDER BY frame.observed_at, frame.dedupe_key
           LIMIT ${limit.max(1)}"""
      .query[(String, Option[String], Option[String], Option[String], java.sql.Timestamp,
        Option[String], Option[String], Option[String], Int, Boolean, String, String)]
      .map(SearchDocumentSource.apply.tupled)

  def persist(document: PreparedSearchDocument): ConnectionIO[Unit] =
    val tagsJson = document.tags.map { case (kind, value) =>
      Json.obj("type" -> kind.asJson, "value" -> value.asJson)
    }.asJson.noSpaces
    val metadata = Json.obj(
      "producer" -> "octopus".asJson,
      "normalized_sha256" -> document.normalizedSha256.asJson
    ).noSpaces

    for
      _ <- sql"""UPDATE atheros_search.search_documents
                   SET status = 'superseded', updated_at = CURRENT_TIMESTAMP(6)
                   WHERE source_table = 'wireless_frames'
                     AND source_key = ${document.sourceKey}
                     AND document_id <> ${document.documentId}
                     AND status = 'active'""".update.run
      _ <- sql"""INSERT INTO atheros_search.search_documents (
                   document_id, source_key, source_table, source_kind, source_version,
                   source_mac, location_id, sensor_id, observed_at, bssid, ssid,
                   frame_subtype, tags, detail_json, security_flags, handshake_captured,
                   title, normalized_text, normalized_sha256, locale, status, metadata,
                   created_at, updated_at
                 ) VALUES (
                   ${document.documentId}, ${document.sourceKey}, 'wireless_frames', 'wireless_frame',
                   ${document.sourceVersion}, ${document.sourceMac}, ${document.locationId},
                   ${document.sensorId}, ${document.observedAt}, ${document.bssid}, ${document.ssid},
                   ${document.frameSubtype}, $tagsJson, ${document.detailJson}, ${document.securityFlags},
                   ${document.handshakeCaptured}, ${document.title}, ${document.normalizedText},
                   ${document.normalizedSha256}, 'und', 'active', $metadata,
                   CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
                 ) ON DUPLICATE KEY UPDATE
                   status = 'active',
                   tags = VALUES(tags),
                   detail_json = VALUES(detail_json),
                   title = VALUES(title),
                   normalized_text = VALUES(normalized_text),
                   normalized_sha256 = VALUES(normalized_sha256),
                   source_version = VALUES(source_version),
                   metadata = VALUES(metadata),
                   updated_at = CURRENT_TIMESTAMP(6)""".update.run
      _ <- sql"DELETE FROM atheros_search.search_document_tokens WHERE document_id = ${document.documentId}".update.run
      _ <- TokenInsert.updateMany(document.tokens.map { case (token, frequency, count) =>
        (token, document.documentId, frequency, count)
      }).void
      _ <- sql"DELETE FROM atheros_search.search_document_tags WHERE document_id = ${document.documentId}".update.run
      _ <- TagInsert.updateMany(document.tags.map { case (kind, value) =>
        (document.documentId, kind, value)
      }).void
    yield ()

  def documentsMissingEmbeddingJobs(
      embeddingModel: String,
      limit: Int
  ): Query0[(String, String)] =
    sql"""SELECT document.document_id, document.normalized_sha256
           FROM atheros_search.search_documents document
           LEFT JOIN atheros_search.embedding_jobs job
             ON job.document_id = document.document_id
            AND job.embedding_kind = 'event'
            AND job.embedding_model = $embeddingModel
            AND job.content_sha256 = document.normalized_sha256
           WHERE document.status = 'active'
             AND job.job_id IS NULL
           ORDER BY document.observed_at, document.document_id
           LIMIT ${limit.max(1)}""".query[(String, String)]

  def enqueueEmbeddingJob(
      jobId: String,
      documentId: String,
      contentSha256: String,
      embeddingModel: String
  ): ConnectionIO[Unit] =
    sql"""INSERT INTO atheros_search.embedding_jobs (
             job_id, document_id, embedding_kind, embedding_model, content_sha256,
             status, priority, attempt_count, max_attempts, next_attempt_at,
             created_at, updated_at
           ) VALUES (
             $jobId, $documentId, 'event', $embeddingModel,
             $contentSha256, 'pending', 100, 0, 5,
             CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
           ) ON DUPLICATE KEY UPDATE job_id = embedding_jobs.job_id""".update.run.void

  val TokenInsert: Update[(String, String, Double, Int)] =
    Update[(String, String, Double, Int)](
      """INSERT INTO atheros_search.search_document_tokens (
           token, document_id, field_name, term_frequency, token_count, updated_at
         ) VALUES (?, ?, 'body', ?, ?, CURRENT_TIMESTAMP(6))"""
    )

  val TagInsert: Update[(String, String, String)] =
    Update[(String, String, String)](
      """INSERT INTO atheros_search.search_document_tags (
           document_id, tag_type, tag_value, created_at
         ) VALUES (?, ?, ?, CURRENT_TIMESTAMP(6))"""
    )
