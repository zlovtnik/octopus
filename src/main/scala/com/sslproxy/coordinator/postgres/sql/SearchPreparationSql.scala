package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.processor.{PreparedSearchDocument, SearchDocumentKind, SearchDocumentSource}
import doobie.{ConnectionIO, Query0, Update}
import doobie.implicits.*
import io.circe.Json
import io.circe.syntax.*

object SearchPreparationSql:
  val supportedKinds: List[SearchDocumentKind] = SearchDocumentKind.values.toList

  def candidates(kind: SearchDocumentKind, limit: Int): Query0[SearchDocumentSource] =
    kind match
      case SearchDocumentKind.Event => eventCandidates(limit)
      case SearchDocumentKind.Device => deviceCandidates(limit)
      case SearchDocumentKind.Behaviour => behaviourCandidates(limit)
      case SearchDocumentKind.Sequence => sequenceCandidates(limit)

  private def eventCandidates(limit: Int): Query0[SearchDocumentSource] =
    sql"""SELECT frame.dedupe_key, frame.source_mac, frame.location_id, frame.sensor_id,
                  frame.observed_at, frame.bssid, frame.ssid, frame.frame_subtype,
                  security.security_flags, identity_row.handshake_captured,
                  COALESCE(NULLIF(identity_row.normalized_text, ''), CONCAT_WS(
                    ' ', frame.source_mac, frame.bssid, frame.ssid, frame.frame_subtype,
                    identity_row.wps_device_name, identity_row.wps_manufacturer,
                    identity_row.wps_model_name, network.app_protocol,
                    network.src_ip, network.dst_ip
                  )),
                  jsonb_build_object(
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
            AND document.source_kind = 'event'
            AND document.status = 'active'
           WHERE document.document_id IS NULL
           ORDER BY frame.observed_at, frame.dedupe_key
           LIMIT ${limit.max(1)}"""
      .query[
        (
          String,
          Option[String],
          Option[String],
          Option[String],
          java.sql.Timestamp,
          Option[String],
          Option[String],
          Option[String],
          Int,
          Boolean,
          String,
          String
        )
      ]
      .map(row =>
        SearchDocumentSource(
          SearchDocumentKind.Event,
          row._1,
          row._2,
          row._3,
          row._4,
          row._5,
          row._6,
          row._7,
          row._8,
          row._9,
          row._10,
          row._11,
          row._12
        )
      )

  private def deviceCandidates(limit: Int): Query0[SearchDocumentSource] =
    sql"""SELECT device.mac, device.mac, device.location_id, CAST(NULL AS TEXT),
                  device.last_seen, CAST(NULL AS TEXT), CAST(NULL AS TEXT), CAST(NULL AS TEXT),
                  0, FALSE,
                  CONCAT_WS(' ', 'kind: device', device.mac, device.display_name, device.owner_id,
                    device.location_id, cluster.cluster_name,
                    CASE WHEN cluster.cluster_size IS NULL THEN NULL
                         ELSE CONCAT('cluster_size: ', cluster.cluster_size) END),
                  jsonb_build_object(
                    'registered', device.registered,
                    'active', device.active,
                    'owner_id', device.owner_id,
                    'similarity_cluster_id', device.similarity_cluster_id,
                    'dedup_confidence', device.dedup_confidence,
                    'known_macs', device.known_macs,
                    'tags', device.tags
                  )
           FROM atheros_search.inventory_devices device
           LEFT JOIN atheros_search.identity_clusters cluster
             ON cluster.cluster_id = device.similarity_cluster_id
           LEFT JOIN atheros_search.search_documents document
             ON document.source_table = 'inventory_devices'
            AND document.source_key = device.mac
            AND document.source_kind = 'device'
            AND document.status = 'active'
           WHERE document.document_id IS NULL OR document.updated_at < device.updated_at
           ORDER BY device.last_seen, device.mac
           LIMIT ${limit.max(1)}"""
      .query[
        (
          String,
          Option[String],
          Option[String],
          Option[String],
          java.sql.Timestamp,
          Option[String],
          Option[String],
          Option[String],
          Int,
          Boolean,
          String,
          String
        )
      ]
      .map(row =>
        SearchDocumentSource(
          SearchDocumentKind.Device,
          row._1,
          row._2,
          row._3,
          row._4,
          row._5,
          row._6,
          row._7,
          row._8,
          row._9,
          row._10,
          row._11,
          row._12
        )
      )

  private def behaviourCandidates(limit: Int): Query0[SearchDocumentSource] =
    sql"""SELECT snapshot.snapshot_key, snapshot.source_mac, snapshot.location_id, snapshot.sensor_id,
                  snapshot.window_end, CAST(NULL AS TEXT), CAST(NULL AS TEXT),
                  CAST('behaviour_window' AS TEXT), 0, FALSE,
                  COALESCE(NULLIF(snapshot.embedding_text, ''), snapshot.text_summary),
                  jsonb_build_object(
                    'window_start', snapshot.window_start,
                    'window_end', snapshot.window_end,
                    'event_count', snapshot.event_count,
                    'protocol_mix', snapshot.protocol_mix,
                    'frame_type_distribution', snapshot.frame_type_distribution,
                    'text_summary', snapshot.text_summary
                  )
           FROM atheros_search.behaviour_snapshots snapshot
           LEFT JOIN atheros_search.search_documents document
             ON document.source_table = 'behaviour_snapshots'
            AND document.source_key = snapshot.snapshot_key
            AND document.source_kind = 'behaviour_window'
            AND document.status = 'active'
           WHERE (document.document_id IS NULL OR document.updated_at < snapshot.updated_at)
             AND COALESCE(NULLIF(snapshot.embedding_text, ''), NULLIF(snapshot.text_summary, '')) IS NOT NULL
           ORDER BY snapshot.window_end, snapshot.snapshot_key
           LIMIT ${limit.max(1)}"""
      .query[
        (
          String,
          Option[String],
          Option[String],
          Option[String],
          java.sql.Timestamp,
          Option[String],
          Option[String],
          Option[String],
          Int,
          Boolean,
          String,
          String
        )
      ]
      .map(row =>
        SearchDocumentSource(
          SearchDocumentKind.Behaviour,
          row._1,
          row._2,
          row._3,
          row._4,
          row._5,
          row._6,
          row._7,
          row._8,
          row._9,
          row._10,
          row._11,
          row._12
        )
      )

  private def sequenceCandidates(limit: Int): Query0[SearchDocumentSource] =
    sql"""SELECT sequence_row.session_key, sequence_row.source_mac, sequence_row.location_id,
                  sequence_row.sensor_id, sequence_row.window_end, CAST(NULL AS TEXT), CAST(NULL AS TEXT),
                  CAST('frame_sequence' AS TEXT), 0, FALSE,
                  COALESCE(NULLIF(sequence_row.semantic_tokens, ''), sequence_row.sequence_tokens),
                  jsonb_build_object(
                    'window_start', sequence_row.window_start,
                    'window_end', sequence_row.window_end,
                    'frame_count', sequence_row.frame_count,
                    'sequence_tokens', sequence_row.sequence_tokens,
                    'semantic_tokens', sequence_row.semantic_tokens
                  )
           FROM atheros_search.frame_sequences sequence_row
           LEFT JOIN atheros_search.search_documents document
             ON document.source_table = 'frame_sequences'
            AND document.source_key = sequence_row.session_key
            AND document.source_kind = 'frame_sequence'
            AND document.status = 'active'
           WHERE (document.document_id IS NULL OR document.updated_at < sequence_row.updated_at)
             AND COALESCE(NULLIF(sequence_row.semantic_tokens, ''), NULLIF(sequence_row.sequence_tokens, '')) IS NOT NULL
           ORDER BY sequence_row.window_end, sequence_row.session_key
           LIMIT ${limit.max(1)}"""
      .query[
        (
          String,
          Option[String],
          Option[String],
          Option[String],
          java.sql.Timestamp,
          Option[String],
          Option[String],
          Option[String],
          Int,
          Boolean,
          String,
          String
        )
      ]
      .map(row =>
        SearchDocumentSource(
          SearchDocumentKind.Sequence,
          row._1,
          row._2,
          row._3,
          row._4,
          row._5,
          row._6,
          row._7,
          row._8,
          row._9,
          row._10,
          row._11,
          row._12
        )
      )

  def persist(document: PreparedSearchDocument): ConnectionIO[Unit] =
    val sourceTable = document.kind.sourceTable
    val sourceKind = document.kind.sourceKind
    val tagsJson = document.tags
      .map { case (kind, value) =>
        Json.obj("type" -> kind.asJson, "value" -> value.asJson)
      }
      .asJson
      .noSpaces
    val metadata = Json
      .obj(
        "producer" -> "octopus".asJson,
        "embedding_kind" -> document.kind.embeddingKind.asJson,
        "normalized_sha256" -> document.normalizedSha256.asJson
      )
      .noSpaces

    for
      _ <- sql"""UPDATE atheros_search.search_documents
                   SET status = 'superseded', updated_at = CURRENT_TIMESTAMP
                   WHERE source_table = $sourceTable
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
                   ${document.documentId}, ${document.sourceKey}, $sourceTable, $sourceKind,
                   ${document.sourceVersion}, ${document.sourceMac}, ${document.locationId},
                   ${document.sensorId}, ${document.observedAt}, ${document.bssid}, ${document.ssid},
                   ${document.frameSubtype}, $tagsJson, ${document.detailJson}, ${document.securityFlags},
                   ${document.handshakeCaptured}, ${document.title}, ${document.normalizedText},
                   ${document.normalizedSha256}, 'und', 'active', $metadata,
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                 ) ON CONFLICT (document_id) DO UPDATE SET
                   status = 'active',
                   source_key = EXCLUDED.source_key,
                   source_table = EXCLUDED.source_table,
                   source_kind = EXCLUDED.source_kind,
                   source_mac = EXCLUDED.source_mac,
                   location_id = EXCLUDED.location_id,
                   sensor_id = EXCLUDED.sensor_id,
                   observed_at = EXCLUDED.observed_at,
                   bssid = EXCLUDED.bssid,
                   ssid = EXCLUDED.ssid,
                   frame_subtype = EXCLUDED.frame_subtype,
                   security_flags = EXCLUDED.security_flags,
                   handshake_captured = EXCLUDED.handshake_captured,
                   tags = EXCLUDED.tags,
                   detail_json = EXCLUDED.detail_json,
                   title = EXCLUDED.title,
                   normalized_text = EXCLUDED.normalized_text,
                   normalized_sha256 = EXCLUDED.normalized_sha256,
                   source_version = EXCLUDED.source_version,
                   metadata = EXCLUDED.metadata,
                   updated_at = CURRENT_TIMESTAMP""".update.run
      _ <- sql"DELETE FROM atheros_search.search_document_tokens WHERE document_id = ${document.documentId}".update.run
      _ <- TokenInsert
        .updateMany(document.tokens.map { case (token, frequency, count) =>
          (token, document.documentId, frequency, count)
        })
        .void
      _ <- sql"DELETE FROM atheros_search.search_document_tags WHERE document_id = ${document.documentId}".update.run
      _ <- TagInsert
        .updateMany(document.tags.map { case (kind, value) =>
          (document.documentId, kind, value)
        })
        .void
    yield ()

  def documentsMissingEmbeddingJobs(
    kind: SearchDocumentKind,
    embeddingModel: String,
    limit: Int
  ): Query0[(String, String)] =
    val embeddingKind = kind.embeddingKind
    val sourceKind = kind.sourceKind
    sql"""SELECT document.document_id, document.normalized_sha256
           FROM atheros_search.search_documents document
           LEFT JOIN atheros_search.embedding_jobs job
             ON job.document_id = document.document_id
            AND job.embedding_kind = $embeddingKind
            AND job.embedding_model = $embeddingModel
            AND job.content_sha256 = document.normalized_sha256
           WHERE document.status = 'active'
             AND document.source_kind = $sourceKind
             AND job.job_id IS NULL
           ORDER BY document.observed_at, document.document_id
           LIMIT ${limit.max(1)}""".query[(String, String)]

  def enqueueEmbeddingJob(
    kind: SearchDocumentKind,
    jobId: String,
    documentId: String,
    contentSha256: String,
    embeddingModel: String
  ): ConnectionIO[Unit] =
    val embeddingKind = kind.embeddingKind
    sql"""INSERT INTO atheros_search.embedding_jobs (
             job_id, document_id, embedding_kind, embedding_model, content_sha256,
             status, priority, attempt_count, max_attempts, next_attempt_at,
             created_at, updated_at
           ) VALUES (
             $jobId, $documentId, $embeddingKind, $embeddingModel,
             $contentSha256, 'pending', 100, 0, 5,
             CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
           ) ON CONFLICT (document_id, embedding_kind, embedding_model, content_sha256) DO UPDATE SET job_id = embedding_jobs.job_id""".update.run.void

  val TokenInsert: Update[(String, String, Double, Int)] =
    Update[(String, String, Double, Int)](
      """INSERT INTO atheros_search.search_document_tokens (
           token, document_id, field_name, term_frequency, token_count, updated_at
         ) VALUES (?, ?, 'body', ?, ?, CURRENT_TIMESTAMP)"""
    )

  val TagInsert: Update[(String, String, String)] =
    Update[(String, String, String)](
      """INSERT INTO atheros_search.search_document_tags (
           document_id, tag_type, tag_value, created_at
         ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)"""
    )
