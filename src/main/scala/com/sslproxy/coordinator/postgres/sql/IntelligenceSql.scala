package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.processor.{BaselineProjection, BehaviorSnapshotProjection, FrameToken, ProjectionFrame, SequenceProjection, SimilarityCandidate, SimilarityProjection, TimingProfileProjection}
import doobie.{ConnectionIO, Fragment, Query0}
import doobie.implicits.*

object IntelligenceSql:
  enum VectorKind(val table: String, val index: String, val embeddingKind: String, val pairKind: String):
    case Event extends VectorKind("search_vectors_event",
          "search_vectors_event_embedding_hnsw_idx", "event", "event_event")
    case Behaviour extends VectorKind("search_vectors_behaviour",
          "search_vectors_behaviour_embedding_hnsw_idx", "behaviour", "device_device")
    case Sequence extends VectorKind("search_vectors_sequence",
          "search_vectors_sequence_embedding_hnsw_idx", "sequence", "sequence_sequence")

  private val FrameColumnsFragment: Fragment = Fragment.const(
    "frame.dedupe_key, frame.source_mac, frame.location_id, frame.sensor_id, frame.observed_at, " +
      "frame.frame_type, frame.frame_subtype, radio.signal_dbm, COALESCE(qos.retry, false), " +
      "COALESCE(qos.protected, false), frame.bssid, " +
      "network_row.app_protocol, security_row.adjacent_mac_hint, radio.tsft_delta_us, " +
      "radio.wall_clock_delta_ms, identity_row.session_key"
  )

  def behaviorCandidates(limit: Int): Query0[ProjectionFrame] =
    val batchLimit = limit.max(1)
    (fr"""WITH source_windows AS (
             SELECT frame.source_mac,
                    FLOOR(EXTRACT(EPOCH FROM frame.observed_at) / 3600) AS window_bucket,
                    COUNT(*) AS source_event_count
             FROM wireless_frames frame
             WHERE frame.source_mac IS NOT NULL
             GROUP BY frame.source_mac, window_bucket
           ), candidate_windows AS (
             SELECT source.source_mac, source.window_bucket
             FROM source_windows source
             LEFT JOIN atheros_search.behaviour_snapshots snapshot
               ON snapshot.snapshot_key = CONCAT(
                 source.source_mac, ':', CAST(source.window_bucket * 3600 AS TEXT)
               )
             WHERE snapshot.snapshot_key IS NULL
                OR snapshot.event_count <> source.source_event_count
             ORDER BY source.window_bucket, source.source_mac
             LIMIT $batchLimit
           )
           SELECT""" ++ FrameColumnsFragment ++ fr"""
           FROM wireless_frames frame
           JOIN candidate_windows candidate
             ON candidate.source_mac = frame.source_mac
            AND candidate.window_bucket = FLOOR(EXTRACT(EPOCH FROM frame.observed_at) / 3600)
           LEFT JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_qos qos ON qos.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_network network_row ON network_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_security security_row ON security_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_identity identity_row ON identity_row.dedupe_key = frame.dedupe_key
           ORDER BY frame.observed_at, frame.dedupe_key""").query[ProjectionFrame]

  def timingCandidates(limit: Int): Query0[ProjectionFrame] =
    val batchLimit = limit.max(1)
    (fr"""WITH source_windows AS (
             SELECT frame.source_mac,
                    FLOOR(EXTRACT(EPOCH FROM frame.observed_at) / 3600) AS window_bucket,
                    COUNT(*) AS source_event_count
             FROM wireless_frames frame
             WHERE frame.source_mac IS NOT NULL
             GROUP BY frame.source_mac, window_bucket
           ), candidate_windows AS (
             SELECT source.source_mac, source.window_bucket
             FROM source_windows source
             LEFT JOIN atheros_search.timing_profiles profile
               ON profile.profile_key = CONCAT(
                 source.source_mac, ':', CAST(source.window_bucket * 3600 AS TEXT)
               )
             WHERE profile.profile_key IS NULL
                OR profile.source_event_count <> source.source_event_count
             ORDER BY source.window_bucket, source.source_mac
             LIMIT $batchLimit
           )
           SELECT""" ++ FrameColumnsFragment ++ fr"""
           FROM wireless_frames frame
           JOIN candidate_windows candidate
             ON candidate.source_mac = frame.source_mac
            AND candidate.window_bucket = FLOOR(EXTRACT(EPOCH FROM frame.observed_at) / 3600)
           LEFT JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_qos qos ON qos.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_network network_row ON network_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_security security_row ON security_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_identity identity_row ON identity_row.dedupe_key = frame.dedupe_key
           ORDER BY frame.observed_at, frame.dedupe_key""").query[ProjectionFrame]

  def sequenceCandidates(limit: Int): Query0[ProjectionFrame] =
    val batchLimit = limit.max(1)
    (fr"""WITH source_sessions AS (
             SELECT identity_row.session_key, COUNT(*) AS source_event_count
             FROM wireless_frame_identity identity_row
             WHERE identity_row.session_key IS NOT NULL
             GROUP BY identity_row.session_key
           ), candidate_sessions AS (
             SELECT source.session_key
             FROM source_sessions source
             LEFT JOIN atheros_search.frame_sequences sequence_row
               ON sequence_row.session_key = source.session_key
             WHERE sequence_row.session_key IS NULL
                OR sequence_row.frame_count <> source.source_event_count
             ORDER BY source.session_key
             LIMIT $batchLimit
           )
           SELECT""" ++ FrameColumnsFragment ++ fr"""
           FROM wireless_frames frame
           JOIN wireless_frame_identity identity_row ON identity_row.dedupe_key = frame.dedupe_key
           JOIN candidate_sessions candidate ON candidate.session_key = identity_row.session_key
           LEFT JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_qos qos ON qos.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_network network_row ON network_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_security security_row ON security_row.dedupe_key = frame.dedupe_key
           ORDER BY identity_row.session_key, frame.observed_at, frame.dedupe_key""").query[ProjectionFrame]

  def baselineCandidates(limit: Int): Query0[(String, Double)] =
    val batchLimit = limit.max(1)
    sql"""WITH source_bssids AS (
             SELECT frame.bssid, COUNT(*) AS source_event_count
             FROM wireless_frames frame
             JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
             WHERE frame.bssid IS NOT NULL AND radio.signal_dbm IS NOT NULL
             GROUP BY frame.bssid
           ), candidate_bssids AS (
             SELECT source.bssid
             FROM source_bssids source
             LEFT JOIN atheros_search.baseline_profiles baseline
               ON baseline.bssid = source.bssid AND baseline.metric = 'signal_dbm'
             WHERE baseline.baseline_id IS NULL
                OR baseline.sample_count <> source.source_event_count
             ORDER BY source.bssid
             LIMIT $batchLimit
           )
           SELECT frame.bssid, CAST(radio.signal_dbm AS DOUBLE PRECISION)
           FROM wireless_frames frame
           JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
           JOIN candidate_bssids candidate ON candidate.bssid = frame.bssid
           ORDER BY frame.bssid, frame.observed_at, frame.dedupe_key""".query[(String, Double)]

  def annReady(
      kind: VectorKind): Query0[Boolean] =
    sql"""SELECT
             EXISTS (
               SELECT 1
               FROM pg_catalog.pg_indexes index_state
               WHERE index_state.schemaname = 'atheros_search'
                 AND index_state.tablename = ${kind.table}
                 AND index_state.indexname = ${kind.index}
                 AND index_state.indexdef ILIKE '%USING hnsw%'
             )""".query[Boolean]

  def similarityAnchors(
      kind: VectorKind,
    limit: Int
  ): Query0[(Long, String, String, String)] =
    val vectorTable = Fragment.const(s" atheros_search.${kind.table} ")
    val batchLimit = limit.max(1)
    (fr"""SELECT vector_id, document_id, embedding_model, embedding::text
           FROM""" ++ vectorTable ++ fr"""
           ORDER BY vector_id DESC
           LIMIT $batchLimit""").query[(Long, String, String, String)]

  def similarityCandidatesForAnchor(
    kind: VectorKind,
    anchorDocumentId: String,
    anchorEmbeddingModel: String,
    anchorEmbedding: String,
      maximumDistance: Double,
      limit: Int
  ): Query0[SimilarityCandidate] =
    val vectorTable = Fragment.const(s" atheros_search.${kind.table} ")
    val pairKind = kind.pairKind
    val embeddingKind = kind.embeddingKind
    val distance = maximumDistance.max(0.0d).min(2.0d)
    val batchLimit = limit.max(1)
    val topK = (batchLimit * 2).min(64)
    (fr"""SELECT $pairKind, $embeddingKind, $anchorEmbeddingModel,
                  $anchorDocumentId, right_vector.document_id,
                  left_document.source_table, left_document.source_key, left_document.source_mac,
                  left_document.sensor_id, left_document.location_id, left_document.observed_at,
                  right_document.source_table, right_document.source_key, right_document.source_mac,
                  right_document.sensor_id, right_document.location_id, right_document.observed_at,
                  right_vector.cosine_distance
           FROM (
             SELECT candidate.vector_id, candidate.document_id,
                    candidate.embedding_model,
                    candidate.embedding <=> CAST($anchorEmbedding AS vector) AS cosine_distance
           FROM""" ++ vectorTable ++ fr"""candidate
             WHERE candidate.document_id <> $anchorDocumentId
               AND candidate.embedding_model = $anchorEmbeddingModel
             ORDER BY candidate.embedding <=> CAST($anchorEmbedding AS vector) ASC
             LIMIT $topK
           ) right_vector
           JOIN atheros_search.search_documents left_document
             ON left_document.document_id = $anchorDocumentId
           JOIN atheros_search.search_documents right_document
             ON right_document.document_id = right_vector.document_id
           WHERE right_vector.cosine_distance <= $distance
             AND NOT EXISTS (
               SELECT 1 FROM atheros_search.similarity_pairs pair
               WHERE pair.pair_kind = $pairKind
                 AND pair.embedding_model = $anchorEmbeddingModel
                 AND (
                   (pair.left_document_id = $anchorDocumentId
                 AND pair.right_document_id = right_vector.document_id)
                   OR
                   (pair.left_document_id = right_vector.document_id
                     AND pair.right_document_id = $anchorDocumentId)
                 )
             )
           ORDER BY right_vector.cosine_distance, right_vector.vector_id
           LIMIT $batchLimit""").query[SimilarityCandidate]

  def persistBehavior(value: BehaviorSnapshotProjection): ConnectionIO[Int] =
    sql"""INSERT INTO atheros_search.behaviour_snapshots (
             snapshot_id, snapshot_key, source_mac, location_id, sensor_id,
             window_start, window_end, event_count, text_summary, embedding_text,
             protocol_mix, frame_type_distribution, signal_min_dbm, signal_max_dbm,
             signal_avg_dbm, retry_count, protected_count, unprotected_count,
             unique_bssid_count, mac_rotation_indicators, projection_run_id
           ) VALUES (
             ${value.snapshotId}, ${value.snapshotKey}, ${value.sourceMac},
             ${value.locationId}, ${value.sensorId}, ${value.windowStart}, ${value.windowEnd},
             ${value.eventCount}, ${value.textSummary}, ${Some(value.textSummary)},
             CAST(${value.protocolMixJson} AS JSON), CAST(${value.frameDistributionJson} AS JSON),
             ${value.signalMin}, ${value.signalMax}, ${value.signalAverage}, ${value.retryCount},
             ${value.protectedCount}, ${value.unprotectedCount}, ${value.uniqueBssidCount},
             CAST(${value.rotationIndicatorsJson} AS JSON), ${value.projectionRunId}
           ) ON CONFLICT (snapshot_key) DO UPDATE SET
             location_id = EXCLUDED.location_id,
             sensor_id = EXCLUDED.sensor_id,
             window_start = EXCLUDED.window_start,
             window_end = EXCLUDED.window_end,
             event_count = EXCLUDED.event_count,
             text_summary = EXCLUDED.text_summary,
             embedding_text = EXCLUDED.embedding_text,
             protocol_mix = EXCLUDED.protocol_mix,
             frame_type_distribution = EXCLUDED.frame_type_distribution,
             signal_min_dbm = EXCLUDED.signal_min_dbm,
             signal_max_dbm = EXCLUDED.signal_max_dbm,
             signal_avg_dbm = EXCLUDED.signal_avg_dbm,
             retry_count = EXCLUDED.retry_count,
             protected_count = EXCLUDED.protected_count,
             unprotected_count = EXCLUDED.unprotected_count,
             unique_bssid_count = EXCLUDED.unique_bssid_count,
             mac_rotation_indicators = EXCLUDED.mac_rotation_indicators,
             projection_run_id = EXCLUDED.projection_run_id,
             updated_at = CURRENT_TIMESTAMP""".update.run

  def persistTiming(value: TimingProfileProjection): ConnectionIO[Int] =
    sql"""INSERT INTO atheros_search.timing_profiles (
             profile_id, profile_key, source_mac, sensor_id, location_id,
             window_start, window_end, embedding_text, tsft_p50_us, tsft_p95_us,
             tsft_jitter, wall_p50_ms, wall_jitter_ms, source_event_count, projection_run_id
           ) VALUES (
             ${value.profileId}, ${value.profileKey}, ${value.sourceMac}, ${value.sensorId},
             ${value.locationId}, ${value.windowStart}, ${value.windowEnd}, NULL,
             ${value.tsftP50}, ${value.tsftP95}, ${value.tsftJitter},
             ${value.wallP50}, ${value.wallJitter}, ${value.sourceEventCount}, ${value.projectionRunId}
           ) ON CONFLICT (profile_key) DO UPDATE SET
             sensor_id = EXCLUDED.sensor_id,
             location_id = EXCLUDED.location_id,
             window_start = EXCLUDED.window_start,
             window_end = EXCLUDED.window_end,
             tsft_p50_us = EXCLUDED.tsft_p50_us,
             tsft_p95_us = EXCLUDED.tsft_p95_us,
             tsft_jitter = EXCLUDED.tsft_jitter,
             wall_p50_ms = EXCLUDED.wall_p50_ms,
             wall_jitter_ms = EXCLUDED.wall_jitter_ms,
             source_event_count = EXCLUDED.source_event_count,
             projection_run_id = EXCLUDED.projection_run_id,
             updated_at = CURRENT_TIMESTAMP""".update.run

  def persistSequence(value: SequenceProjection): ConnectionIO[Int] =
    val tokenText = value.tokens.map(_.value).mkString(" ")
    val counts = value.tokens.zip(value.tokens.drop(1)).groupMapReduce(identity)(_ => 1L)(_ + _)
    val totals = counts.toList.groupMapReduce(_._1._1)(_._2)(_ + _)
    for
      oldPrevious <- sql"""SELECT previous_token
                            FROM atheros_search.sequence_previous_totals
                            WHERE session_key = ${value.sessionKey}
                              AND sequence_kind = 'frame_sequence'""".query[String].to[List]
      sequence <- sql"""INSERT INTO atheros_search.frame_sequences (
                          session_key, source_mac, location_id, sensor_id, window_start, window_end,
                          sequence_tokens, semantic_tokens, frame_count, projection_run_id
                        ) VALUES (
                          ${value.sessionKey}, ${value.sourceMac}, ${value.locationId}, ${value.sensorId},
                          ${value.windowStart}, ${value.windowEnd}, $tokenText, $tokenText,
                          ${value.tokens.size.toLong}, ${value.projectionRunId}
                        ) ON CONFLICT (session_key) DO UPDATE SET
                          source_mac = EXCLUDED.source_mac,
                          location_id = EXCLUDED.location_id,
                          sensor_id = EXCLUDED.sensor_id,
                          window_start = EXCLUDED.window_start,
                          window_end = EXCLUDED.window_end,
                          sequence_tokens = EXCLUDED.sequence_tokens,
                          semantic_tokens = EXCLUDED.semantic_tokens,
                          frame_count = EXCLUDED.frame_count,
                          projection_run_id = EXCLUDED.projection_run_id,
                          updated_at = CURRENT_TIMESTAMP""".update.run
      _ <- sql"""DELETE FROM atheros_search.sequence_transition_contributions
                  WHERE session_key = ${value.sessionKey}
                    AND sequence_kind = 'frame_sequence'""".update.run
      _ <- sql"""DELETE FROM atheros_search.sequence_previous_totals
                  WHERE session_key = ${value.sessionKey}
                    AND sequence_kind = 'frame_sequence'""".update.run
      contributions <- counts.toList.traverse { case ((previous, next), count) =>
        sql"""INSERT INTO atheros_search.sequence_transition_contributions (
               session_key, previous_token, next_token, sequence_kind,
               transition_count, previous_total, projection_run_id
             ) VALUES (
               ${value.sessionKey}, ${previous.value}, ${next.value}, 'frame_sequence',
               $count, ${totals(previous)}, ${value.projectionRunId}
             )""".update.run
      }
      previousTotals <- totals.toList.traverse { case (previous, total) =>
        sql"""INSERT INTO atheros_search.sequence_previous_totals (
               session_key, previous_token, sequence_kind, previous_total, projection_run_id
             ) VALUES (
               ${value.sessionKey}, ${previous.value}, 'frame_sequence', $total,
               ${value.projectionRunId}
             )""".update.run
      }
      affectedPrevious = (oldPrevious ++ totals.keysIterator.map(_.value)).distinct
      transitions <- affectedPrevious.traverse { previous =>
        for
          deleted <- sql"""DELETE FROM atheros_search.sequence_transitions
                            WHERE previous_token = $previous
                              AND sequence_kind = 'frame_sequence'""".update.run
          inserted <- sql"""INSERT INTO atheros_search.sequence_transitions (
                              previous_token, next_token, sequence_kind, transition_count,
                              previous_total, vocabulary_size, probability, projection_run_id
                            )
                            SELECT contribution.previous_token, contribution.next_token,
                                   contribution.sequence_kind,
                                   SUM(contribution.transition_count), totals.previous_total,
                                   ${FrameToken.values.length.toLong},
                                   SUM(contribution.transition_count) / NULLIF(totals.previous_total, 0),
                                   ${value.projectionRunId}
                            FROM atheros_search.sequence_transition_contributions contribution
                            JOIN (
                              SELECT previous_token, sequence_kind, SUM(previous_total) AS previous_total
                              FROM atheros_search.sequence_previous_totals
                              WHERE previous_token = $previous
                                AND sequence_kind = 'frame_sequence'
                              GROUP BY previous_token, sequence_kind
                            ) totals
                              ON totals.previous_token = contribution.previous_token
                             AND totals.sequence_kind = contribution.sequence_kind
                            WHERE contribution.previous_token = $previous
                              AND contribution.sequence_kind = 'frame_sequence'
                            GROUP BY contribution.previous_token, contribution.next_token,
                                     contribution.sequence_kind, totals.previous_total""".update.run
        yield deleted + inserted
      }
    yield sequence + contributions.sum + previousTotals.sum + transitions.sum

  def persistBaseline(value: BaselineProjection): ConnectionIO[Int] =
    sql"""INSERT INTO atheros_search.baseline_profiles (
             baseline_id, bssid, metric, p5, p50, p95, sample_count, projection_run_id
           ) VALUES (
             ${value.baselineId}, ${value.bssid}, ${value.metric}, ${value.p5},
             ${value.p50}, ${value.p95}, ${value.sampleCount}, ${value.projectionRunId}
           ) ON CONFLICT (bssid, metric) DO UPDATE SET
             p5 = EXCLUDED.p5,
             p50 = EXCLUDED.p50,
             p95 = EXCLUDED.p95,
             sample_count = EXCLUDED.sample_count,
             projection_run_id = EXCLUDED.projection_run_id,
             updated_at = CURRENT_TIMESTAMP""".update.run

  def persistSimilarity(value: SimilarityProjection): ConnectionIO[Int] =
    val candidate = value.candidate
    sql"""INSERT INTO atheros_search.similarity_pairs (
             pair_id, pair_kind, embedding_model, embedding_kind,
             left_document_id, right_document_id,
             left_source_table, left_source_key, left_source_mac, left_sensor_id,
             left_location_id, left_observed_at,
             right_source_table, right_source_key, right_source_mac, right_sensor_id,
             right_location_id, right_observed_at,
             cosine_distance, cosine_similarity, pair_rank, evidence,
             computed_at, projection_run_id
           ) VALUES (
             ${value.pairId}, ${candidate.pairKind}, ${candidate.embeddingModel},
             ${candidate.embeddingKind}, ${candidate.leftDocumentId}, ${candidate.rightDocumentId},
             ${candidate.leftSourceTable}, ${candidate.leftSourceKey}, ${candidate.leftSourceMac},
             ${candidate.leftSensorId}, ${candidate.leftLocationId}, ${candidate.leftObservedAt},
             ${candidate.rightSourceTable}, ${candidate.rightSourceKey}, ${candidate.rightSourceMac},
             ${candidate.rightSensorId}, ${candidate.rightLocationId}, ${candidate.rightObservedAt},
             ${candidate.cosineDistance}, ${value.cosineSimilarity}, 1,
             CAST(${value.evidenceJson} AS JSON), CURRENT_TIMESTAMP, ${value.projectionRunId}
           ) ON CONFLICT (pair_kind, embedding_model, left_document_id, right_document_id) DO UPDATE SET
             cosine_distance = EXCLUDED.cosine_distance,
             cosine_similarity = EXCLUDED.cosine_similarity,
             evidence = EXCLUDED.evidence,
             computed_at = EXCLUDED.computed_at,
             projection_run_id = EXCLUDED.projection_run_id,
             updated_at = CURRENT_TIMESTAMP""".update.run
