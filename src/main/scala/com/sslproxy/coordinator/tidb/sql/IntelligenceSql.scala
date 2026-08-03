package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.processor.{BaselineProjection, BehaviorSnapshotProjection, FrameToken, ProjectionFrame, SequenceProjection, SimilarityCandidate, SimilarityProjection, TimingProfileProjection}
import doobie.{ConnectionIO, Fragment, Query0}
import doobie.implicits.*

object IntelligenceSql:
  enum VectorKind(val table: String, val embeddingKind: String, val pairKind: String):
    case Event extends VectorKind("search_vectors_event", "event", "event_event")
    case Behaviour extends VectorKind("search_vectors_behaviour", "behaviour", "device_device")
    case Sequence extends VectorKind("search_vectors_sequence", "sequence", "sequence_sequence")

  private val FrameColumnsFragment: Fragment = Fragment.const(
    "frame.dedupe_key, frame.source_mac, frame.location_id, frame.sensor_id, frame.observed_at, " +
      "frame.frame_type, frame.frame_subtype, radio.signal_dbm, COALESCE(qos.retry, 0), " +
      "COALESCE(qos.protected, 0), frame.bssid, " +
      "network_row.app_protocol, security_row.adjacent_mac_hint, radio.tsft_delta_us, " +
      "radio.wall_clock_delta_ms, identity_row.session_key"
  )

  def behaviorCandidates(limit: Int): Query0[ProjectionFrame] =
    val batchLimit = limit.max(1)
    (fr"""WITH candidate_windows AS (
             SELECT frame.source_mac,
                    FLOOR(UNIX_TIMESTAMP(frame.observed_at) / 3600) AS window_bucket
             FROM wireless_frames frame
             WHERE frame.source_mac IS NOT NULL
               AND NOT EXISTS (
                 SELECT 1 FROM atheros_search.behaviour_snapshots snapshot
                 WHERE snapshot.snapshot_key = CONCAT(
                   frame.source_mac, ':',
                   CAST(FLOOR(UNIX_TIMESTAMP(frame.observed_at) / 3600) * 3600 AS CHAR)
                 )
               )
             GROUP BY frame.source_mac, window_bucket
             ORDER BY window_bucket, frame.source_mac
             LIMIT $batchLimit
           )
           SELECT""" ++ FrameColumnsFragment ++ fr"""
           FROM wireless_frames frame
           JOIN candidate_windows candidate
             ON candidate.source_mac = frame.source_mac
            AND candidate.window_bucket = FLOOR(UNIX_TIMESTAMP(frame.observed_at) / 3600)
           LEFT JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_qos qos ON qos.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_network network_row ON network_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_security security_row ON security_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_identity identity_row ON identity_row.dedupe_key = frame.dedupe_key
           ORDER BY frame.observed_at, frame.dedupe_key""").query[ProjectionFrame]

  def timingCandidates(limit: Int): Query0[ProjectionFrame] =
    val batchLimit = limit.max(1)
    (fr"""WITH candidate_windows AS (
             SELECT frame.source_mac,
                    FLOOR(UNIX_TIMESTAMP(frame.observed_at) / 3600) AS window_bucket
             FROM wireless_frames frame
             WHERE frame.source_mac IS NOT NULL
               AND NOT EXISTS (
                 SELECT 1 FROM atheros_search.timing_profiles profile
                 WHERE profile.profile_key = CONCAT(
                   frame.source_mac, ':',
                   CAST(FLOOR(UNIX_TIMESTAMP(frame.observed_at) / 3600) * 3600 AS CHAR)
                 )
               )
             GROUP BY frame.source_mac, window_bucket
             ORDER BY window_bucket, frame.source_mac
             LIMIT $batchLimit
           )
           SELECT""" ++ FrameColumnsFragment ++ fr"""
           FROM wireless_frames frame
           JOIN candidate_windows candidate
             ON candidate.source_mac = frame.source_mac
            AND candidate.window_bucket = FLOOR(UNIX_TIMESTAMP(frame.observed_at) / 3600)
           LEFT JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_qos qos ON qos.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_network network_row ON network_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_security security_row ON security_row.dedupe_key = frame.dedupe_key
           LEFT JOIN wireless_frame_identity identity_row ON identity_row.dedupe_key = frame.dedupe_key
           ORDER BY frame.observed_at, frame.dedupe_key""").query[ProjectionFrame]

  def sequenceCandidates(limit: Int): Query0[ProjectionFrame] =
    val batchLimit = limit.max(1)
    (fr"""WITH candidate_sessions AS (
             SELECT identity_row.session_key
             FROM wireless_frame_identity identity_row
             WHERE identity_row.session_key IS NOT NULL
               AND NOT EXISTS (
                 SELECT 1 FROM atheros_search.frame_sequences sequence_row
                 WHERE sequence_row.session_key = identity_row.session_key
               )
             GROUP BY identity_row.session_key
             ORDER BY identity_row.session_key
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
    sql"""WITH candidate_bssids AS (
             SELECT frame.bssid
             FROM wireless_frames frame
             JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
             WHERE frame.bssid IS NOT NULL AND radio.signal_dbm IS NOT NULL
               AND NOT EXISTS (
                 SELECT 1 FROM atheros_search.baseline_profiles baseline
                 WHERE baseline.bssid = frame.bssid AND baseline.metric = 'signal_dbm'
               )
             GROUP BY frame.bssid
             ORDER BY frame.bssid
             LIMIT $batchLimit
           )
           SELECT frame.bssid, CAST(radio.signal_dbm AS DOUBLE)
           FROM wireless_frames frame
           JOIN wireless_frame_radio radio ON radio.dedupe_key = frame.dedupe_key
           JOIN candidate_bssids candidate ON candidate.bssid = frame.bssid
           ORDER BY frame.bssid, frame.observed_at, frame.dedupe_key""".query[(String, Double)]

  def similarityCandidates(
      kind: VectorKind,
      maximumDistance: Double,
      limit: Int
  ): Query0[SimilarityCandidate] =
    val vectorTable = Fragment.const(s" atheros_search.${kind.table} ")
    val pairKind = kind.pairKind
    val embeddingKind = kind.embeddingKind
    val distance = maximumDistance.max(0.0d).min(2.0d)
    val batchLimit = limit.max(1)
    (fr"""SELECT $pairKind, $embeddingKind, left_vector.embedding_model,
                  left_vector.document_id, right_vector.document_id,
                  left_document.source_table, left_document.source_key, left_document.source_mac,
                  left_document.sensor_id, left_document.location_id, left_document.observed_at,
                  right_document.source_table, right_document.source_key, right_document.source_mac,
                  right_document.sensor_id, right_document.location_id, right_document.observed_at,
                  VEC_COSINE_DISTANCE(left_vector.embedding, right_vector.embedding)
           FROM""" ++ vectorTable ++ fr"""left_vector
           JOIN""" ++ vectorTable ++ fr"""right_vector
             ON right_vector.vector_id > left_vector.vector_id
            AND right_vector.embedding_model = left_vector.embedding_model
           JOIN atheros_search.search_documents left_document
             ON left_document.document_id = left_vector.document_id
           JOIN atheros_search.search_documents right_document
             ON right_document.document_id = right_vector.document_id
           WHERE VEC_COSINE_DISTANCE(left_vector.embedding, right_vector.embedding) <= $distance
             AND NOT EXISTS (
               SELECT 1 FROM atheros_search.similarity_pairs pair
               WHERE pair.pair_kind = $pairKind
                 AND pair.embedding_model = left_vector.embedding_model
                 AND pair.left_document_id = left_vector.document_id
                 AND pair.right_document_id = right_vector.document_id
             )
           ORDER BY left_vector.vector_id, right_vector.vector_id
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
           ) ON DUPLICATE KEY UPDATE snapshot_id = snapshot_id""".update.run

  def persistTiming(value: TimingProfileProjection): ConnectionIO[Int] =
    sql"""INSERT INTO atheros_search.timing_profiles (
             profile_id, profile_key, source_mac, sensor_id, location_id,
             window_start, window_end, embedding_text, tsft_p50_us, tsft_p95_us,
             tsft_jitter, wall_p50_ms, wall_jitter_ms, projection_run_id
           ) VALUES (
             ${value.profileId}, ${value.profileKey}, ${value.sourceMac}, ${value.sensorId},
             ${value.locationId}, ${value.windowStart}, ${value.windowEnd}, NULL,
             ${value.tsftP50}, ${value.tsftP95}, ${value.tsftJitter},
             ${value.wallP50}, ${value.wallJitter}, ${value.projectionRunId}
           ) ON DUPLICATE KEY UPDATE profile_id = profile_id""".update.run

  def persistSequence(value: SequenceProjection): ConnectionIO[Int] =
    val tokenText = value.tokens.map(_.value).mkString(" ")
    sql"""INSERT INTO atheros_search.frame_sequences (
             session_key, source_mac, location_id, sensor_id, window_start, window_end,
             sequence_tokens, semantic_tokens, frame_count, projection_run_id
           ) VALUES (
             ${value.sessionKey}, ${value.sourceMac}, ${value.locationId}, ${value.sensorId},
             ${value.windowStart}, ${value.windowEnd}, $tokenText, $tokenText,
             ${value.tokens.size.toLong}, ${value.projectionRunId}
           ) ON DUPLICATE KEY UPDATE session_key = session_key""".update.run.flatMap { inserted =>
      if inserted != 1 then 0.pure[ConnectionIO]
      else
        val counts = value.tokens.zip(value.tokens.drop(1)).groupMapReduce(identity)(_ => 1L)(_ + _)
        val totals = counts.toList.groupMapReduce(_._1._1)(_._2)(_ + _)
        counts.toList.traverse { case ((previous, next), count) =>
          val probability = count.toDouble / totals(previous).toDouble
          sql"""INSERT INTO atheros_search.sequence_transitions (
                 previous_token, next_token, sequence_kind, transition_count,
                 previous_total, vocabulary_size, probability, projection_run_id
               ) VALUES (
                 ${previous.value}, ${next.value}, 'frame_sequence', $count,
                 ${totals(previous)}, ${FrameToken.values.length.toLong}, $probability,
                 ${value.projectionRunId}
               ) ON DUPLICATE KEY UPDATE
                 transition_count = transition_count + VALUES(transition_count),
                 previous_total = previous_total + VALUES(previous_total),
                 vocabulary_size = VALUES(vocabulary_size),
                 probability = transition_count / NULLIF(previous_total, 0),
                 last_updated = CURRENT_TIMESTAMP(6),
                 projection_run_id = VALUES(projection_run_id)""".update.run
        }.map(_.sum)
    }

  def persistBaseline(value: BaselineProjection): ConnectionIO[Int] =
    sql"""INSERT INTO atheros_search.baseline_profiles (
             baseline_id, bssid, metric, p5, p50, p95, sample_count, projection_run_id
           ) VALUES (
             ${value.baselineId}, ${value.bssid}, ${value.metric}, ${value.p5},
             ${value.p50}, ${value.p95}, ${value.sampleCount}, ${value.projectionRunId}
           ) ON DUPLICATE KEY UPDATE baseline_id = baseline_id""".update.run

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
             CAST(${value.evidenceJson} AS JSON), CURRENT_TIMESTAMP(6), ${value.projectionRunId}
           ) ON DUPLICATE KEY UPDATE
             cosine_distance = VALUES(cosine_distance),
             cosine_similarity = VALUES(cosine_similarity),
             evidence = VALUES(evidence),
             computed_at = VALUES(computed_at),
             projection_run_id = VALUES(projection_run_id),
             updated_at = CURRENT_TIMESTAMP(6)""".update.run
