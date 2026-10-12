package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import doobie.{ConnectionIO, Query0}
import doobie.implicits.*

/** All arrays come from one statement and one MVCC snapshot. Revisions hash
  * complete rows, so a writer changing content at the same timestamp is seen.
  */
object DqSnapshotSql:
  def source(
      scope: String,
      operation: String,
      limit: Int,
      candidateId: Option[String],
      minimumSimilarity: Double
  ): Query0[String] =
    sql"""WITH selected_pairs AS MATERIALIZED (
             SELECT pair.*,
                    (('x' || substr(md5(to_jsonb(left_document)::text), 1, 15))::bit(60)::bigint) AS left_revision,
                    (('x' || substr(md5(to_jsonb(right_document)::text), 1, 15))::bit(60)::bigint) AS right_revision
             FROM atheros_search.similarity_pairs pair
             JOIN atheros_search.search_documents left_document ON left_document.document_id = pair.left_document_id
               AND left_document.status = 'active'
             JOIN atheros_search.search_documents right_document ON right_document.document_id = pair.right_document_id
               AND right_document.status = 'active'
             JOIN atheros_search.devices left_device ON left_device.mac = pair.left_source_mac
             JOIN atheros_search.devices right_device ON right_device.mac = pair.right_source_mac
             WHERE $operation = 'prepare_candidates' AND pair.pair_kind = 'device_device'
               AND pair.left_source_mac <> pair.right_source_mac
               AND pair.cosine_similarity >= $minimumSimilarity
               AND ($scope = '*' OR (left_device.location_id = $scope AND right_device.location_id = $scope))
               AND NOT EXISTS (
                 SELECT 1 FROM atheros_search.merge_candidates candidate
                 WHERE candidate.mac_a = LEAST(pair.left_source_mac, pair.right_source_mac)
                   AND candidate.mac_b = GREATEST(pair.left_source_mac, pair.right_source_mac)
                   AND candidate.confidence >= pair.cosine_similarity
               )
             ORDER BY pair.computed_at, pair.pair_id LIMIT $limit
           ), selected_candidates AS MATERIALIZED (
             SELECT candidate.* FROM atheros_search.merge_candidates candidate
             JOIN atheros_search.devices left_device ON left_device.mac = candidate.mac_a
             JOIN atheros_search.devices right_device ON right_device.mac = candidate.mac_b
             WHERE ($scope = '*' OR (left_device.location_id = $scope AND right_device.location_id = $scope))
               AND (($operation = 'reconcile_decision' AND candidate.candidate_id = $candidateId)
                 OR EXISTS (SELECT 1 FROM selected_pairs pair
                   WHERE candidate.mac_a = LEAST(pair.left_source_mac, pair.right_source_mac)
                     AND candidate.mac_b = GREATEST(pair.left_source_mac, pair.right_source_mac)))
           ), selected_macs AS MATERIALIZED (
             SELECT left_source_mac AS mac FROM selected_pairs UNION SELECT right_source_mac FROM selected_pairs
             UNION SELECT mac_a FROM selected_candidates UNION SELECT mac_b FROM selected_candidates
           )
           SELECT jsonb_build_object(
             'pairs', COALESCE((SELECT jsonb_agg(jsonb_build_object(
               'pair_id', pair.pair_id, 'mac_a', LEAST(pair.left_source_mac, pair.right_source_mac),
               'mac_b', GREATEST(pair.left_source_mac, pair.right_source_mac), 'confidence', pair.cosine_similarity,
               'left_revision', pair.left_revision, 'right_revision', pair.right_revision,
               'pair_revision', (('x' || substr(md5((to_jsonb(pair) - 'left_revision' - 'right_revision')::text), 1, 15))::bit(60)::bigint)
             ) ORDER BY pair.computed_at, pair.pair_id) FROM selected_pairs pair), '[]'::jsonb),
             'devices', COALESCE((SELECT jsonb_agg(jsonb_build_object(
               'mac', device.mac, 'registered', device.registered,
               'registered_device_id', COALESCE(device.registered_device_id::text, ''),
               'revision', (('x' || substr(md5(to_jsonb(device)::text), 1, 15))::bit(60)::bigint)
             ) ORDER BY device.mac) FROM atheros_search.devices device JOIN selected_macs selected ON selected.mac = device.mac), '[]'::jsonb),
             'candidates', COALESCE((SELECT jsonb_agg(jsonb_build_object(
               'candidate_id', candidate.candidate_id, 'mac_a', candidate.mac_a, 'mac_b', candidate.mac_b,
               'confidence', candidate.confidence, 'status', candidate.status,
               'revision', (('x' || substr(md5(to_jsonb(candidate)::text), 1, 15))::bit(60)::bigint)
             ) ORDER BY candidate.candidate_id) FROM selected_candidates candidate), '[]'::jsonb),
             'decisions', COALESCE((SELECT jsonb_agg(jsonb_build_object(
               'candidate_id', decision.candidate_id, 'decision', decision.decision, 'decided_at', decision.decided_at
             ) ORDER BY decision.candidate_id) FROM atheros_search.merge_decisions decision
               JOIN selected_candidates candidate ON candidate.candidate_id = decision.candidate_id), '[]'::jsonb)
           )::text""".query[String]

  def serializable: ConnectionIO[Unit] =
    sql"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE".update.run.void

  def lockSources(snapshot: String): ConnectionIO[Unit] =
    for
      _ <-
        sql"""SELECT candidate.candidate_id FROM atheros_search.merge_candidates candidate
                   JOIN jsonb_array_elements(($snapshot::jsonb)->'candidates') item
                     ON item->>'candidate_id' = candidate.candidate_id
                   ORDER BY candidate.candidate_id FOR UPDATE OF candidate"""
          .query[String]
          .to[List]
      _ <- sql"""SELECT device.mac FROM atheros_search.devices device
                   JOIN jsonb_array_elements(($snapshot::jsonb)->'devices') item ON item->>'mac' = device.mac
                   ORDER BY device.mac FOR SHARE OF device"""
        .query[String]
        .to[List]
      _ <-
        sql"""SELECT pair.pair_id::text FROM atheros_search.similarity_pairs pair
                   JOIN jsonb_array_elements(($snapshot::jsonb)->'pairs') item ON (item->>'pair_id')::uuid = pair.pair_id
                   ORDER BY pair.pair_id FOR SHARE OF pair"""
          .query[String]
          .to[List]
      _ <-
        sql"""SELECT document.document_id::text FROM atheros_search.search_documents document
                   WHERE document.document_id IN (
                     SELECT pair.left_document_id FROM atheros_search.similarity_pairs pair
                     JOIN jsonb_array_elements(($snapshot::jsonb)->'pairs') item ON (item->>'pair_id')::uuid = pair.pair_id
                     UNION SELECT pair.right_document_id FROM atheros_search.similarity_pairs pair
                     JOIN jsonb_array_elements(($snapshot::jsonb)->'pairs') item ON (item->>'pair_id')::uuid = pair.pair_id
                   ) ORDER BY document.document_id FOR SHARE OF document"""
          .query[String]
          .to[List]
    yield ()
