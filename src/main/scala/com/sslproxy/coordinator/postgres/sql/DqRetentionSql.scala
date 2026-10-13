package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import doobie.ConnectionIO
import doobie.implicits.*
import io.circe.Json
import io.circe.parser.parse

/** Retention never deletes an active run or evidence for an open review.
  * Object deletion happens outside database transactions, by exact version.
  * Final human decisions cannot be reopened by the existing decision API.
  */
object DqRetentionSql:
  def reopenArtifacts(installation: String, namespace: String, limit: Int): ConnectionIO[Int] =
    sql"""WITH reopened AS (
      SELECT r.run_id FROM octopus_core.dq_runs r
      WHERE r.installation_id=$installation AND r.namespace=$namespace AND r.projection_applied
        AND (jsonb_typeof(r.summary->'review_candidate_ids') IS DISTINCT FROM 'array' OR EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(COALESCE(r.summary->'review_candidate_ids','[]'::JSONB)) ids(candidate_id)
          LEFT JOIN atheros_search.merge_candidates c USING(candidate_id)
          WHERE c.candidate_id IS NULL OR c.status NOT IN ('confirmed','rejected','not_match')
        )) AND EXISTS(SELECT 1 FROM octopus_core.dq_artifacts a WHERE a.run_id=r.run_id AND a.closed_at IS NOT NULL)
      ORDER BY r.run_id LIMIT $limit
    ) UPDATE octopus_core.dq_artifacts a SET closed_at=NULL,retained_until=NULL
      FROM reopened WHERE a.run_id=reopened.run_id AND a.closed_at IS NOT NULL""".update.run

  def closeArtifacts(installation: String, namespace: String, limit: Int): ConnectionIO[Int] =
    sql"""WITH eligible AS (
      SELECT r.run_id, GREATEST(r.completed_at, COALESCE((
        SELECT max(GREATEST(c.updated_at, d.decided_at))
        FROM jsonb_array_elements_text(COALESCE(r.summary->'review_candidate_ids','[]'::JSONB)) ids(candidate_id)
        JOIN atheros_search.merge_candidates c USING(candidate_id)
        LEFT JOIN atheros_search.merge_decisions d USING(candidate_id)
      ),r.completed_at)) closed_at
      FROM octopus_core.dq_runs r
      WHERE r.installation_id=$installation AND r.namespace=$namespace
        AND r.status IN ('succeeded','failed','cancelled') AND r.completed_at IS NOT NULL
        AND (NOT r.projection_applied OR jsonb_typeof(r.summary->'review_candidate_ids')='array')
        AND NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(COALESCE(r.summary->'review_candidate_ids','[]'::JSONB)) ids(candidate_id)
          LEFT JOIN atheros_search.merge_candidates c USING(candidate_id)
          WHERE c.candidate_id IS NULL OR c.status NOT IN ('confirmed','rejected','not_match')
        ) AND EXISTS(SELECT 1 FROM octopus_core.dq_artifacts a WHERE a.run_id=r.run_id
          AND (a.closed_at IS NULL OR a.retained_until<=CURRENT_TIMESTAMP))
    ), closed AS (
      SELECT e.run_id,e.closed_at FROM eligible e WHERE EXISTS (
        SELECT 1 FROM octopus_core.dq_artifacts a WHERE a.run_id=e.run_id
        AND (a.closed_at IS NULL OR a.closed_at<e.closed_at)
      ) ORDER BY e.closed_at,e.run_id LIMIT $limit
    ) UPDATE octopus_core.dq_artifacts a SET closed_at=closed.closed_at,
      retained_until=closed.closed_at+INTERVAL '90 days'
      FROM closed WHERE a.run_id=closed.run_id AND (a.closed_at IS NULL OR a.closed_at<closed.closed_at)""".update.run

  def expiredArtifacts(installation: String, namespace: String, limit: Int): ConnectionIO[List[Json]] =
    sql"""SELECT a.reference::TEXT FROM octopus_core.dq_artifacts a
      JOIN octopus_core.dq_runs r ON r.run_id=a.run_id
      WHERE a.installation_id=$installation AND a.namespace=$namespace AND a.retained_until<=CURRENT_TIMESTAMP
        AND r.status IN ('succeeded','failed','cancelled')
        AND (NOT r.projection_applied OR jsonb_typeof(r.summary->'review_candidate_ids')='array')
        AND NOT EXISTS (
          SELECT 1 FROM jsonb_array_elements_text(COALESCE(r.summary->'review_candidate_ids','[]'::JSONB)) ids(candidate_id)
          LEFT JOIN atheros_search.merge_candidates c USING(candidate_id)
          LEFT JOIN atheros_search.merge_decisions d USING(candidate_id)
          WHERE c.candidate_id IS NULL OR c.status NOT IN ('confirmed','rejected','not_match')
            OR GREATEST(c.updated_at,d.decided_at)>CURRENT_TIMESTAMP-INTERVAL '90 days'
        )
      ORDER BY a.retained_until,a.artifact_id LIMIT $limit""".query[String].to[List]
      .flatMap(_.traverse(raw => parse(raw).fold(error => doobie.free.connection.raiseError[Json](error), _.pure[ConnectionIO])))

  def referenced(uri: String, version: String): ConnectionIO[Boolean] =
    sql"""SELECT EXISTS(SELECT 1 FROM octopus_core.dq_artifacts
      WHERE reference->>'uri'=$uri AND reference->>'version_id'=$version)""".query[Boolean].unique

  def forgetArtifact(id: String, version: String): ConnectionIO[Int] =
    sql"""DELETE FROM octopus_core.dq_artifacts WHERE artifact_id=$id
      AND reference->>'version_id'=$version AND retained_until<=CURRENT_TIMESTAMP""".update.run

  def forgetInvalidArtifact(installation: String, namespace: String, reference: Json): ConnectionIO[Int] =
    sql"""DELETE FROM octopus_core.dq_artifacts WHERE installation_id=$installation AND namespace=$namespace
      AND reference=${reference.noSpaces}::JSONB AND retained_until<=CURRENT_TIMESTAMP""".update.run

  def pruneRuns(installation: String, namespace: String, limit: Int): ConnectionIO[Int] =
    for
      ids <- sql"""SELECT r.run_id FROM octopus_core.dq_runs r
        WHERE r.installation_id=$installation AND r.namespace=$namespace
          AND r.status IN ('succeeded','failed','cancelled') AND r.completed_at<CURRENT_TIMESTAMP-INTERVAL '90 days'
          AND NOT EXISTS(SELECT 1 FROM octopus_core.dq_artifacts a WHERE a.run_id=r.run_id)
          AND NOT EXISTS(SELECT 1 FROM octopus_core.dq_outbox o WHERE o.payload->>'run_id'=r.run_id AND o.status<>'published')
        ORDER BY r.completed_at,r.run_id FOR UPDATE SKIP LOCKED LIMIT $limit""".query[String].to[List]
      counts <- ids.traverse { id =>
        sql"DELETE FROM octopus_core.dq_attempts WHERE run_id=$id".update.run *>
          sql"DELETE FROM octopus_core.dq_runs WHERE run_id=$id".update.run
      }
    yield counts.sum

  def pruneEvidence(installation: String, namespace: String, limit: Int): ConnectionIO[Int] =
    for
      receipts <- sql"""DELETE FROM octopus_core.dq_receipts WHERE ctid IN (
      SELECT ctid FROM octopus_core.dq_receipts WHERE installation_id=$installation AND namespace=$namespace
      AND received_at<CURRENT_TIMESTAMP-INTERVAL '90 days' LIMIT $limit)""".update.run
      audit <- sql"""DELETE FROM octopus_core.dq_audit WHERE audit_id IN (
      SELECT audit_id FROM octopus_core.dq_audit WHERE installation_id=$installation AND namespace=$namespace
      AND created_at<CURRENT_TIMESTAMP-INTERVAL '180 days' LIMIT $limit)""".update.run
      outbox <- sql"""DELETE FROM octopus_core.dq_outbox WHERE outbox_id IN (
      SELECT outbox_id FROM octopus_core.dq_outbox WHERE status='published'
      AND payload->>'installation_id'=$installation AND payload->>'namespace'=$namespace
      AND published_at<CURRENT_TIMESTAMP-INTERVAL '180 days' LIMIT $limit)""".update.run
      workers <- sql"""DELETE FROM octopus_core.dq_workers WHERE worker_instance_id IN (
      SELECT w.worker_instance_id FROM octopus_core.dq_workers w
      WHERE w.installation_id=$installation AND w.namespace=$namespace AND w.last_seen_at<CURRENT_TIMESTAMP-INTERVAL '1 day'
      AND NOT EXISTS(SELECT 1 FROM octopus_core.dq_attempts a WHERE a.worker_instance_id=w.worker_instance_id
        AND a.status IN ('reserved','accepted','running','applying')) LIMIT $limit)""".update.run
    yield receipts + audit + outbox + workers
