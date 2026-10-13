package com.sslproxy.coordinator.postgres.sql

import doobie.ConnectionIO
import doobie.implicits.*

object DqMetricsSql:
  def sample(installation: String, namespace: String): ConnectionIO[Map[String, Double]] =
    for
      active <- sql"""SELECT
        count(*) FILTER(WHERE status IN ('queued','preparing','retrying'))::DOUBLE PRECISION,
        coalesce(max(extract(epoch FROM CURRENT_TIMESTAMP-created_at)) FILTER(WHERE status IN ('queued','preparing','retrying')),0)::DOUBLE PRECISION,
        count(*) FILTER(WHERE status='retrying')::DOUBLE PRECISION,
        count(*) FILTER(WHERE status='applying')::DOUBLE PRECISION,
        greatest(coalesce(max(extract(epoch FROM CURRENT_TIMESTAMP-next_attempt_at)) FILTER(WHERE status='applying'),0),0)::DOUBLE PRECISION
        FROM octopus_core.dq_runs WHERE installation_id=$installation AND namespace=$namespace
        AND status NOT IN ('succeeded','failed','cancelled')""".query[(Double, Double, Double, Double, Double)].unique
      admitted <- sql"""SELECT count(*)::DOUBLE PRECISION FROM octopus_core.dq_runs WHERE installation_id=$installation
        AND namespace=$namespace AND created_at>=CURRENT_TIMESTAMP-INTERVAL '1 minute'""".query[Double].unique
      completed <- sql"""SELECT count(*)::DOUBLE PRECISION FROM octopus_core.dq_runs WHERE installation_id=$installation
        AND namespace=$namespace AND status IN ('succeeded','failed','cancelled')
        AND completed_at>=CURRENT_TIMESTAMP-INTERVAL '1 minute'""".query[Double].unique
      fences <- sql"""SELECT count(*)::DOUBLE PRECISION FROM octopus_core.dq_attempts a
        JOIN octopus_core.dq_runs r USING(run_id) WHERE r.installation_id=$installation AND r.namespace=$namespace
        AND a.status='expired' AND a.finished_at>=CURRENT_TIMESTAMP-INTERVAL '1 hour'""".query[Double].unique
      pending <- sql"""SELECT count(*)::DOUBLE PRECISION,
        count(*) FILTER(WHERE topic='dq.dlq.v1')::DOUBLE PRECISION FROM octopus_core.dq_outbox
        WHERE status<>'published' AND payload->>'installation_id'=$installation AND payload->>'namespace'=$namespace"""
        .query[(Double, Double)]
        .unique
      artifacts <-
        sql"""SELECT count(*)::DOUBLE PRECISION FROM octopus_core.dq_artifacts WHERE installation_id=$installation
        AND namespace=$namespace AND retained_until<=CURRENT_TIMESTAMP""".query[Double].unique
    yield Map(
      "queue.runs" -> active._1,
      "queue.oldest.seconds" -> active._2,
      "retrying.runs" -> active._3,
      "applying.runs" -> active._4,
      "apply.ready.age.seconds" -> active._5,
      "admissions.last.minute" -> admitted,
      "completions.last.minute" -> completed,
      "expired.attempts.last.hour" -> fences,
      "outbox.pending" -> pending._1,
      "dlq.outbox.pending" -> pending._2,
      "retention.expired.artifacts" -> artifacts
    )
