package com.sslproxy.coordinator.postgres.sql

import com.sslproxy.coordinator.dq.DqError
import doobie.{ConnectionIO, Fragment, Query0}
import doobie.implicits.*

object DqProjectionSql:
  private def decisionRevision(
    installation: String,
    namespace: String,
    scope: Fragment,
    policy: Fragment,
    mode: Fragment
  ): Fragment =
    // Runtime binds strings as unspecified for enum/JSON compatibility. Every
    // variadic JSON argument therefore needs a concrete PostgreSQL type.
    fr"encode(sha256(convert_to(jsonb_build_array(" ++ fr"$installation::TEXT,$namespace::TEXT," ++
      scope ++ fr"::TEXT," ++ policy ++ fr"::TEXT," ++ mode ++
      fr"::TEXT,decision.candidate_id,decision.decision)::text,'UTF8')),'hex')"

  def recordDecision(
    installation: String,
    namespace: String,
    scope: String,
    policy: String,
    mode: String,
    candidateId: String,
    runId: String
  ): ConnectionIO[Int] =
    val revision = decisionRevision(installation, namespace, fr"$scope", fr"$policy", fr"$mode")
    val available = sql"""SELECT EXISTS(SELECT 1 FROM atheros_search.merge_decisions decision
      JOIN atheros_search.merge_candidates candidate ON candidate.candidate_id=decision.candidate_id
      JOIN atheros_search.devices left_device ON left_device.mac=candidate.mac_a
      JOIN atheros_search.devices right_device ON right_device.mac=candidate.mac_b
      WHERE decision.candidate_id=$candidateId AND ($scope='*' OR
        (left_device.location_id=$scope AND right_device.location_id=$scope)))""".query[Boolean].unique
    available.flatMap { exists =>
      if !exists then doobie.free.connection.raiseError[Int](DqError(404, "decision_not_found"))
      else (fr"""INSERT INTO octopus_core.dq_decision_sources(candidate_id,decision,decision_revision,run_id)
      SELECT decision.candidate_id,decision.decision,""" ++ revision ++ fr""",$runId
      FROM atheros_search.merge_decisions decision
      JOIN atheros_search.merge_candidates candidate ON candidate.candidate_id=decision.candidate_id
      JOIN atheros_search.devices left_device ON left_device.mac=candidate.mac_a
      JOIN atheros_search.devices right_device ON right_device.mac=candidate.mac_b
      WHERE decision.candidate_id=$candidateId AND ($scope='*' OR
        (left_device.location_id=$scope AND right_device.location_id=$scope))
      ON CONFLICT(candidate_id,decision_revision) DO UPDATE SET run_id=EXCLUDED.run_id
      WHERE dq_decision_sources.completed_at IS NULL""").update.run
    }

  def candidateId(left: String, right: String): Query0[String] =
    sql"""SELECT candidate_id FROM atheros_search.merge_candidates
           WHERE mac_a = $left AND mac_b = $right""".query[String]

  /** Anti-join the immutable decision identity rather than a wall-clock cursor.
    * Failed and cancelled runs are surfaced for explicit replay; they are never
    * silently considered projected or rediscovered in a hot loop.
    */
  def decisions(
    installation: String,
    namespace: String,
    limit: Int
  ): Query0[(String, String, String)] =
    val revision =
      decisionRevision(installation, namespace, fr"policy.authorized_scope", fr"policy.policy_version", fr"policy.mode")
    (fr"""SELECT decision.candidate_id, decision.decision, policy.authorized_scope
           FROM atheros_search.merge_decisions decision
           JOIN atheros_search.merge_candidates candidate ON candidate.candidate_id = decision.candidate_id
           JOIN atheros_search.devices left_device ON left_device.mac = candidate.mac_a
           JOIN atheros_search.devices right_device ON right_device.mac = candidate.mac_b
           JOIN octopus_core.dq_scope_policies policy
             ON policy.installation_id = $installation AND policy.namespace = $namespace
            AND policy.mode IN ('shadow','enabled')
            AND (policy.authorized_scope = '*' OR (
              left_device.location_id = policy.authorized_scope AND right_device.location_id = policy.authorized_scope
            ))
           WHERE NOT EXISTS (
             SELECT 1 FROM octopus_core.dq_decision_sources source
             WHERE source.candidate_id=decision.candidate_id AND source.decision_revision=""" ++ revision ++ fr""")
           ORDER BY decision.candidate_id, policy.authorized_scope LIMIT $limit""")
      .query[(String, String, String)]
