package com.sslproxy.coordinator.postgres.sql

import doobie.Query0
import doobie.implicits.*

object DqProjectionSql:
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
    sql"""SELECT decision.candidate_id, decision.decision, policy.authorized_scope
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
             SELECT 1 FROM octopus_core.dq_runs run
             WHERE run.installation_id = $installation AND run.namespace = $namespace
               AND run.operation = 'reconcile_decision'
               AND run.document->'parameters'->>'candidate_id' = decision.candidate_id
               AND run.document->'parameters'->>'authorized_scope' = policy.authorized_scope
               AND run.policy_version = policy.policy_version
               AND run.execution_mode = policy.mode
           )
           ORDER BY decision.candidate_id, policy.authorized_scope LIMIT $limit"""
      .query[(String, String, String)]
