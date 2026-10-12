package com.sslproxy.coordinator.dq

import com.sslproxy.coordinator.postgres.sql.DqSnapshotSql
import doobie.ConnectionIO
import io.circe.Json
import io.circe.parser.parse

object DqSnapshot:
  import DqArtifacts.required

  def prepare(run: Json, minimumSimilarity: Double): ConnectionIO[Json] =
    val parameters = run.hcursor
      .downField("parameters")
      .focus
      .getOrElse(throw DqPermanentError("parameters_missing"))
    val scope = required(parameters, "authorized_scope")
    val operation = required(run, "operation")
    val limit = parameters.hcursor
      .get[Int]("max_candidates")
      .toOption
      .filter(value => value >= 1 && value <= 1000)
      .getOrElse(throw DqPermanentError("candidate_limit_invalid"))
    val candidateId =
      parameters.hcursor.get[String]("candidate_id").toOption.filter(_.nonEmpty)
    if !Set("prepare_candidates", "reconcile_decision").contains(operation) ||
      (operation == "reconcile_decision" && candidateId.isEmpty)
    then throw DqPermanentError("operation_invalid")
    DqSnapshotSql
      .source(scope, operation, limit, candidateId, minimumSimilarity)
      .unique
      .map { source =>
        val rows = parse(source).fold(
          _ => throw DqPermanentError("source_json_invalid"),
          identity
        )
        val metadata = Json.obj(
          "installation_id" -> Json
            .fromString(required(run, "installation_id")),
          "namespace" -> Json.fromString(required(run, "namespace")),
          "authorized_scope" -> Json.fromString(scope),
          "operation" -> Json.fromString(operation),
          "policy_version" -> Json.fromString(required(run, "policy_version"))
        )
        val base = rows
          .deepMerge(metadata)
          .deepMerge(
            candidateId.fold(Json.obj())(value =>
              Json.obj("candidate_id" -> Json.fromString(value))
            )
          )
        val digest = DqArtifacts.digest(base)
        val requested = parameters.hcursor
          .get[String]("snapshot_revision")
          .toOption
          .filter(_.nonEmpty)
        if requested.exists(_ != digest) then throw DqStaleSource()
        base.deepMerge(
          Json.obj("source_revision_digest" -> Json.fromString(digest))
        )
      }

  def revision(snapshot: Json): String =
    required(snapshot, "source_revision_digest")

  def validateDigest(snapshot: Json): Boolean =
    snapshot.asObject.exists { obj =>
      snapshot.hcursor
        .get[String]("source_revision_digest")
        .contains(
          DqArtifacts.digest(
            Json.fromJsonObject(obj.remove("source_revision_digest"))
          )
        )
    }
