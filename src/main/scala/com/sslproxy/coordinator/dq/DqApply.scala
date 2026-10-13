package com.sslproxy.coordinator.dq

import cats.syntax.all.*
import com.sslproxy.coordinator.postgres.sql.{DqProjectionSql, DqSnapshotSql, IdentityGraphSql}
import com.sslproxy.coordinator.processor.IntelligencePreparation
import doobie.ConnectionIO
import io.circe.{Decoder, Json}
import io.circe.generic.semiauto.deriveDecoder

private[dq] final case class DqCandidateProposal(mac_a: String, mac_b: String, confidence: Double, pair_id: String)
private[dq] final case class DqReconciliation(candidate_id: String, decision: String)
private[dq] final case class DqProposalSummary(candidate_count: Int, reconciliation_count: Int)
private[dq] final case class DqProposal(
  run_id: String,
  attempt_id: String,
  fence: Long,
  operation: String,
  source_revision_digest: String,
  policy_version: String,
  candidates: Vector[DqCandidateProposal],
  reconciliations: Vector[DqReconciliation],
  summary: DqProposalSummary
)

object DqApply:
  import DqArtifacts.required
  private given Decoder[DqCandidateProposal] = deriveDecoder
  private given Decoder[DqReconciliation] = deriveDecoder
  private given Decoder[DqProposalSummary] = deriveDecoder
  private given Decoder[DqProposal] = deriveDecoder
  private val Mac = "^[0-9a-f]{2}(:[0-9a-f]{2}){5}$".r

  private def exactFields(value: Json, fields: Set[String]): Boolean =
    value.asObject.exists(_.keys.toSet == fields)

  private[dq] def validate(run: Json, snapshot: Json, output: Json): DqProposal =
    val proposal = output.as[DqProposal].fold(_ => throw DqPermanentError("proposal_schema_invalid"), identity)
    val fields = Set(
      "run_id",
      "attempt_id",
      "fence",
      "operation",
      "source_revision_digest",
      "policy_version",
      "candidates",
      "reconciliations",
      "summary"
    )
    val candidateFields = Set("mac_a", "mac_b", "confidence", "pair_id")
    val reconciliationFields = Set("candidate_id", "decision")
    val summaryFields = Set("candidate_count", "reconciliation_count")
    val attempt = run.hcursor.downField("current_attempt").focus.getOrElse(throw DqPermanentError("attempt_missing"))
    val scope = run.hcursor.downField("parameters").get[String]("authorized_scope").toOption
    val limit = run.hcursor.downField("parameters").get[Int]("max_candidates").toOption.getOrElse(0)
    val sameIdentity = proposal.run_id == required(run, "run_id") &&
      proposal.attempt_id == required(attempt, "attempt_id") && attempt.hcursor
        .get[Long]("fence")
        .contains(proposal.fence) &&
      proposal.operation == required(run, "operation") && proposal.policy_version == required(run, "policy_version") &&
      proposal.source_revision_digest == required(run, "source_revision_digest") &&
      proposal.source_revision_digest == DqSnapshot.revision(snapshot) && DqSnapshot.validateDigest(snapshot) &&
      snapshot.hcursor.get[String]("installation_id").contains(required(run, "installation_id")) &&
      snapshot.hcursor.get[String]("namespace").contains(required(run, "namespace")) &&
      snapshot.hcursor.get[String]("operation").contains(proposal.operation) &&
      snapshot.hcursor.get[String]("policy_version").contains(proposal.policy_version) &&
      snapshot.hcursor.get[String]("authorized_scope").toOption == scope &&
      required(run, "processor_id") == "atheros.dedup"
    val strictFields = exactFields(output, fields) &&
      output.hcursor.downField("summary").focus.exists(exactFields(_, summaryFields)) &&
      output.hcursor.get[Vector[Json]]("candidates").exists(_.forall(exactFields(_, candidateFields))) &&
      output.hcursor.get[Vector[Json]]("reconciliations").exists(_.forall(exactFields(_, reconciliationFields)))
    if !sameIdentity || !strictFields || proposal.candidates.size > limit ||
      proposal.summary.candidate_count != proposal.candidates.size ||
      proposal.summary.reconciliation_count != proposal.reconciliations.size
    then throw DqPermanentError("proposal_identity_mismatch")
    val pairs =
      snapshot.hcursor.get[Vector[Json]]("pairs").fold(_ => throw DqPermanentError("snapshot_pairs_invalid"), identity)
    val decisions = snapshot.hcursor
      .get[Vector[Json]]("decisions")
      .fold(_ => throw DqPermanentError("snapshot_decisions_invalid"), identity)
    val candidates = snapshot.hcursor
      .get[Vector[Json]]("candidates")
      .fold(_ => throw DqPermanentError("snapshot_candidates_invalid"), identity)
    val devices = snapshot.hcursor
      .get[Vector[Json]]("devices")
      .fold(_ => throw DqPermanentError("snapshot_devices_invalid"), identity)
    val macs = devices.map(required(_, "mac")).toSet
    proposal.candidates.foreach { candidate =>
      val pairMatches = pairs.exists { source =>
        source.hcursor.get[String]("pair_id").contains(candidate.pair_id) &&
        source.hcursor.get[String]("mac_a").contains(candidate.mac_a) &&
        source.hcursor.get[String]("mac_b").contains(candidate.mac_b) &&
        source.hcursor.get[Double]("confidence").contains(candidate.confidence)
      }
      val decided = candidates
        .find(source =>
          source.hcursor.get[String]("mac_a").contains(candidate.mac_a) &&
            source.hcursor.get[String]("mac_b").contains(candidate.mac_b)
        )
        .exists { existing =>
          decisions.exists(decision => required(decision, "candidate_id") == required(existing, "candidate_id"))
        }
      if !Mac.matches(candidate.mac_a) || !Mac.matches(candidate.mac_b) || candidate.mac_a >= candidate.mac_b ||
        !candidate.confidence.isFinite || candidate.confidence < 0.0d || candidate.confidence > 1.0d ||
        !pairMatches || decided || !macs(candidate.mac_a) || !macs(candidate.mac_b)
      then throw DqPermanentError("proposal_candidate_forbidden")
    }
    if proposal.candidates.distinctBy(value => (value.mac_a, value.mac_b)).size != proposal.candidates.size then
      throw DqPermanentError("proposal_duplicate_pair")
    proposal.operation match
      case "prepare_candidates" =>
        if proposal.reconciliations.nonEmpty then throw DqPermanentError("proposal_operation_mismatch")
      case "reconcile_decision" =>
        val candidateId = run.hcursor.downField("parameters").get[String]("candidate_id").toOption
        if proposal.candidates.nonEmpty || proposal.reconciliations.size != 1 ||
          !proposal.reconciliations.forall { reconciliation =>
            candidateId.contains(reconciliation.candidate_id) && candidates.exists(
              _.hcursor.get[String]("candidate_id").contains(reconciliation.candidate_id)
            ) &&
            decisions.exists(source =>
              source.hcursor.get[String]("candidate_id").contains(reconciliation.candidate_id) &&
                source.hcursor.get[String]("decision").contains(reconciliation.decision)
            )
          }
        then throw DqPermanentError("proposal_decision_forbidden")
      case _ => throw DqPermanentError("proposal_operation_unsupported")
    proposal

  private[dq] def shadowComparison(snapshot: Json, proposal: DqProposal): Json =
    val pairs = snapshot.hcursor.get[Vector[Json]]("pairs").getOrElse(Vector.empty)
    val legacy = pairs.foldLeft(Map.empty[(String, String), Double]) { (current, pair) =>
      val key = (required(pair, "mac_a"), required(pair, "mac_b"))
      val confidence =
        pair.hcursor.get[Double]("confidence").fold(_ => throw DqPermanentError("source_confidence_invalid"), identity)
      current.updated(key, current.get(key).fold(confidence)(_.max(confidence)))
    }
    val proposed = proposal.candidates.map(value => (value.mac_a, value.mac_b) -> value.confidence).toMap
    Json.obj(
      "source_revision_digest" -> Json.fromString(proposal.source_revision_digest),
      "legacy_candidate_count" -> Json.fromInt(legacy.size),
      "proposal_candidate_count" -> Json.fromInt(proposed.size),
      "missing_pairs" -> Json.fromInt((legacy.keySet -- proposed.keySet).size),
      "additional_pairs" -> Json.fromInt((proposed.keySet -- legacy.keySet).size),
      "confidence_differences" -> Json.fromInt(legacy.count { case (key, confidence) =>
        proposed.get(key).exists(_ != confidence)
      }),
      "reconciliation_count" -> Json.fromInt(proposal.reconciliations.size)
    )

  def transaction(
    store: DqStore,
    owner: String,
    run: Json,
    snapshot: Json,
    output: Json,
    minimumSimilarity: Double
  ): ConnectionIO[Boolean] =
    val proposal = validate(run, snapshot, output)
    val shadow = required(run, "execution_mode") == "shadow"
    for
      _ <- DqSnapshotSql.serializable
      authorized <- store.guardOutputTx(
        proposal.run_id,
        proposal.attempt_id,
        proposal.fence,
        owner,
        proposal.source_revision_digest
      )
      completed <-
        if !authorized then false.pure[ConnectionIO]
        else
          for
            _ <- DqSnapshotSql.lockSources(DqArtifacts.CanonicalPrinter.print(snapshot))
            current <- DqSnapshot.prepare(run, minimumSimilarity)
            _ <-
              if DqSnapshot.revision(current) == proposal.source_revision_digest then ().pure[ConnectionIO]
              else doobie.free.connection.raiseError[Unit](DqStaleSource())
            ids <-
              if shadow then List.empty[String].pure[ConnectionIO]
              else
                proposal.candidates.toList.traverse { candidate =>
                  IdentityGraphSql.persistMergeCandidate(candidate.mac_a, candidate.mac_b, candidate.confidence) *>
                    DqProjectionSql.candidateId(candidate.mac_a, candidate.mac_b).unique
                }
            _ <-
              if shadow then ().pure[ConnectionIO]
              else
                IdentityGraphSql
                  .reconcileMergeConfirmationsFor(ids ++ proposal.reconciliations.map(_.candidate_id))
                  .flatMap { _ =>
                    // Derived clusters remain global. Replacing only a scoped subset
                    // would erase identities maintained by another scope or path.
                    IdentityGraphSql.approvedIdentityEdges.to[List].flatMap { edges =>
                      IdentityGraphSql.replaceClusters(IntelligencePreparation.identityClusters(edges).toList)
                    }
                  }
                  .void
            summary = Json
              .obj(
                "candidate_count" -> Json.fromInt(proposal.candidates.size),
                "reconciliation_count" -> Json.fromInt(proposal.reconciliations.size),
                "review_candidate_ids" -> Json.fromValues(
                  (ids ++ proposal.reconciliations.map(_.candidate_id)).distinct.sorted.map(Json.fromString)
                )
              )
              .deepMerge(
                if shadow then Json.obj("shadow_comparison" -> shadowComparison(snapshot, proposal)) else Json.obj()
              )
            done <- store.completeOutputTx(
              proposal.run_id,
              proposal.attempt_id,
              proposal.fence,
              owner,
              proposal.source_revision_digest,
              !shadow,
              summary
            )
            _ <-
              if done then ().pure[ConnectionIO]
              else doobie.free.connection.raiseError[Unit](DqPermanentError("application_fence_lost"))
          yield done
    yield completed
