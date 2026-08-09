package com.sslproxy.coordinator.cutover

import io.circe.Json

import java.nio.charset.StandardCharsets
import java.time.Instant

final case class DurableCutoverEvidence(canonicalJson: String, sha256: String)

enum CutoverEvidenceType(val wireValue: String):
  case RecordOffsetAuthorization extends CutoverEvidenceType("record_offset_authorization")
  case BootstrapPosition extends CutoverEvidenceType("bootstrap_position")

enum CutoverEvidenceMode(val wireValue: String):
  case Verified extends CutoverEvidenceMode("verified")
  case DevBypass extends CutoverEvidenceMode("dev_bypass")

final case class CutoverVerificationEvidence(
    evidenceVersion: Int,
    evidenceMode: CutoverEvidenceMode,
    schemaVersion: Int,
    groupVersion: Int,
    artifactId: String,
    clusterId: String,
    capturedAt: Instant,
    artifactSha256: String,
    canonicalArtifactSha256: String,
    signatureSha256: String,
    publicKeySha256: String,
    verifiedAt: Instant
):
  def durable: DurableCutoverEvidence =
    CutoverEvidenceJson.durable(
      Json.obj(
        "artifact_id" -> Json.fromString(artifactId),
        "artifact_sha256" -> Json.fromString(artifactSha256),
        "canonical_artifact_sha256" -> Json.fromString(canonicalArtifactSha256),
        "captured_at" -> Json.fromString(capturedAt.toString),
        "cluster_id" -> Json.fromString(clusterId),
        "evidence_mode" -> Json.fromString(evidenceMode.wireValue),
        "evidence_type" -> Json.fromString("artifact_verification"),
        "evidence_version" -> Json.fromInt(evidenceVersion),
        "group_version" -> Json.fromInt(groupVersion),
        "public_key_sha256" -> Json.fromString(publicKeySha256),
        "schema_version" -> Json.fromInt(schemaVersion),
        "signature_sha256" -> Json.fromString(signatureSha256),
        "verified_at" -> Json.fromString(verifiedAt.toString)
      )
    )

final case class CutoverOffsetEvidence(
    verification: CutoverVerificationEvidence,
    evidenceType: CutoverEvidenceType,
    groupId: String,
    topic: String,
    partition: Int,
    cutoffOffset: Long,
    observedOffset: Long
):
  def key: CutoffKey = CutoffKey(groupId, topic, partition)

  def durable: DurableCutoverEvidence =
    CutoverEvidenceJson.durable(
      Json.obj(
        "artifact_id" -> Json.fromString(verification.artifactId),
        "artifact_sha256" -> Json.fromString(verification.artifactSha256),
        "canonical_artifact_sha256" -> Json.fromString(verification.canonicalArtifactSha256),
        "captured_at" -> Json.fromString(verification.capturedAt.toString),
        "cluster_id" -> Json.fromString(verification.clusterId),
        "cutoff_offset" -> Json.fromLong(cutoffOffset),
        "evidence_type" -> Json.fromString(evidenceType.wireValue),
        "evidence_mode" -> Json.fromString(verification.evidenceMode.wireValue),
        "evidence_version" -> Json.fromInt(verification.evidenceVersion),
        "group_id" -> Json.fromString(groupId),
        "group_version" -> Json.fromInt(verification.groupVersion),
        "observed_offset" -> Json.fromLong(observedOffset),
        "partition" -> Json.fromInt(partition),
        "public_key_sha256" -> Json.fromString(verification.publicKeySha256),
        "schema_version" -> Json.fromInt(verification.schemaVersion),
        "signature_sha256" -> Json.fromString(verification.signatureSha256),
        "topic" -> Json.fromString(topic),
        "verified_at" -> Json.fromString(verification.verifiedAt.toString)
      )
    )

private object CutoverEvidenceJson:
  def durable(json: Json): DurableCutoverEvidence =
    val canonical = CanonicalCutoverJson.render(json)
    DurableCutoverEvidence(
      canonicalJson = canonical,
      sha256 = CutoverSha256.hex(canonical.getBytes(StandardCharsets.UTF_8))
    )
