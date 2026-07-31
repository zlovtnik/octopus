package com.sslproxy.coordinator.cutover

import java.time.Instant

final class VerifiedCutoverArtifact private[cutover] (
    val artifact: CutoverArtifact,
    val canonicalArtifactSha256: String,
    val signatureSha256: String,
    val publicKeySha256: String,
    val verifiedAt: Instant,
    val devMode: Boolean = false
):
  private val cutoffIndex: Map[CutoffKey, Long] =
    artifact.cutoffs.iterator.map(cutoff => cutoff.key -> cutoff.cutoffOffset).toMap

  val verificationEvidence: CutoverVerificationEvidence =
    CutoverVerificationEvidence(
      evidenceVersion = 1,
      evidenceMode = if devMode then CutoverEvidenceMode.DevBypass else CutoverEvidenceMode.Verified,
      schemaVersion = artifact.schemaVersion,
      groupVersion = artifact.groupVersion,
      artifactId = artifact.artifactId,
      clusterId = artifact.clusterId,
      capturedAt = artifact.capturedAt,
      artifactSha256 = artifact.artifactSha256,
      canonicalArtifactSha256 = canonicalArtifactSha256,
      signatureSha256 = signatureSha256,
      publicKeySha256 = publicKeySha256,
      verifiedAt = verifiedAt
    )

  def cutoffFor(groupId: String, topic: String, partition: Int): Either[CutoverError, Long] =
    if devMode then Right(0L)
    else
      val key = CutoffKey(groupId, topic, partition)
      cutoffIndex.get(key).toRight(MissingCutoverOffset(groupId, topic, partition))

  def requireCoverage(keys: Iterable[CutoffKey]): Either[CutoverError, Unit] =
    if devMode then Right(())
    else
      val missing = keys.iterator.filterNot(cutoffIndex.contains).toList
        .sortBy(key => (key.groupId, key.topic, key.partition))
      Either.cond(missing.isEmpty, (), MissingCutoverCoverage(missing))

  def authorizeRecordOffset(
      groupId: String,
      topic: String,
      partition: Int,
      recordOffset: Long
  ): Either[CutoverError, CutoverOffsetEvidence] =
    if devMode then
      Right(evidence(
        CutoverEvidenceType.RecordOffsetAuthorization,
        groupId, topic, partition, 0L, recordOffset
      ))
    else
      cutoffFor(groupId, topic, partition).flatMap { cutoff =>
        Either.cond(
          recordOffset >= cutoff,
          evidence(
            CutoverEvidenceType.RecordOffsetAuthorization,
            groupId,
            topic,
            partition,
            cutoff,
            recordOffset
          ),
          CutoverOffsetBelowCutoff(groupId, topic, partition, cutoff, recordOffset)
        )
      }

  def verifyBootstrapPosition(
      groupId: String,
      topic: String,
      partition: Int,
      position: Long
  ): Either[CutoverError, CutoverOffsetEvidence] =
    if devMode then
      Right(evidence(
        CutoverEvidenceType.BootstrapPosition,
        groupId, topic, partition, 0L, position
      ))
    else
      cutoffFor(groupId, topic, partition).flatMap { cutoff =>
        if position < cutoff then
          Left(CutoverOffsetBelowCutoff(groupId, topic, partition, cutoff, position))
        else
          Either.cond(
            position == cutoff,
            evidence(
              CutoverEvidenceType.BootstrapPosition,
              groupId,
              topic,
              partition,
              cutoff,
              position
            ),
            CutoverBootstrapOffsetMismatch(groupId, topic, partition, cutoff, position)
          )
      }

  private def evidence(
      evidenceType: CutoverEvidenceType,
      groupId: String,
      topic: String,
      partition: Int,
      cutoffOffset: Long,
      observedOffset: Long
  ): CutoverOffsetEvidence =
    CutoverOffsetEvidence(
      verificationEvidence,
      evidenceType,
      groupId,
      topic,
      partition,
      cutoffOffset,
      observedOffset
    )

object VerifiedCutoverArtifact:
  private val DummySha256 = "0" * 64

  def devBypass(clusterId: String, verifiedAt: Instant): VerifiedCutoverArtifact =
    val artifact = CutoverArtifact(
      schemaVersion = 1,
      artifactId = "dev-bypass",
      clusterId = clusterId,
      capturedAt = verifiedAt,
      groupVersion = 1,
      cutoffs = List.empty,
      artifactSha256 = DummySha256
    )
    new VerifiedCutoverArtifact(
      artifact = artifact,
      canonicalArtifactSha256 = DummySha256,
      signatureSha256 = DummySha256,
      publicKeySha256 = DummySha256,
      verifiedAt = verifiedAt,
      devMode = true
    )
