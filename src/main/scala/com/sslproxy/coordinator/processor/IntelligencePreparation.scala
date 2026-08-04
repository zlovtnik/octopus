package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*

import java.sql.Timestamp
import java.time.Instant

final case class ProjectionFrame(
    dedupeKey: String,
    sourceMac: String,
    locationId: Option[String],
    sensorId: Option[String],
    observedAt: Timestamp,
    frameType: Option[String],
    frameSubtype: Option[String],
    signalDbm: Option[Int],
    retry: Boolean,
    protectedFrame: Boolean,
    bssid: Option[String],
    appProtocol: Option[String],
    adjacentMacHint: Option[String],
    tsftDeltaUs: Option[Long],
    wallClockDeltaMs: Option[Long],
    sessionKey: Option[String]
)

final case class BehaviorSnapshotProjection(
    snapshotId: String,
    snapshotKey: String,
    sourceMac: String,
    locationId: Option[String],
    sensorId: Option[String],
    windowStart: Timestamp,
    windowEnd: Timestamp,
    eventCount: Long,
    textSummary: String,
    protocolMixJson: String,
    frameDistributionJson: String,
    signalMin: Option[Int],
    signalMax: Option[Int],
    signalAverage: Option[Double],
    retryCount: Long,
    protectedCount: Long,
    unprotectedCount: Long,
    uniqueBssidCount: Long,
    rotationIndicatorsJson: String,
    projectionRunId: String
)

final case class TimingProfileProjection(
    profileId: String,
    profileKey: String,
    sourceMac: String,
    sensorId: Option[String],
    locationId: Option[String],
    windowStart: Timestamp,
    windowEnd: Timestamp,
    tsftP50: Option[Double],
    tsftP95: Option[Double],
    tsftJitter: Option[Double],
    wallP50: Option[Double],
    wallJitter: Option[Double],
    sourceEventCount: Long,
    projectionRunId: String
)

final case class SequenceProjection(
    sessionKey: String,
    sourceMac: Option[String],
    locationId: Option[String],
    sensorId: Option[String],
    windowStart: Timestamp,
    windowEnd: Timestamp,
    tokens: Vector[FrameToken],
    projectionRunId: String
)

final case class BaselineProjection(
    baselineId: String,
    bssid: String,
    metric: String,
    p5: Double,
    p50: Double,
    p95: Double,
    sampleCount: Long,
    projectionRunId: String
)

final case class SimilarityCandidate(
    pairKind: String,
    embeddingKind: String,
    embeddingModel: String,
    leftDocumentId: String,
    rightDocumentId: String,
    leftSourceTable: String,
    leftSourceKey: String,
    leftSourceMac: Option[String],
    leftSensorId: Option[String],
    leftLocationId: Option[String],
    leftObservedAt: Option[Timestamp],
    rightSourceTable: String,
    rightSourceKey: String,
    rightSourceMac: Option[String],
    rightSensorId: Option[String],
    rightLocationId: Option[String],
    rightObservedAt: Option[Timestamp],
    cosineDistance: Double
)

final case class SimilarityProjection(
    pairId: String,
    candidate: SimilarityCandidate,
    cosineSimilarity: Double,
    evidenceJson: String,
    projectionRunId: String
)

final case class IdentityClusterProjection(
    clusterId: String,
    members: Vector[String],
    confidence: Double,
    projectionRunId: String
)

final case class DnsThreatCandidate(
    host: String,
    blockedCount: Long,
    attemptedBytes: Long,
    recentCount: Long,
    lastSeen: Timestamp
)

final case class DnsThreatProjection(
    sourceKey: String,
    signalId: String,
    score: Double,
    severity: String,
    evidenceJson: String,
    detectedAt: Timestamp,
    projectionRunId: String
)

final case class ApRiskProjection(
    bssid: String,
    composite: Double,
    signalRisk: Double,
    identityRisk: Double,
    behaviorRisk: Double,
    evidenceJson: String,
    projectionRunId: String
)

object IntelligencePreparation:
  private val WindowSeconds = 3600L

  def behavior(frames: List[ProjectionFrame]): List[BehaviorSnapshotProjection] =
    frames.groupBy(frame => frame.sourceMac -> windowStart(frame.observedAt.toInstant)).toList
      .sortBy((key, _) => key)
      .map { case ((sourceMac, start), grouped) =>
        val ordered = grouped.sortBy(_.observedAt.getTime)
        val end = start.plusSeconds(WindowSeconds)
        val signals = grouped.flatMap(_.signalDbm).map(_.toDouble).toVector
        val protocols = counts(grouped.flatMap(_.appProtocol).map(_.toLowerCase(java.util.Locale.ROOT)))
        val framesByKind = counts(grouped.map(frame => FrameToken.normalize(frame.frameType, frame.frameSubtype).value))
        val adjacent = grouped.flatMap(_.adjacentMacHint).distinct.sorted
        val key = s"$sourceMac:${start.getEpochSecond}"
        val runId = ProjectionFunctions.stableId("behavior-run", Vector(key))
        BehaviorSnapshotProjection(
          ProjectionFunctions.stableId("behavior", Vector(key)),
          key,
          sourceMac,
          ordered.flatMap(_.locationId).lastOption,
          ordered.flatMap(_.sensorId).lastOption,
          Timestamp.from(start),
          Timestamp.from(end),
          grouped.size.toLong,
          s"${grouped.size} wireless frames for $sourceMac from $start to $end",
          protocols.asJson.noSpaces,
          framesByKind.asJson.noSpaces,
          signals.minOption.map(_.toInt),
          signals.maxOption.map(_.toInt),
          Option.when(signals.nonEmpty)(signals.sum / signals.size.toDouble),
          grouped.count(_.retry).toLong,
          grouped.count(_.protectedFrame).toLong,
          grouped.count(frame => !frame.protectedFrame).toLong,
          grouped.flatMap(_.bssid).distinct.size.toLong,
          Json.obj(
            "adjacent_mac_count" -> Json.fromInt(adjacent.size),
            "adjacent_macs" -> adjacent.asJson,
            "rotation_suspected" -> Json.fromBoolean(adjacent.nonEmpty)
          ).noSpaces,
          runId
        )
      }

  def timing(frames: List[ProjectionFrame]): List[TimingProfileProjection] =
    frames.groupBy(frame => frame.sourceMac -> windowStart(frame.observedAt.toInstant)).toList
      .sortBy((key, _) => key)
      .map { case ((sourceMac, start), grouped) =>
        val ordered = grouped.sortBy(_.observedAt.getTime)
        val tsft = grouped.flatMap(_.tsftDeltaUs).map(_.toDouble).toVector
        val wall = grouped.flatMap(_.wallClockDeltaMs).map(_.toDouble).toVector
        val key = s"$sourceMac:${start.getEpochSecond}"
        TimingProfileProjection(
          ProjectionFunctions.stableId("timing", Vector(key)),
          key,
          sourceMac,
          ordered.flatMap(_.sensorId).lastOption,
          ordered.flatMap(_.locationId).lastOption,
          Timestamp.from(start),
          Timestamp.from(start.plusSeconds(WindowSeconds)),
          ProjectionFunctions.percentile(tsft, 0.5d),
          ProjectionFunctions.percentile(tsft, 0.95d),
          ProjectionFunctions.medianAbsoluteDeviation(tsft),
          ProjectionFunctions.percentile(wall, 0.5d),
          ProjectionFunctions.medianAbsoluteDeviation(wall),
          grouped.size.toLong,
          ProjectionFunctions.stableId("timing-run", Vector(key))
        )
      }

  def sequences(frames: List[ProjectionFrame]): List[SequenceProjection] =
    frames.flatMap(frame => frame.sessionKey.map(_ -> frame)).groupMap(_._1)(_._2).toList
      .sortBy(_._1)
      .map { case (sessionKey, grouped) =>
        val ordered = grouped.sortBy(_.observedAt.getTime)
        SequenceProjection(
          sessionKey,
          ordered.headOption.map(_.sourceMac),
          ordered.flatMap(_.locationId).lastOption,
          ordered.flatMap(_.sensorId).lastOption,
          ordered.headOption.fold(Timestamp.from(Instant.EPOCH))(_.observedAt),
          ordered.lastOption.fold(Timestamp.from(Instant.EPOCH))(_.observedAt),
          ordered.map(frame => FrameToken.normalize(frame.frameType, frame.frameSubtype)).toVector,
          ProjectionFunctions.stableId("sequence-run", Vector(sessionKey))
        )
      }

  def baseline(bssid: String, signals: Vector[Double]): Option[BaselineProjection] =
    for
      p5 <- ProjectionFunctions.percentile(signals, 0.05d)
      p50 <- ProjectionFunctions.percentile(signals, 0.5d)
      p95 <- ProjectionFunctions.percentile(signals, 0.95d)
    yield BaselineProjection(
      ProjectionFunctions.stableId("baseline", Vector(bssid, "signal_dbm")),
      bssid,
      "signal_dbm",
      p5,
      p50,
      p95,
      signals.size.toLong,
      ProjectionFunctions.stableId("baseline-run", Vector(bssid, "signal_dbm"))
    )

  def similarity(candidate: SimilarityCandidate): Either[String, SimilarityProjection] =
    if !candidate.cosineDistance.isFinite || candidate.cosineDistance < 0.0d || candidate.cosineDistance > 2.0d then
      Left(s"invalid cosine distance ${candidate.cosineDistance}")
    else
      val ordered = Vector(candidate.leftDocumentId, candidate.rightDocumentId).sorted
      val pairId = ProjectionFunctions.stableId(
        "similarity-pair",
        Vector(candidate.pairKind, candidate.embeddingModel) ++ ordered
      )
      Right(SimilarityProjection(
        pairId,
        candidate,
        (1.0d - candidate.cosineDistance).max(-1.0d).min(1.0d),
        Json.obj(
          "distance_function" -> Json.fromString("VEC_COSINE_DISTANCE"),
          "content_pair" -> ordered.asJson
        ).noSpaces,
        ProjectionFunctions.stableId("similarity-run", Vector(pairId))
      ))

  def identityClusters(
      edges: Iterable[(String, String, Double)]
  ): Vector[IdentityClusterProjection] =
    val normalized = edges.iterator
      .filter((left, right, confidence) =>
        left.nonEmpty && right.nonEmpty && left != right && confidence.isFinite && confidence >= 0.0d
      )
      .toVector
    val confidenceByPair = normalized.groupMapReduce { case (left, right, _) =>
      val pair = Vector(left, right).sorted
      (pair.head, pair.last)
    } { case (_, _, confidence) => confidence.max(0.0d).min(1.0d)
    }(math.max)
    ProjectionFunctions.connectedComponents(normalized.map((left, right, _) => left -> right)).map { members =>
      val memberSet = members.toSet
      val confidences = confidenceByPair.collect {
        case ((left, right), confidence) if memberSet.contains(left) && memberSet.contains(right) => confidence
      }.toVector
      val clusterId = ProjectionFunctions.stableId("identity-cluster", members)
      IdentityClusterProjection(
        clusterId,
        members,
        confidences.minOption.getOrElse(1.0d),
        ProjectionFunctions.stableId("identity-cluster-run", Vector(clusterId))
      )
    }

  def dnsThreat(candidate: DnsThreatCandidate): DnsThreatProjection =
    val count = candidate.blockedCount.max(1L)
    val averageBytes = candidate.attemptedBytes.toDouble / count.toDouble
    val recencyWeight = 1.0d + (2.0d * candidate.recentCount.max(0L).toDouble / count.toDouble)
    val score = count.toDouble * (averageBytes + 1.0d) * recencyWeight
    val severity =
      if score >= 1000.0d then "critical"
      else if score >= 500.0d then "high"
      else "medium"
    val signalId = ProjectionFunctions.stableId("dns-threat", Vector(candidate.host))
    DnsThreatProjection(
      s"dns:${candidate.host}",
      signalId,
      score,
      severity,
      Json.obj(
        "blocked_count_7d" -> Json.fromLong(candidate.blockedCount),
        "attempted_bytes_7d" -> Json.fromLong(candidate.attemptedBytes),
        "recent_count_24h" -> Json.fromLong(candidate.recentCount),
        "legacy_recency_weight" -> Json.fromDoubleOrNull(recencyWeight)
      ).noSpaces,
      candidate.lastSeen,
      ProjectionFunctions.stableId("dns-threat-run", Vector(candidate.host))
    )

  def apRisk(
      bssid: String,
      deauthScore: Double,
      signalAnomalyScore: Double,
      typosquatScore: Double,
      vendorMismatchScore: Double,
      embeddingOutlierScore: Double
  ): Either[String, ApRiskProjection] =
    val inputs = Vector(
      deauthScore,
      signalAnomalyScore,
      typosquatScore,
      vendorMismatchScore,
      embeddingOutlierScore
    )
    if !inputs.forall(_.isFinite) then Left("AP risk inputs must be finite")
    else
      val bounded = inputs.map(_.max(0.0d).min(1.0d))
      val signalRisk = (bounded(0) * 0.25d) + (bounded(1) * 0.20d)
      val identityRisk = (bounded(2) * 0.20d) + (bounded(3) * 0.15d)
      val behaviorRisk = bounded(4) * 0.20d
      val composite = (signalRisk + identityRisk + behaviorRisk).max(0.0d).min(1.0d)
      Right(ApRiskProjection(
        bssid,
        composite,
        signalRisk,
        identityRisk,
        behaviorRisk,
        Json.obj(
          "deauth_score" -> Json.fromDoubleOrNull(deauthScore),
          "signal_anomaly_score" -> Json.fromDoubleOrNull(signalAnomalyScore),
          "typosquat_score" -> Json.fromDoubleOrNull(typosquatScore),
          "vendor_mismatch_score" -> Json.fromDoubleOrNull(vendorMismatchScore),
          "embedding_outlier_score" -> Json.fromDoubleOrNull(embeddingOutlierScore),
          "weights" -> Json.arr(
            Json.fromDoubleOrNull(0.25d),
            Json.fromDoubleOrNull(0.20d),
            Json.fromDoubleOrNull(0.20d),
            Json.fromDoubleOrNull(0.15d),
            Json.fromDoubleOrNull(0.20d)
          )
        ).noSpaces,
        ProjectionFunctions.stableId("ap-risk-run", Vector(bssid))
      ))

  private def windowStart(value: Instant): Instant =
    Instant.ofEpochSecond(Math.floorDiv(value.getEpochSecond, WindowSeconds) * WindowSeconds)

  private def counts(values: Iterable[String]): Map[String, Long] =
    values.iterator.filter(_.nonEmpty).toList.groupMapReduce(identity)(_ => 1L)(_ + _)
