package com.sslproxy.coordinator.processor

import io.circe.parser.parse
import munit.FunSuite

import java.sql.Timestamp
import java.time.Instant

class ProxyIntelligenceEvidenceSuite extends FunSuite:
  private val observedAt = Timestamp.from(Instant.parse("2026-10-24T12:00:00Z"))

  test("legacy blocked intensity is explicitly uncalibrated observed evidence"):
    val projection = IntelligencePreparation.dnsThreat(
      DnsThreatCandidate("blocked.example", 4L, 396L, 2L, observedAt)
    )
    val evidence = parse(projection.evidenceJson).toOption.get.hcursor
    assertEquals(projection.score, 800.0d)
    assertEquals(projection.severity, "high")
    assertEquals(evidence.get[String]("score_version"), Right("legacy-blocked-intensity-v1"))
    assertEquals(evidence.get[String]("calibration_status"), Right("uncalibrated"))
    assertEquals(evidence.get[String]("count_semantics"), Right("retained_observations_not_attempts"))
    assertEquals(evidence.get[String]("byte_semantics"), Right("legacy_scalar_sum_not_validated_transfer"))
    assertEquals(evidence.get[Boolean]("confirmed_threat"), Right(false))

  test("invalid legacy scalar counts cannot produce negative intensity or recency above one"):
    val negative = IntelligencePreparation.dnsThreat(
      DnsThreatCandidate("blocked.example", 4L, -500L, 100L, observedAt)
    )
    assertEquals(negative.score, 12.0d)
    val huge = IntelligencePreparation.dnsThreat(
      DnsThreatCandidate("blocked.example", Long.MaxValue, Long.MaxValue, Long.MaxValue, observedAt)
    )
    assert(huge.score.isFinite)
    assert(huge.score >= 0.0d)
