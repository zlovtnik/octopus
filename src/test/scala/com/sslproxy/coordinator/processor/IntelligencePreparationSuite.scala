package com.sslproxy.coordinator.processor

import munit.FunSuite

import java.sql.Timestamp
import java.time.Instant

class IntelligencePreparationSuite extends FunSuite:
  test("behavior preparation is stable and partitions frames into UTC hour windows"):
    val frames = List(
      frame("2026-08-03T10:45:00Z", Some(-50), retry = true, protectedFrame = true),
      frame("2026-08-03T10:05:00Z", Some(-70), retry = false, protectedFrame = false)
    )

    val first = IntelligencePreparation.behavior(frames)
    val reordered = IntelligencePreparation.behavior(frames.reverse)
    assertEquals(first, reordered)
    assertEquals(first.size, 1)
    assertEquals(first.headOption.map(_.eventCount), Some(2L))
    assertEquals(first.headOption.flatMap(_.signalAverage), Some(-60.0d))
    assertEquals(first.headOption.map(_.retryCount), Some(1L))

  test("timing preparation delegates percentile and jitter laws to the pure projection core"):
    val frames = List(
      frame("2026-08-03T10:00:00Z", None, tsft = Some(10L), wall = Some(2L)),
      frame("2026-08-03T10:01:00Z", None, tsft = Some(30L), wall = Some(6L))
    )

    val profile = IntelligencePreparation.timing(frames).headOption.getOrElse(fail("expected profile"))
    assertEquals(profile.tsftP50, Some(20.0d))
    assertEquals(profile.tsftJitter, Some(10.0d))
    assertEquals(profile.wallP50, Some(4.0d))

  test("sequence preparation is ordered by observation time and uses stable tokens"):
    val later = frame("2026-08-03T10:01:00Z", None, subtype = Some("deauth"), session = Some("s1"))
    val earlier = frame("2026-08-03T10:00:00Z", None, subtype = Some("beacon"), session = Some("s1"))
    val sequence = IntelligencePreparation.sequences(List(later, earlier)).headOption.getOrElse(fail("expected sequence"))

    assertEquals(sequence.tokens, Vector(FrameToken.Beacon, FrameToken.Deauthentication))

  test("baseline preparation rejects empty samples and characterizes percentiles"):
    assertEquals(IntelligencePreparation.baseline("aa:bb:cc:dd:ee:ff", Vector.empty), None)
    val baseline = IntelligencePreparation.baseline(
      "aa:bb:cc:dd:ee:ff",
      Vector(-90.0d, -70.0d, -50.0d)
    ).getOrElse(fail("expected baseline"))
    assertEquals(baseline.p50, -70.0d)
    assertEquals(baseline.sampleCount, 3L)

  test("similarity preparation orders identifiers and rejects invalid distances"):
    val candidate = SimilarityCandidate(
      "event_event",
      "event",
      "model",
      "b-document",
      "a-document",
      "wireless_frames",
      "b",
      None,
      None,
      None,
      None,
      "wireless_frames",
      "a",
      None,
      None,
      None,
      None,
      0.05d
    )
    val projection = IntelligencePreparation.similarity(candidate).toOption
      .getOrElse(fail("expected similarity"))
    assertEquals(projection.cosineSimilarity, 0.95d)
    assertEquals(
      projection.pairId,
      IntelligencePreparation.similarity(candidate.copy(
        leftDocumentId = "a-document",
        rightDocumentId = "b-document"
      )).toOption.map(_.pairId).getOrElse(fail("expected reordered similarity"))
    )
    assert(IntelligencePreparation.similarity(candidate.copy(cosineDistance = Double.NaN)).isLeft)

  test("approved identity edges produce deterministic transitive clusters"):
    val edges = Vector(
      ("bb:bb:bb:bb:bb:bb", "cc:cc:cc:cc:cc:cc", 0.91d),
      ("aa:aa:aa:aa:aa:aa", "bb:bb:bb:bb:bb:bb", 0.95d),
      ("bb:bb:bb:bb:bb:bb", "aa:aa:aa:aa:aa:aa", 0.40d)
    )
    val cluster = IntelligencePreparation.identityClusters(edges).headOption
      .getOrElse(fail("expected cluster"))
    assertEquals(cluster.members, Vector(
      "aa:aa:aa:aa:aa:aa",
      "bb:bb:bb:bb:bb:bb",
      "cc:cc:cc:cc:cc:cc"
    ))
    assertEquals(cluster.confidence, 0.91d)
    assertEquals(
      IntelligencePreparation.identityClusters(edges.reverse).headOption.map(_.clusterId),
      Some(cluster.clusterId)
    )

  test("DNS threat scoring preserves the characterized seven-day recency formula"):
    val projection = IntelligencePreparation.dnsThreat(DnsThreatCandidate(
      "blocked.example",
      blockedCount = 4L,
      attemptedBytes = 396L,
      recentCount = 2L,
      Timestamp.from(Instant.parse("2026-08-03T10:00:00Z"))
    ))
    assertEquals(projection.score, 800.0d)
    assertEquals(projection.severity, "high")

  test("DNS threat scoring assigns the low band below the medium cutoff"):
    val projection = IntelligencePreparation.dnsThreat(DnsThreatCandidate(
      "quiet.example",
      blockedCount = 0L,
      attemptedBytes = 0L,
      recentCount = 0L,
      Timestamp.from(Instant.parse("2026-08-03T10:00:00Z"))
    ))

    assertEquals(projection.score, 0.0d)
    assertEquals(projection.severity, "low")

  test("AP risk decomposition rejects non-finite values and clamps scores"):
    val projection = IntelligencePreparation.apRisk(
      "11:22:33:44:55:66",
      deauthScore = 4.0d,
      signalAnomalyScore = 3.0d,
      typosquatScore = 2.0d,
      vendorMismatchScore = 1.0d,
      embeddingOutlierScore = 0.5d
    )
      .toOption
      .getOrElse(fail("expected bounded AP risk"))
    assertEqualsDouble(projection.signalRisk, 0.45d, 0.0000001d)
    assertEqualsDouble(projection.identityRisk, 0.35d, 0.0000001d)
    assertEqualsDouble(projection.behaviorRisk, 0.1d, 0.0000001d)
    assertEqualsDouble(projection.composite, 0.9d, 0.0000001d)
    assert(
      IntelligencePreparation
        .apRisk(
          "11:22:33:44:55:66",
          Double.NaN,
          0.0d,
          0.0d,
          0.0d,
          0.0d
        )
        .isLeft)

  private def frame(
      observedAt: String,
      signal: Option[Int],
      retry: Boolean = false,
      protectedFrame: Boolean = false,
      subtype: Option[String] = Some("data"),
      tsft: Option[Long] = None,
      wall: Option[Long] = None,
      session: Option[String] = None
  ): ProjectionFrame =
    ProjectionFrame(
      dedupeKey = observedAt,
      sourceMac = "aa:bb:cc:dd:ee:ff",
      locationId = Some("lab"),
      sensorId = Some("sensor-1"),
      observedAt = Timestamp.from(Instant.parse(observedAt)),
      frameType = Some("data"),
      frameSubtype = subtype,
      signalDbm = signal,
      retry = retry,
      protectedFrame = protectedFrame,
      bssid = Some("11:22:33:44:55:66"),
      appProtocol = Some("dns"),
      adjacentMacHint = None,
      tsftDeltaUs = tsft,
      wallClockDeltaMs = wall,
      sessionKey = session
    )
