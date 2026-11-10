package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.{Duration, Instant}
import munit.FunSuite

class ProxyBehaviorScopeSuite extends FunSuite:
  private val start = Instant.parse("2026-11-01T12:00:00Z")
  private val policy = ProxyBehavior.Policy(
    "deployment-a",
    "reviewed-v1",
    Duration.ofSeconds(30),
    3600,
    start.minusSeconds(3600),
    start.plusSeconds(10800),
    true,
    true,
    start.plusSeconds(7200),
    start,
    100,
    168
  )
  private val device = "00000000-0000-4000-8000-000000000100"
  private val peer = "a" * 64
  private def event(
      id: Int,
      seconds: Long = 30,
      host: String = "example.com:443",
      kind: String = "http_proxied"
  ): ProxyBehavior.Source =
    ProxyBehavior.Source(
      Json.obj(
        "event_id" -> f"00000000-0000-4000-8000-$id%012d".asJson,
        "type" -> kind.asJson,
        "time" -> start.plusSeconds(seconds).toString.asJson,
        "host" -> host.asJson,
        "authority_raw" -> host.asJson,
        "authority_source" -> "http_authority".asJson,
        "evidence_version" -> ProxyEvidence.Version.asJson,
        "blocked" -> Set("block", "http_blocked", "session.blocked")(
          kind
        ).asJson
      ),
      id.toLong
    )
  private def claim(
      source: ProxyBehavior.Source,
      reference: String = peer,
      registration: String = device,
      from: Instant = start,
      until: Instant = start.plusSeconds(7200),
      method: String = "peer_claim"
  ): ProxyBehavior.Source =
    source.copy(row =
      source.row.deepMerge(
        Json.obj(
          "device_id" -> registration.asJson,
          "identity_source" -> "registered".asJson,
          "attribution" -> Json.obj(
            "version" -> "proxy-attribution-v1".asJson,
            "method" -> method.asJson,
            "valid_from" -> from.toString.asJson,
            "valid_until" -> until.toString.asJson,
            "mapping_revision" -> from.toString.asJson,
            "peer_reference" -> reference.asJson,
            "reference_scope" -> "process_wireguard_peer".asJson,
            "reference_rotation" -> "process_restart_or_key_change".asJson,
            "reference_expiry" -> "process_lifetime".asJson,
            "ownership" -> "registration_claim_only".asJson
          )
        )
      )
    )
  private def build(rows: ProxyBehavior.Source*): ProxyBehavior.Result =
    ProxyBehavior.build(policy, rows.toList).toOption.get
  private def deployment(
      result: ProxyBehavior.Result
  ): List[ProxyBehavior.Projection] =
    result.destinations.filter(
      _.evidence.hcursor.get[String]("scope_kind").contains("deployment")
    )

  test(
    "retained first observation rarity and late earlier events are scoped and replay safe"
  ):
    val late = event(3, 10)
    val a = event(1, 60)
    val b = event(2, 80, "other.example:443")
    val before = deployment(build(a, b))
      .find(
        _.evidence.hcursor
          .get[String]("endpoint_key")
          .toOption
          .get
          .contains("example.com")
      )
      .get
    val result = build(a, b, late, a, late)
    val after = deployment(result).find(_.key == before.key).get.evidence
    assertEquals(
      after.hcursor.get[String]("first_observed_at"),
      Right(start.plusSeconds(10).toString)
    )
    assertEquals(after.hcursor.get[Int]("observation_count"), Right(2))
    assertEquals(after.hcursor.get[Int]("scope_observation_count"), Right(3))
    assertEquals(after.hcursor.get[Double]("observed_fraction"), Right(2.0 / 3))
    assertEquals(after.hcursor.get[Boolean]("left_censored"), Right(true))
    assert(after.hcursor.downField("domain_registration_time").focus.get.isNull)
    assertEquals(result, build(late, b, a))
    val other = ProxyBehavior
      .build(policy.copy(deploymentScope = "deployment-b"), List(a, b, late))
      .toOption
      .get
    assert(
      result.destinations
        .map(_.key)
        .toSet
        .intersect(other.destinations.map(_.key).toSet)
        .isEmpty
    )

  test(
    "earlier retained history bounds move first observation and label collection limits"
  ):
    val rows = List(event(1, 10), event(2, 3700))
    val bounded = ProxyBehavior
      .build(policy.copy(lookbackStart = start.plusSeconds(3600)), rows)
      .toOption
      .get
    val evidence = deployment(bounded).head.evidence
    assertEquals(
      evidence.hcursor.get[String]("first_observed_at"),
      Right(start.plusSeconds(3700).toString)
    )
    assertEquals(
      evidence.hcursor.get[String]("retained_history_start"),
      Right(start.plusSeconds(3600).toString)
    )
    assertEquals(evidence.hcursor.get[Boolean]("left_censored"), Right(true))
    val incomplete = ProxyBehavior
      .build(policy.copy(coverageAttested = false), rows)
      .toOption
      .get
    assertEquals(
      deployment(incomplete).head.evidence.hcursor
        .get[Boolean]("history_complete_within_bounds"),
      Right(false)
    )

  test(
    "only valid observed registration epochs create device windows; shared addresses do not"
  ):
    val registered = claim(event(1))
    val address = event(2).copy(row =
      event(2).row.deepMerge(
        Json.obj(
          "device_id" -> device.asJson,
          "identity_source" -> "registered".asJson,
          "peer_ip" -> "10.0.0.2".asJson
        )
      )
    )
    val peerOnly = claim(event(3)).copy(row =
      claim(event(3)).row.deepMerge(Json.obj("device_id" -> Json.Null))
    )
    val unknownHost = event(4, host = "<invalid>")
    val result = build(registered, address, peerOnly, unknownHost)
    assertEquals(result.deviceWindows.size, 2)
    assertEquals(
      result.deviceWindows.head.evidence.hcursor.get[Int]("observed_count"),
      Right(1)
    )
    val coverage = result.attributionWindows.head.evidence.hcursor
    assertEquals(coverage.get[Int]("denominator"), Right(4))
    assertEquals(coverage.get[Int]("supported_registration"), Right(1))
    assertEquals(coverage.get[Int]("scoped_unattributed_peer"), Right(1))
    assertEquals(coverage.get[Int]("unknown"), Right(2))
    assertEquals(coverage.get[Double]("unavailable_rate"), Right(0.75))
    assertEquals(
      coverage.downField("method_counts").get[Int]("peer_claim"),
      Right(2)
    )
    val publicText = result.deviceWindows.map(_.evidence.noSpaces).mkString
    assert(!publicText.contains(device))
    assert(!publicText.contains("10.0.0.2"))

  test(
    "claim interval is half open and unsupported methods or revisions cannot establish ownership"
  ):
    val expired = claim(event(1, 3600), until = start.plusSeconds(3600))
    val future = claim(event(2), from = start.plusSeconds(60))
    val unsupported = claim(event(3), method = "client_address")
    val revision = claim(event(4)).copy(row =
      claim(event(4)).row.deepMerge(
        Json.obj(
          "attribution" -> Json.obj(
            "mapping_revision" -> start.minusSeconds(1).toString.asJson
          )
        )
      )
    )
    val result = build(expired, future, unsupported, revision)
    assertEquals(result.deviceWindows, Nil)
    assertEquals(
      result.destinations
        .map(_.evidence.hcursor.get[String]("scope_kind").toOption.get)
        .toSet,
      Set("deployment", "peer")
    )

  test(
    "overlapping claims for a shared peer invalidate registration without fabricating a device"
  ):
    val result = build(
      claim(event(1)),
      claim(event(2), registration = "00000000-0000-4000-8000-000000000101")
    )
    assertEquals(result.deviceWindows, Nil)
    assertEquals(
      result.attributionWindows.head.evidence.hcursor
        .downField("validity_counts")
        .get[Int]("mapping_conflict"),
      Right(2)
    )
    assertEquals(
      result.attributionWindows.head.evidence.hcursor
        .get[Int]("scoped_unattributed_peer"),
      Right(2)
    )

  test("key rotation and changed validity create separate registration epochs"):
    val rotated = claim(
      event(2, 3700),
      reference = "b" * 64,
      from = start.plusSeconds(3600)
    )
    val result =
      build(claim(event(1), until = start.plusSeconds(3600)), rotated)
    val registrations = result.destinations.filter(
      _.evidence.hcursor.get[String]("scope_kind").contains("registration")
    )
    assertEquals(registrations.size, 2)
    assertEquals(
      registrations
        .map(_.evidence.hcursor.get[String]("scope_key").toOption.get)
        .distinct
        .size,
      2
    )
    assertEquals(result.deviceWindows.size, 2)
    assertEquals(
      result.attributionWindows.last.evidence.hcursor
        .get[Int]("mapping_change_count"),
      Right(1)
    )
    assert(
      result.deviceWindows.forall(
        _.evidence.hcursor
          .get[String]("ownership")
          .contains("registration_claim_only")
      )
    )

  test("window budgeting counts only overlapping registration epochs"):
    val rows = (0 until 5).map { hour =>
      claim(
        event(hour + 1, hour * 3600L + 30),
        from = start.plusSeconds(hour * 3600L),
        until = start.plusSeconds((hour + 1) * 3600L)
      )
    }.toList
    val bounded = policy.copy(
      asOf = start.plusSeconds(86400),
      coverageEnd = start.plusSeconds(90000),
      maxWindows = 60
    )
    val result = ProxyBehavior.build(bounded, rows).toOption.get
    assertEquals(result.windows.size, 24)
    assertEquals(result.attributionWindows.size, 24)
    assertEquals(result.deviceWindows.size, 5)
    assertEquals(result.destinations.size, 7)
    assertEquals(
      ProxyBehavior.build(bounded.copy(maxWindows = 59), rows),
      Left("proxy behavior window row bound exceeded")
    )

  test(
    "policy friction is bounded temporal evidence with IDs and explicit unknown retries"
  ):
    val rows = (1 to 40)
      .map(id =>
        event(
          id,
          id.toLong,
          kind =
            if id <= 2 then "http_blocked"
            else if id <= 4 then "http_error"
            else "http_proxied"
        )
      )
      .toList
    val p = ProxyBehavior
      .build(policy, rows)
      .toOption
      .get
      .windows
      .head
      .evidence
      .hcursor
      .downField("policy_friction")
    assertEquals(p.get[Int]("blocked_observations"), Right(2))
    assertEquals(p.get[Int]("error_observations"), Right(2))
    assertEquals(p.get[Int]("related_allowed_observations"), Right(36))
    assertEquals(p.get[List[Json]]("timeline").toOption.get.size, 32)
    assertEquals(p.get[Int]("timeline_omitted"), Right(8))
    assertEquals(p.get[String]("causality"), Right("unavailable"))
    assert(p.downField("retry_count").focus.get.isNull)
    assertEquals(
      p.get[String]("window_end"),
      Right(start.plusSeconds(3600).toString)
    )

  test(
    "conflicting source IDs reconcile coverage exclusions and empty windows have unavailable rates"
  ):
    val first = claim(event(1))
    val conflicting = first.copy(row =
      first.row.deepMerge(Json.obj("reason" -> "different".asJson))
    )
    val result = build(first, conflicting)
    val coverage = result.attributionWindows.head.evidence.hcursor
    assertEquals(coverage.get[Int]("source_observation_count"), Right(1))
    assertEquals(
      coverage.get[Int]("excluded_conflicting_observations"),
      Right(1)
    )
    assertEquals(coverage.get[Int]("denominator"), Right(0))
    assert(coverage.downField("registration_rate").focus.get.isNull)
    assertEquals(result.deviceWindows, Nil)

  test(
    "excluded conflicting claims cannot contaminate valid registrations or mapping changes"
  ):
    val valid = claim(event(1))
    val invalid =
      claim(event(2), registration = "00000000-0000-4000-8000-000000000101")
    val conflict = invalid.copy(row =
      invalid.row.deepMerge(Json.obj("reason" -> "different".asJson))
    )
    val result = build(valid, invalid, conflict)
    assertEquals(result.deviceWindows.size, 2)
    assertEquals(
      result.attributionWindows.head.evidence.hcursor
        .get[Int]("supported_registration"),
      Right(1)
    )
    assertEquals(
      result.attributionWindows.head.evidence.hcursor
        .get[Int]("mapping_change_count"),
      Right(0)
    )

  test(
    "device windows stop at epoch expiry and label partial interval coverage"
  ):
    val result = build(
      claim(
        event(1),
        from = start.plusSeconds(10),
        until = start.plusSeconds(1800)
      )
    )
    assertEquals(result.deviceWindows.size, 1)
    assertEquals(
      result.deviceWindows.head.disposition,
      "attribution_interval_limited"
    )
    assertEquals(
      result.deviceWindows.head.evidence.hcursor
        .downField("polling")
        .get[Boolean]("collection_censored"),
      Right(true)
    )

  test("published windows omit transient threat feed state and signals"):
    val result = build(claim(event(1)))
    assert(result.windows.nonEmpty)
    assert(result.deviceWindows.nonEmpty)
    (result.windows ++ result.deviceWindows).foreach { window =>
      val report = window.evidence.hcursor.downField("threat_feed")
      assert(report.downField("state").focus.isEmpty)
      assert(report.downField("signals").focus.isEmpty)
      assertEquals(report.get[List[String]]("signal_ids"), Right(Nil))
      assertEquals(
        report.get[String]("signal_read_surface"),
        Right("atheros_search.proxy_signal")
      )
    }

  test(
    "friction timeline retains late blocks even after many allowed observations"
  ):
    val rows = (1 to 38).map(id => event(id, id.toLong)).toList ++ List(
      event(39, 39, kind = "http_blocked"),
      event(40, 40, kind = "http_blocked")
    )
    val p = ProxyBehavior
      .build(policy, rows)
      .toOption
      .get
      .windows
      .head
      .evidence
      .hcursor
      .downField("policy_friction")
    assertEquals(
      p.get[List[Json]]("timeline")
        .toOption
        .get
        .count(_.hcursor.get[String]("outcome").contains("blocked")),
      2
    )

  test(
    "friction timeline fills spare slots while reserving room for allowed observations"
  ):
    for (frictionCount, allowedCount, expectedFriction, expectedAllowed) <-
        List(
          (25, 0, 25, 0),
          (40, 0, 32, 0),
          (40, 3, 29, 3),
          (40, 8, 24, 8),
          (40, 20, 24, 8),
          (4, 40, 4, 28)
        )
    do
      val rows = (1 to frictionCount)
        .map(id => event(id, id.toLong, kind = "http_blocked").row)
        .toList ++
        (1 to allowedCount)
          .map(id => event(frictionCount + id, frictionCount + id.toLong).row)
          .toList
      val evidence = ProxyBehaviorScopes
        .friction(rows.reverse, start, start.plusSeconds(3600))
        .hcursor
      val timeline = evidence.get[List[Json]]("timeline").toOption.get
      assertEquals(
        timeline.count(_.hcursor.get[String]("outcome").contains("blocked")),
        expectedFriction
      )
      assertEquals(
        timeline.count(_.hcursor.get[String]("outcome").contains("allowed")),
        expectedAllowed
      )
      assertEquals(
        evidence.get[Int]("timeline_omitted"),
        Right(rows.size - timeline.size)
      )
      assertEquals(
        timeline.flatMap(_.hcursor.get[String]("time").toOption),
        timeline.flatMap(_.hcursor.get[String]("time").toOption).sorted
      )

  test("cohorts and polling share the source conflict-attestation requirement"):
    val settings = Json.obj(
      "version" -> "cohorts-fixture-v1".asJson,
      "calibration_reference" -> "synthetic-controls".asJson,
      "b11_acceptance_reference" -> "exact-reference".asJson,
      "minimum_destinations" -> 1.asJson,
      "minimum_observations" -> 1.asJson,
      "minimum_similarity" -> BigDecimal("0.5").asJson,
      "max_pairs" -> 256.asJson
    )
    val configured = policy.copy(intelligencePolicy =
      Json.obj(
        "version" -> ProxyIntelligencePolicy.Version.asJson,
        "cohorts" -> settings
      )
    )
    val rows = List(
      claim(event(1)),
      claim(
        event(2),
        reference = "b" * 64,
        registration = "00000000-0000-4000-8000-000000000101"
      )
    )
    def windows(attested: Boolean) = ProxyBehavior
      .build(configured.copy(conflictsAttested = attested), rows)
      .toOption
      .get
      .deviceWindows
    assert(
      windows(true).exists(
        _.evidence.hcursor
          .downField("behavioral_cohorts")
          .get[Int]("match_count")
          .exists(_ > 0)
      )
    )
    val unattested = windows(false)
    assert(unattested.nonEmpty)
    assert(
      unattested.forall(
        _.evidence.hcursor
          .downField("behavioral_cohorts")
          .get[Int]("match_count")
          .contains(0)
      )
    )
    assert(
      unattested.forall(
        _.evidence.hcursor
          .downField("polling")
          .get[Boolean]("collection_censored")
          .contains(true)
      )
    )
