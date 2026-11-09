package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.{Duration, Instant}
import munit.FunSuite

class ProxyFollowOnIntegrationSuite extends FunSuite:
  private val start = Instant.parse("2026-11-01T12:00:00Z")
  private val policy = ProxyBehavior.Policy("follow-on", "reviewed-fixture-v1",
    Duration.ofSeconds(30), 3600, start, start.plusSeconds(7200), true, true,
    start.plusSeconds(7200), start, 100, 168)

  private def event(id: Int, seconds: Long, host: String = "api.example.com"): ProxyBehavior.Source =
    ProxyBehavior.Source(Json.obj(
      "event_id" -> f"00000000-0000-4000-8000-$id%012d".asJson,
      "time" -> start.plusSeconds(seconds).toString.asJson,
      "type" -> "tunnel_open".asJson,
      "blocked" -> false.asJson,
      "host" -> host.asJson, "authority_raw" -> s"$host:443".asJson,
      "authority_source" -> "connect_authority".asJson,
      "evidence_version" -> ProxyEvidence.Version.asJson,
      "selected_ip" -> (if seconds < 3600 then "192.0.2.10" else "192.0.2.11").asJson,
      "resolved_ips" -> List("192.0.2.10", "192.0.2.11").asJson,
      "peer_ip" -> "10.0.0.99".asJson,
      "tls_evidence" -> Json.obj("source" -> "client_hello".asJson,
        "parser_version" -> "proxy-clienthello-v2".asJson,
        "parse_status" -> "parsed".asJson,
        "semantics" -> "offered_not_negotiated".asJson,
        "ja3_lite" -> (if seconds < 3600 then "771,4865,0-16,29,0" else "771,4865-4866,0-16,29,0").asJson,
        "offered_alpn" -> "h2".asJson),
      "device_id" -> "00000000-0000-4000-8000-000000000100".asJson,
      "identity_source" -> "registered".asJson,
      "attribution" -> Json.obj("version" -> "proxy-attribution-v1".asJson,
        "method" -> "peer_claim".asJson, "peer_reference" -> ("a" * 64).asJson,
        "reference_scope" -> "process_wireguard_peer".asJson,
        "reference_rotation" -> "process_restart_or_key_change".asJson,
        "reference_expiry" -> "process_lifetime".asJson,
        "ownership" -> "registration_claim_only".asJson,
        "valid_from" -> start.toString.asJson,
        "valid_until" -> start.plusSeconds(7200).toString.asJson,
        "mapping_revision" -> start.toString.asJson)), id.toLong)

  private val samples = (0 until 12).map(i => event(i + 1, i * 600L)).toList
  private def build(rows: List[ProxyBehavior.Source], p: ProxyBehavior.Policy = policy): ProxyBehavior.Result =
    ProxyBehavior.build(p, rows).toOption.get

  test("follow-on evidence publishes through host and registration windows with stable replay"):
    val result = build(samples)
    assertEquals(build(samples.reverse ++ samples), result)
    (result.windows ++ result.deviceWindows).foreach { projection =>
      val c = projection.evidence.hcursor
      assertEquals(c.get[String]("calculation_version"), Right(ProxyBehavior.CalculationVersion))
      List("clienthello_drift", "destination_infrastructure", "domain_lexical").foreach { field =>
        assert(c.downField(field).focus.exists(_.isObject), field)
      }
      assert(!projection.evidence.noSpaces.contains("10.0.0.99"))
      assert(!projection.semanticText.contains("192.0.2."))
      assert(!projection.semanticText.contains("4865"))
    }
    val second = result.deviceWindows.find(_.observedAt == start.plusSeconds(3600)).get
    assertEquals(second.evidence.hcursor.downField("intelligence_context").get[String]("history_end"),
      Right(start.plusSeconds(3600).toString))
    assertEquals(second.evidence.hcursor.downField("intelligence_context").get[Boolean]("history_complete_within_bounds"), Right(true))
    assertEquals(second.evidence.hcursor.downField("clienthello_drift").get[Boolean]("candidate"), Right(true))
    val infrastructure = second.evidence.hcursor.downField("destination_infrastructure")
    assertEquals(infrastructure.downField("current").get[Int]("selected_ip_observations"), Right(6))
    assertEquals(infrastructure.downField("selected_ip_change").get[Boolean]("observed_change"), Right(true))
    assertEquals(infrastructure.downField("resolved_ip_change").get[Boolean]("observed_change"), Right(false))

  test("subdomain churn uses domain-wide rows within the same validated registration epoch"):
    val extra = event(13, 3700, "new.example.com")
    val otherEpoch = event(14, 3800, "outside.example.com").copy(row =
      event(14, 3800, "outside.example.com").row.deepMerge(Json.obj(
        "attribution" -> Json.obj("peer_reference" -> ("b" * 64).asJson))))
    val result = build(samples ++ List(extra, otherEpoch))
    val scoped = result.deviceWindows.find(p => p.observedAt == start.plusSeconds(3600) &&
      p.evidence.hcursor.get[String]("host_normalized").contains("api.example.com")).get
    val churn = scoped.evidence.hcursor.downField("domain_lexical").downField("subdomain_churn").downArray
    assertEquals(churn.get[Int]("current_subdomain_count"), Right(2))
    assertEquals(churn.get[List[String]]("new_to_retained_history_subdomains"), Right(List("new.example.com")))

  test("future and conflicting rows cannot leak into the current baseline"):
    val first = build(samples.take(6)).deviceWindows.head
    val withLater = build(samples).deviceWindows.find(_.key == first.key).get
    List("clienthello_drift", "destination_infrastructure", "domain_lexical").foreach { field =>
      assertEquals(first.evidence.hcursor.downField(field).focus, withLater.evidence.hcursor.downField(field).focus)
    }
    val conflict = samples.head.copy(row = samples.head.row.deepMerge(Json.obj("selected_ip" -> "203.0.113.99".asJson)))
    val conflicting = build(samples ++ List(conflict))
    assert(!conflicting.windows.map(_.evidence.noSpaces).mkString.contains("203.0.113.99"))

  test("history coverage and registration rotation preserve explicit uncertainty"):
    val censored = build(samples, policy.copy(coverageStart = start.plusSeconds(3600)))
    assert(censored.deviceWindows.forall(_.evidence.hcursor.downField("intelligence_context")
      .get[Boolean]("history_complete_within_bounds").contains(false)))
    val epoch = start.plusSeconds(3600)
    val rotated = samples.drop(6).map(source => source.copy(row = source.row.deepMerge(Json.obj(
      "attribution" -> Json.obj("peer_reference" -> ("b" * 64).asJson,
        "valid_from" -> epoch.toString.asJson, "mapping_revision" -> epoch.toString.asJson)))))
    val result = build(samples.take(6) ++ rotated)
    val current = result.deviceWindows.find(p => p.observedAt == epoch &&
      p.evidence.hcursor.get[String]("valid_from").contains(epoch.toString)).get
    assertEquals(current.evidence.hcursor.downField("intelligence_context").get[String]("history_start"), Right(epoch.toString))
    assertEquals(current.evidence.hcursor.downField("intelligence_context").get[String]("history_end"), Right(epoch.toString))
