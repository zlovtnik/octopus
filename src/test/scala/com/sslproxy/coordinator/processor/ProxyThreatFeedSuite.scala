package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import munit.FunSuite

class ProxyThreatFeedSuite extends FunSuite:
  private val start = Instant.parse("2026-11-01T12:00:00Z")
  private val end = start.plusSeconds(3600)
  private val expires = end.plusSeconds(7200)
  private val window = ProxyBehavior.Projection("fixture-window", Json.obj(
    "deployment_scope" -> "fixture-deployment".asJson,
    "scope_kind" -> "registration".asJson, "scope_key" -> ("a" * 64).asJson,
    "window_start" -> start.toString.asJson, "window_end" -> end.toString.asJson,
    "policy_version" -> "behavior-fixture-v1".asJson,
    "calculation_version" -> ProxyBehavior.CalculationVersion.asJson,
    "normalization_version" -> ProxyAuthority.Version.asJson),
    "no feed data in embedding text", "fixture-fingerprint", "available", 12L, start)

  private def indicator(id: String, kind: String = "exact_host", value: String = "api.example.com"): Json =
    Json.obj("indicator_id" -> id.asJson, "kind" -> kind.asJson, "value" -> value.asJson)
  private def snapshot(values: List[Json], version: String = "snapshot-1", feed: String = "synthetic-fixture"): Json =
    Json.obj("feed_id" -> feed.asJson, "snapshot_version" -> version.asJson,
      "provenance" -> Json.obj("publisher" -> "synthetic test fixture".asJson,
        "source_uri" -> "urn:synthetic:proxy-feed".asJson),
      "license" -> "LicenseRef-SyntheticFixtureOnly".asJson,
      "generated_at" -> start.minusSeconds(120).toString.asJson,
      "retrieved_at" -> start.minusSeconds(60).toString.asJson,
      "expires_at" -> expires.toString.asJson,
      "sha256" -> ProxyThreatFeed.snapshotChecksum(values).asJson,
      "indicators" -> values.asJson)
  private def policy(feeds: List[Json], alerts: Boolean = true): Json =
    Json.obj("version" -> "feed-policy-fixture-v1".asJson,
      "calibration_reference" -> "synthetic fixture; not deployment calibration".asJson,
      "b12_acceptance_reference" -> "synthetic local acceptance only".asJson,
      "alerts_enabled" -> alerts.asJson, "snapshots" -> feeds.asJson)
  private def row(id: Int, kind: String = "tunnel_open", seconds: Long = 30,
      host: String = "api.example.com", selected: String = "192.0.2.10"): Json =
    Json.obj("event_id" -> f"00000000-0000-4000-8000-$id%012d".asJson,
      "time" -> start.plusSeconds(seconds).toString.asJson,
      "type" -> kind.asJson, "blocked" -> false.asJson,
      "evidence_version" -> ProxyEvidence.Version.asJson,
      "host" -> host.asJson, "authority_raw" -> s"$host:443".asJson,
      "authority_source" -> "connect_authority".asJson,
      "selected_ip" -> selected.asJson, "peer_ip" -> "198.51.100.99".asJson)
  private val exact = indicator("exact")
  private def result(rows: List[Json] = List(row(1)), feed: Json = snapshot(List(exact)),
      alerts: Boolean = true, asOf: Instant = end, projection: ProxyBehavior.Projection = window): Json =
    ProxyThreatFeed.evidence(rows, projection, policy(List(feed), alerts), asOf)
  private def signals(evidence: Json): List[Json] = evidence.hcursor.get[List[Json]]("signals").toOption.get

  test("exact licensed local snapshot produces versioned cited signals without peer inference"):
    val evidence = result()
    val signal = signals(evidence).head.hcursor
    assertEquals(evidence.hcursor.get[String]("disposition"), Right("correlation_evidence"))
    assertEquals(signal.get[String]("state"), Right("active"))
    assertEquals(signal.get[String]("valid_until"), Right(expires.toString))
    assertEquals(signal.get[Int]("sample_size"), Right(1))
    assertEquals(signal.get[Int]("observation_denominator"), Right(1))
    assertEquals(signal.get[Long]("source_watermark"), Right(12L))
    assertEquals(signal.get[String]("snapshot_version"), Right("snapshot-1"))
    assertEquals(signal.get[String]("feed_license"), Right("LicenseRef-SyntheticFixtureOnly"))
    assertEquals(signal.get[String]("threat_confirmation"), Right("unavailable_external_feed_assertion_only"))
    assertEquals(signal.get[String]("ownership"), Right("unavailable"))
    assert(signal.downField("alert_state").focus.isEmpty)
    assert(!evidence.noSpaces.contains("198.51.100.99"))

  test("registrable-domain matches retain shared-infrastructure caveats and cannot activate alerts"):
    val broad = indicator("broad", "registrable_domain", "example.com")
    val evidence = result(feed = snapshot(List(broad)))
    assertEquals(signals(evidence).head.hcursor.get[String]("state"), Right("candidate"))
    assertEquals(signals(evidence).head.hcursor.get[String]("specificity"), Right("registrable_domain"))
    assert(evidence.noSpaces.contains("shared_infrastructure"))
    val unrelated = result(rows = List(row(1, host = "notexample.com")), feed = snapshot(List(broad)))
    assertEquals(signals(unrelated), Nil)
    val invalidBroad = indicator("bad", "registrable_domain", "sub.example.com")
    assertEquals(signals(result(feed = snapshot(List(invalidBroad)))), Nil)

  test("selected-IP indicators use actual modern unblocked openings, never client or resolved candidates"):
    val selected = indicator("ip", "exact_selected_ip", "192.0.2.10")
    val peer = indicator("peer", "exact_selected_ip", "198.51.100.99")
    val resolved = indicator("resolver", "exact_selected_ip", "192.0.2.20")
    val source = row(1).deepMerge(Json.obj("resolved_ips" -> List("192.0.2.20").asJson))
    val found = signals(result(rows = List(source), feed = snapshot(List(selected, peer, resolved))))
    assertEquals(found.size, 1)
    assertEquals(found.head.hcursor.get[String]("indicator_id"), Right("ip"))
    val close = source.deepMerge(Json.obj("type" -> "tunnel_close".asJson))
    val blocked = source.deepMerge(Json.obj("blocked" -> true.asJson))
    val legacy = source.deepMerge(Json.obj("evidence_version" -> "legacy".asJson))
    List(close, blocked, legacy).foreach { unsupported =>
      assertEquals(signals(result(rows = List(unsupported), feed = snapshot(List(selected)))), Nil)
    }

  test("source scope, window and normalization are explicit and reject incompatible metadata"):
    val sources = List(row(1, seconds = -1), row(2, seconds = 3600), row(3), row(4, kind = "tunnel_close"))
    val found = signals(result(rows = sources))
    assertEquals(found.head.hcursor.get[Int]("sample_size"), Right(1))
    assertEquals(found.head.hcursor.get[List[String]]("source_event_ids"),
      Right(List("00000000-0000-4000-8000-000000000003")))
    val incompatible = window.copy(evidence = window.evidence.deepMerge(Json.obj("normalization_version" -> "future-v9".asJson)))
    assertEquals(signals(result(projection = incompatible)), Nil)
    val unscoped = window.copy(evidence = window.evidence.deepMerge(Json.obj("scope_kind" -> "wireless".asJson)))
    assertEquals(signals(result(projection = unscoped)), Nil)

  test("expired feed or indicator retracts signals exactly at expiry without stale-cache fallback"):
    val expired = result(asOf = expires)
    assertEquals(signals(expired), Nil)
    assertEquals(expired.hcursor.get[String]("state"), Right("retracted"))
    assertEquals(expired.hcursor.get[String]("withdrawal_rule"), Right("retract_all_absent_signal_ids"))
    assertEquals(expired.hcursor.downField("snapshot_dispositions").downArray.get[String]("disposition"), Right("expired"))
    val short = exact.deepMerge(Json.obj("expires_at" -> end.plusSeconds(30).toString.asJson))
    val limited = result(feed = snapshot(List(short)))
    assertEquals(signals(limited).head.hcursor.get[String]("valid_until"), Right(end.plusSeconds(30).toString))
    assertEquals(signals(result(feed = snapshot(List(short)), asOf = end.plusSeconds(30))), Nil)

  test("refresh preserves signal identity but changes versioned evidence and removal withdraws absent IDs"):
    val before = signals(result()).head.hcursor
    val after = signals(result(feed = snapshot(List(exact), version = "snapshot-2"))).head.hcursor
    assertEquals(before.get[String]("signal_id"), after.get[String]("signal_id"))
    assertNotEquals(before.get[String]("signal_fingerprint"), after.get[String]("signal_fingerprint"))
    val removed = ProxyThreatFeed.evidence(List(row(1)), window, policy(Nil), end)
    assertEquals(signals(removed), Nil)
    assertEquals(removed.hcursor.get[String]("state"), Right("retracted"))
    val corrected = result(rows = List(row(1, host = "corrected.example.net")))
    assertEquals(signals(corrected), Nil)

  test("unlicensed malformed future or checksum-invalid snapshots fail closed on outage"):
    val feed = snapshot(List(exact))
    val invalid = List(
      feed.deepMerge(Json.obj("license" -> "unknown".asJson)),
      feed.mapObject(_.remove("provenance")),
      feed.deepMerge(Json.obj("retrieved_at" -> end.plusSeconds(1).toString.asJson)),
      feed.deepMerge(Json.obj("generated_at" -> end.plusSeconds(1).toString.asJson)),
      feed.deepMerge(Json.obj("sha256" -> ("0" * 64).asJson)),
      feed.deepMerge(Json.obj("expires_at" -> end.plusSeconds(31L * 86400).toString.asJson)),
      feed.deepMerge(Json.obj("unrecognized" -> true.asJson)))
    invalid.foreach { bad =>
      val evidence = result(feed = bad)
      assertEquals(signals(evidence), Nil)
      assertEquals(evidence.hcursor.get[String]("state"), Right("retracted"))
    }

  test("duplicate feed identities and indicator identities cannot select an arbitrary winner"):
    val feed = snapshot(List(exact))
    val collision = policy(List(feed, snapshot(List(exact), "snapshot-2")))
    assertEquals(signals(ProxyThreatFeed.evidence(List(row(1)), window, collision, end)), Nil)
    val malformed = feed.deepMerge(Json.obj("sha256" -> ("0" * 64).asJson))
    assertEquals(signals(ProxyThreatFeed.evidence(List(row(1)), window, policy(List(feed, malformed)), end)), Nil)
    assertEquals(signals(result(feed = snapshot(List(exact, exact)))), Nil)

  test("disabled or unreviewed policies cannot activate a match, and incomplete windows stay candidate-only"):
    val disabled = ProxyThreatFeed.evidence(List(row(1)), window, Json.obj(), end)
    assertEquals(disabled.hcursor.get[String]("disposition"), Right("disabled"))
    val missingReview = policy(List(snapshot(List(exact)))).mapObject(_.remove("b12_acceptance_reference"))
    assertEquals(signals(ProxyThreatFeed.evidence(List(row(1)), window, missingReview, end)), Nil)
    assertEquals(signals(result(alerts = false)).head.hcursor.get[String]("state"), Right("candidate"))
    assertEquals(signals(result(projection = window.copy(disposition = "coverage_limited"))).head.hcursor.get[String]("state"), Right("candidate"))

  test("replay, object ordering, source order and conflicting source versions remain deterministic"):
    val other = indicator("other", "registrable_domain", "example.com")
    val sources = List(row(1), row(2, seconds = 60))
    assertEquals(result(rows = sources, feed = snapshot(List(exact, other))),
      result(rows = sources.reverse ++ sources, feed = snapshot(List(other, exact))))
    val conflict = row(1, host = "corrected.example.net")
    val excluded = signals(result(rows = List(row(1), conflict)))
    assertEquals(excluded, Nil)
    val differentScope = window.copy(key = "another-window", evidence = window.evidence.deepMerge(Json.obj("scope_key" -> ("b" * 64).asJson)))
    assertNotEquals(signals(result()).head.hcursor.get[String]("signal_id"),
      signals(result(projection = differentScope)).head.hcursor.get[String]("signal_id"))

  test("source and feed bounds fail closed while citations preserve exact counts"):
    val many = (1 to 70).map(id => row(id, seconds = id.toLong)).toList
    val cited = signals(result(rows = many)).head.hcursor
    assertEquals(cited.get[Int]("sample_size"), Right(70))
    assertEquals(cited.get[List[String]]("source_event_ids").map(_.size), Right(64))
    assertEquals(cited.get[Int]("omitted_source_event_count"), Right(6))
    assertEquals(signals(result(rows = List.fill(10001)(row(1)))), Nil)
    val values = (1 to 257).map(id => indicator(s"id-$id", value = s"host$id.example.com")).toList
    assertEquals(signals(result(feed = snapshot(values))), Nil)
    val tooManyFeeds = (1 to 9).map(id => snapshot(List(exact), feed = s"feed-$id")).toList
    assertEquals(signals(ProxyThreatFeed.evidence(List(row(1)), window, policy(tooManyFeeds), end)), Nil)

  test("numeric IPv6 and IDNA normalization preserve exactness without accepting network names as IPs"):
    val ip6 = indicator("ip6", "exact_selected_ip", "2001:0DB8:0:0:0:0:0:1")
    val found = signals(result(rows = List(row(1, selected = "2001:db8::1")), feed = snapshot(List(ip6))))
    assertEquals(found.head.hcursor.get[String]("indicator_value"), Right("2001:db8::1"))
    List("localhost", "192.0.2.1:443", "[2001:db8::1]", "fe80::1%eth0", "0192.0.2.1").foreach { value =>
      assertEquals(signals(result(feed = snapshot(List(indicator("bad", "exact_selected_ip", value))))), Nil)
    }
    val unicode = indicator("unicode", "exact_host", "b\u00fccher.example")
    val named = signals(result(rows = List(row(1, host = "xn--bcher-kva.example")), feed = snapshot(List(unicode))))
    assertEquals(named.size, 1)
