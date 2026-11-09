package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import munit.FunSuite

class ProxyDestinationInfrastructureSuite extends FunSuite:
  private val at = Instant.parse("2026-11-01T12:00:00Z")
  private def row(id: String, selected: String, resolved: List[String] = Nil, seconds: Long = 0): Json =
    Json.obj("event_id" -> id.asJson, "time" -> at.plusSeconds(seconds).toString.asJson,
      "evidence_version" -> ProxyEvidence.Version.asJson, "type" -> "tunnel_open".asJson,
      "blocked" -> false.asJson, "selected_ip" -> selected.asJson,
      "resolved_ips" -> resolved.asJson, "peer_ip" -> "198.51.100.99".asJson)
  private def evidence(current: List[Json], history: List[Json] = Nil, complete: Boolean = true): Json =
    ProxyDestinationInfrastructure.evidence(current, history, complete)

  test("selected destination is independent of peer and resolver candidates"):
    val result = evidence(List(row("now", "192.0.2.2", List("192.0.2.1", "192.0.2.2"))),
      List(row("prior", "192.0.2.1", List("192.0.2.1", "192.0.2.2"), -3600)))
    assertEquals(result.hcursor.downField("selected_ip_change").get[Boolean]("observed_change"), Right(true))
    assertEquals(result.hcursor.downField("selected_ip_change").get[List[String]]("newly_observed_ips"), Right(List("192.0.2.2")))
    assertEquals(result.hcursor.downField("resolved_ip_change").get[Boolean]("observed_change"), Right(false))
    assert(!result.noSpaces.contains("198.51.100.99"))
    assertEquals(result.hcursor.downField("current").get[Int]("selected_ip_observations"), Right(1))
    assertEquals(result.hcursor.downField("asn").get[String]("disposition"), Right("unavailable_no_versioned_enrichment_source"))

  test("numeric IPv6 forms normalize without resolving names"):
    val result = evidence(List(row("now", "2001:0DB8:0:0:0:0:0:1")), List(row("prior", "2001:db8::1", seconds = -3600)))
    assertEquals(result.hcursor.downField("selected_ip_change").get[Boolean]("observed_change"), Right(false))
    List("localhost", "example.com", "127.1", "0192.0.2.1", "192.0.2.999", "192.0.2.1:443", "[2001:db8::1]", "fe80::1%en0", "-").foreach { value =>
      val invalid = evidence(List(row("bad", value, List(value))))
      assertEquals(invalid.hcursor.get[String]("disposition"), Right("destination_ips_unavailable"), value)
      assertEquals(invalid.hcursor.downField("current").get[Int]("selected_ip_observations"), Right(0), value)
    }

  test("replay and input order preserve counts and supporting references"):
    val rows = List(row("b", "192.0.2.2", List("192.0.2.2", "192.0.2.2"), 10), row("a", "192.0.2.1"))
    assertEquals(evidence(rows), evidence(rows.reverse ++ rows))
    assertEquals(evidence(rows).hcursor.downField("current").get[Int]("eligible_open_observations"), Right(2))
    val conflicting = row("a", "192.0.2.3")
    val result = evidence(rows ++ List(conflicting))
    assertEquals(result.hcursor.downField("current").get[Int]("eligible_open_observations"), Right(1))

  test("closes, blocked observations and unsupported versions cannot invent connections"):
    val base = row("open", "192.0.2.1")
    val close = base.deepMerge(Json.obj("event_id" -> "close".asJson, "type" -> "tunnel_close".asJson))
    val blocked = base.deepMerge(Json.obj("event_id" -> "blocked".asJson, "blocked" -> true.asJson))
    val legacy = base.deepMerge(Json.obj("event_id" -> "legacy".asJson, "evidence_version" -> "legacy".asJson))
    val result = evidence(List(base, close, blocked, legacy))
    assertEquals(result.hcursor.downField("current").get[Int]("eligible_open_observations"), Right(1))
    assertEquals(evidence(List(close, blocked, legacy)).hcursor.get[String]("disposition"), Right("no_supported_open_observations"))

  test("missing history or collection cannot assert a change"):
    val base = row("open", "192.0.2.1")
    assertEquals(evidence(List(base)).hcursor.downField("selected_ip_change").get[String]("disposition"), Right("history_unavailable"))
    val censored = evidence(List(base), List(row("prior", "192.0.2.2", seconds = -3600)), false)
    assertEquals(censored.hcursor.get[String]("disposition"), Right("collection_censored"))
    assertEquals(censored.hcursor.downField("selected_ip_change").get[Option[Boolean]]("observed_change"), Right(None))
    assertEquals(censored.hcursor.downField("selected_ip_change").get[List[String]]("newly_observed_ips"), Right(Nil))
    val resolvedOnly = evidence(List(row("resolved-only", "-", List("192.0.2.1"))))
    assertEquals(resolvedOnly.hcursor.downField("current").get[Int]("selected_ip_observations"), Right(0))
    assertEquals(resolvedOnly.hcursor.downField("current").get[Int]("resolved_ip_observations"), Right(1))

  test("ordinary CDN rotation is descriptive evidence with a shared infrastructure caveat"):
    val result = evidence(List(row("cdn-now", "192.0.2.2")), List(row("cdn-prior", "192.0.2.1", seconds = -3600)))
    assertEquals(result.hcursor.get[String]("interpretation"), Right("observed_infrastructure_change_only"))
    assertEquals(result.hcursor.get[String]("disposition"), Right("descriptive_evidence"))
    assert(result.hcursor.get[String]("caveat").toOption.exists(_.contains("CDN rotation")))
    assert(!result.noSpaces.contains("confirmed_threat"))

  test("row and address bounds fail closed instead of publishing partial sets"):
    val oversized = row("oversized", "192.0.2.1", List.fill(65)("192.0.2.1"))
    val result = evidence(List(oversized))
    assertEquals(result.hcursor.get[String]("disposition"), Right("input_or_address_bound_exceeded"))
    assertEquals(result.hcursor.downField("current").get[List[Json]]("selected_ips"), Right(Nil))
    val rows = (1 to 129).map(i => row(s"ip-$i", s"192.0.2.$i")).toList
    assertEquals(evidence(rows).hcursor.get[String]("disposition"), Right("input_or_address_bound_exceeded"))
    assertEquals(evidence(List.fill(10001)(row("dup", "192.0.2.1"))).hcursor.get[String]("disposition"), Right("input_or_address_bound_exceeded"))

  test("reference pages remain bounded while reporting exact observation counts"):
    val rows = (1 to 40).map(i => row(s"ref-$i", "192.0.2.1", seconds = i)).toList
    val result = evidence(rows)
    assertEquals(result.hcursor.downField("current").get[List[String]]("source_event_ids").map(_.size), Right(32))
    assertEquals(result.hcursor.downField("current").get[Int]("omitted_reference_count"), Right(8))
    assertEquals(result.hcursor.downField("current").get[Int]("selected_ip_observations"), Right(40))
