package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import munit.FunSuite

class ProxyClientHelloDriftSuite extends FunSuite:
  private val start = Instant.parse("2026-11-01T12:00:00Z")
  private val original = "771,4865-4866,0-16,29-23,0"
  private val updated = "771,4865-4867,0-16,29-23,0"
  private def row(id: Int, fingerprint: String = original, parser: String = "proxy-clienthello-v2", alpn: String = "h2", kind: String = "tunnel_open"): Json =
    Json.obj("event_id" -> f"00000000-0000-4000-8000-$id%012d".asJson,
      "time" -> start.plusSeconds(id.toLong).toString.asJson, "type" -> kind.asJson,
      "evidence_version" -> ProxyEvidence.Version.asJson,
      "tls_evidence" -> Json.obj("source" -> "client_hello".asJson, "parser_version" -> parser.asJson,
        "parse_status" -> "parsed".asJson, "semantics" -> "offered_not_negotiated".asJson,
        "ja3_lite" -> fingerprint.asJson, "offered_alpn" -> alpn.asJson))
  private val baseline = (1 to 6).map(row(_)).toList
  private val current = (10 to 15).map(row(_)).toList
  private def evidence(now: List[Json] = current, before: List[Json] = baseline,
      complete: Boolean = true, registered: Boolean = true): Json =
    ProxyClientHelloDrift.evidence(now, before, complete, registered)
  private def profile(value: Json): Json = value.hcursor.get[List[Json]]("parser_profiles").toOption.get.head

  test("unchanged offers are stable and replay ordering does not change evidence"):
    val result = evidence()
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(profile(result).hcursor.get[String]("disposition"), Right("no_new_offer_values"))
    assertEquals(evidence(current.reverse ++ current, baseline.reverse ++ baseline), result)
    assertEquals(result.hcursor.get[Int]("current_available_count"), Right(6))

  test("benign software update is drift evidence with no threat or ownership inference"):
    val update = current.map(_.deepMerge(Json.obj("tls_evidence" -> Json.obj("ja3_lite" -> updated.asJson))))
    val result = evidence(update)
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(true))
    assertEquals(result.hcursor.get[String]("interpretation"), Right("software_stack_drift_candidate_only"))
    assertEquals(result.hcursor.get[String]("ownership"), Right("unavailable"))
    assertEquals(profile(result).hcursor.get[List[String]]("new_ja3_lite_values"), Right(List(updated)))
    assertEquals(result.hcursor.get[List[String]]("benign_explanations"), Right(List("software_update", "application_mix_change")))

  test("first offered ALPN change needs supported samples and never claims negotiation"):
    val result = evidence(current.map(_.deepMerge(Json.obj("tls_evidence" -> Json.obj("offered_alpn" -> "http/1.1".asJson)))))
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(true))
    assertEquals(result.hcursor.get[String]("alpn_semantics"), Right("first_offered_protocol_only_not_complete_list_or_negotiated"))
    val unavailable = current.map(_.deepMerge(Json.obj("tls_evidence" -> Json.obj("offered_alpn" -> Json.Null))))
    assertEquals(evidence(unavailable).hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(profile(evidence(unavailable)).hcursor.get[Boolean]("alpn_comparison_available"), Right(false))

  test("parser revisions never compare and sparse history cannot establish drift"):
    val newParser = current.map(_.deepMerge(Json.obj("tls_evidence" -> Json.obj("parser_version" -> "proxy-clienthello-v1".asJson,
      "ja3_lite" -> updated.asJson))))
    val result = evidence(newParser)
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(result.hcursor.get[List[Json]]("parser_profiles").toOption.get.size, 2)
    val changed = current.map(_.deepMerge(Json.obj("tls_evidence" -> Json.obj("ja3_lite" -> updated.asJson))))
    assertEquals(profile(evidence(changed, baseline.take(5))).hcursor.get[String]("disposition"), Right("insufficient_samples"))
    assertEquals(evidence(changed, Nil).hcursor.get[Boolean]("candidate"), Right(false))

  test("unsupported evidence and parse outcomes remain unavailable in denominators"):
    val mutations = List(
      Json.obj("evidence_version" -> "legacy".asJson),
      Json.obj("tls_evidence" -> Json.obj("source" -> "server_hello".asJson)),
      Json.obj("tls_evidence" -> Json.obj("parser_version" -> "unknown".asJson)),
      Json.obj("tls_evidence" -> Json.obj("parse_status" -> "timeout".asJson)),
      Json.obj("tls_evidence" -> Json.obj("semantics" -> "negotiated".asJson)),
      Json.obj("tls_evidence" -> Json.obj("ja3_lite" -> "771,not-a-fingerprint,,,".asJson)))
    val result = evidence(current.zip(mutations).map((source, mutation) => source.deepMerge(mutation)))
    assertEquals(result.hcursor.get[Int]("current_denominator"), Right(6))
    assertEquals(result.hcursor.get[Int]("current_unavailable_count"), Right(6))
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(false))

  test("collection gaps and unsupported registration prevent positive candidates"):
    val update = (10 to 15).map(row(_, updated)).toList
    assertEquals(evidence(update, complete = false).hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(profile(evidence(update, complete = false)).hcursor.get[String]("disposition"), Right("collection_censored"))
    assertEquals(evidence(update, registered = false).hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(profile(evidence(update, registered = false)).hcursor.get[String]("disposition"), Right("unsupported_attribution_scope"))

  test("open and close cannot double samples; conflicting event IDs are excluded"):
    val closes = (20 to 25).map(row(_, updated, kind = "tunnel_close")).toList
    assertEquals(evidence(current ++ closes), evidence())
    val conflict = current.head.deepMerge(Json.obj("tls_evidence" -> Json.obj("ja3_lite" -> updated.asJson)))
    val result = evidence(current :+ conflict)
    assertEquals(result.hcursor.get[Int]("current_denominator"), Right(5))
    assertEquals(profile(result).hcursor.get[String]("disposition"), Right("insufficient_samples"))

  test("future and overlapping source history does not leak into the baseline"):
    assertEquals(evidence(current, baseline ++ current ++ List(row(100, updated))), evidence())
    val result = profile(evidence()).hcursor.downField("baseline")
    assertEquals(result.get[String]("first_observed_at"), Right(start.plusSeconds(1).toString))
    assertEquals(result.get[String]("last_observed_at"), Right(start.plusSeconds(6).toString))
    assertEquals(result.get[List[String]]("source_event_ids").toOption.get.size, 6)

  test("oversized snapshots and malformed or oversized fingerprints fail closed"):
    val overflow = evidence(List.fill(10001)(current.head))
    assertEquals(overflow.hcursor.get[String]("disposition"), Right("source_row_bound_exceeded"))
    assertEquals(overflow.hcursor.get[Boolean]("candidate"), Right(false))
    List("771,65536,,,", "771,1,,,256", "771,1,,,0,extra", "771," + "1-" * 3000 + ",,,").foreach { invalid =>
      val result = evidence((10 to 15).map(row(_, invalid)).toList)
      assertEquals(result.hcursor.get[Int]("current_available_count"), Right(0))
      assertEquals(result.hcursor.get[Boolean]("candidate"), Right(false))
    }

  test("high cardinality profiles retain exact counts with bounded values and citations"):
    val rows = (10 to 160).map(id => row(id, s"771,$id,,,0")).toList
    val result = profile(evidence(rows)).hcursor
    assertEquals(result.get[Int]("new_ja3_lite_value_count"), Right(151))
    assertEquals(result.get[List[String]]("new_ja3_lite_values").toOption.get.size, 128)
    val currentProfile = result.downField("current")
    assertEquals(currentProfile.get[Int]("sample_size"), Right(151))
    assertEquals(currentProfile.get[Int]("omitted_ja3_lite_value_count"), Right(23))
    assertEquals(currentProfile.get[List[String]]("source_event_ids").toOption.get.size, 64)
    assertEquals(currentProfile.get[Int]("omitted_source_event_count"), Right(87))
