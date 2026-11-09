package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import munit.FunSuite

class ProxyAsnEnrichmentSuite extends FunSuite:
  private val at = Instant.parse("2026-11-01T12:00:00Z")
  private def entry(cidr: String, asn: Long): Json = Json.obj("cidr" -> cidr.asJson, "asn" -> asn.asJson)
  private val rows = List(entry("192.0.2.0/24", 64500), entry("192.0.2.128/25", 64501),
    entry("2001:db8::/32", 64502), entry("2001:db8:1::/48", 64503))
  private def cache(values: List[Json] = rows, version: String = "synthetic-v1"): Json = Json.obj(
    "source" -> "synthetic-routing-fixture".asJson,
    "publisher" -> "test-fixture-author".asJson,
    "source_uri" -> "urn:ssl-proxy:test:asn-cache".asJson,
    "snapshot_version" -> version.asJson,
    "license" -> "synthetic-test-only".asJson,
    "retrieved_at" -> at.minusSeconds(3600).toString.asJson,
    "refresh_after" -> at.plusSeconds(3600).toString.asJson,
    "expires_at" -> at.plusSeconds(7200).toString.asJson,
    "entries_sha256" -> ProxyAsnEnrichment.snapshotChecksum(values.asJson).getOrElse("invalid").asJson,
    "entries" -> values.asJson)
  private def evaluate(addresses: List[String], snapshot: Json = cache(), now: Instant = at): Json =
    ProxyAsnEnrichment.evidence(addresses, snapshot, now)
  private def addresses(result: Json): Map[String, Json] =
    result.hcursor.get[List[Json]]("addresses").toOption.get.map(row => row.hcursor.get[String]("ip").toOption.get -> row).toMap

  test("numeric IPv4 and IPv6 use longest matching prefix with cache provenance"):
    val result = evaluate(List("192.0.2.10", "192.0.2.200", "2001:0DB8:0:0:0:0:0:1", "2001:db8:1::1", "198.51.100.1"))
    val found = addresses(result)
    assertEquals(found("192.0.2.10").hcursor.get[Long]("asn"), Right(64500L))
    assertEquals(found("192.0.2.200").hcursor.get[Long]("asn"), Right(64501L))
    assertEquals(found("2001:db8::1").hcursor.get[Long]("asn"), Right(64502L))
    assertEquals(found("2001:db8:1::1").hcursor.get[Long]("asn"), Right(64503L))
    assertEquals(found("198.51.100.1").hcursor.get[Option[Long]]("asn"), Right(None))
    assertEquals(result.hcursor.get[Int]("matched_address_count"), Right(4))
    assertEquals(result.hcursor.downField("provenance").get[String]("source"), Right("synthetic-routing-fixture"))
    assertEquals(result.hcursor.downField("provenance").get[String]("license"), Right("synthetic-test-only"))
    assertEquals(result.hcursor.get[String]("refresh_state"), Right("fresh"))
    assertEquals(result.hcursor.get[String]("ownership"), Right("unavailable"))

  test("refresh is due at its exact boundary while expiry retracts all ASN values"):
    val due = evaluate(List("192.0.2.10"), now = at.plusSeconds(3600))
    assertEquals(due.hcursor.get[String]("refresh_state"), Right("refresh_due"))
    assertEquals(due.hcursor.get[Int]("matched_address_count"), Right(1))
    val expired = evaluate(List("192.0.2.10"), now = at.plusSeconds(7200))
    assertEquals(expired.hcursor.get[String]("disposition"), Right("unavailable_expired_local_cache"))
    assertEquals(expired.hcursor.get[List[Json]]("addresses"), Right(Nil))
    assertEquals(expired.hcursor.get[Boolean]("stale_fallback"), Right(false))
    assertEquals(expired.hcursor.downField("provenance").get[String]("snapshot_version"), Right("synthetic-v1"))

  test("new local snapshots refresh versioned values and outage cannot reuse an old result"):
    val previous = evaluate(List("192.0.2.10"))
    val changed = evaluate(List("192.0.2.10"), cache(List(entry("192.0.2.0/24", 64504)), "synthetic-v2"))
    assertEquals(addresses(previous)("192.0.2.10").hcursor.get[Long]("asn"), Right(64500L))
    assertEquals(addresses(changed)("192.0.2.10").hcursor.get[Long]("asn"), Right(64504L))
    assertEquals(changed.hcursor.downField("provenance").get[String]("snapshot_version"), Right("synthetic-v2"))
    val outage = evaluate(List("192.0.2.10"), Json.obj())
    assertEquals(outage.hcursor.get[String]("disposition"), Right("unavailable_no_local_cache"))
    assertEquals(outage.hcursor.get[List[Json]]("addresses"), Right(Nil))
    assertEquals(outage.hcursor.get[Boolean]("stale_fallback"), Right(false))

  test("checksum and metadata validation fail closed without publishing untrusted provenance"):
    val invalid = List(
      Json.fromString("unavailable-provider-response"),
      cache().deepMerge(Json.obj("entries_sha256" -> ("0" * 64).asJson)),
      cache().deepMerge(Json.obj("license" -> "".asJson)),
      cache().deepMerge(Json.obj("publisher" -> "".asJson)),
      cache().deepMerge(Json.obj("source_uri" -> "https://user:password@example.com/cache".asJson)),
      cache().deepMerge(Json.obj("source_uri" -> "relative/file".asJson)),
      cache().deepMerge(Json.obj("unexpected" -> "ignored".asJson)),
      cache().mapObject(_.remove("entries_sha256")))
    invalid.foreach { snapshot =>
      val result = evaluate(List("192.0.2.10"), snapshot)
      assertEquals(result.hcursor.get[String]("disposition"), Right("unavailable_invalid_local_cache"))
      assertEquals(result.hcursor.get[Option[Json]]("provenance"), Right(None))
      assertEquals(result.hcursor.get[List[Json]]("addresses"), Right(Nil))
    }

  test("timestamps require finite ordered retrieval refresh and expiry with no future retrieval"):
    val invalid = List(
      "retrieved_at" -> at.plusSeconds(1).toString,
      "retrieved_at" -> "infinity",
      "refresh_after" -> at.minusSeconds(7200).toString,
      "expires_at" -> at.plusSeconds(3600).toString,
      "expires_at" -> "+10000-01-01T00:00:00Z")
    invalid.foreach { (field, value) =>
      assertEquals(evaluate(List("192.0.2.10"), cache().deepMerge(Json.obj(field -> value.asJson)))
        .hcursor.get[String]("disposition"), Right("unavailable_invalid_local_cache"))
    }
    assertEquals(evaluate(List("192.0.2.10"), now = Instant.MAX).hcursor.get[String]("disposition"), Right("invalid_evaluation_time"))

  test("hostnames ports zones short IPv4 and mapped IPv6 cannot take a DNS lookup path"):
    val invalid = List("localhost", "resolver.example", "127.1", "0192.0.2.1", "192.0.2.1:443",
      "[2001:db8::1]", "fe80::1%en0", "::ffff:192.0.2.1", "::ffff:c000:201", "-")
    val result = evaluate(invalid)
    assertEquals(result.hcursor.get[Int]("invalid_address_count"), Right(invalid.size))
    assertEquals(result.hcursor.get[List[Json]]("addresses"), Right(Nil))
    assert(!result.noSpaces.contains("resolver.example"))

  test("prefixes must be network aligned numeric family-valid and unique after normalization"):
    val invalid = List(
      List(entry("192.0.2.1/24", 64500)), List(entry("2001:db8::1/32", 64500)),
      List(entry("resolver.example/24", 64500)), List(entry("192.0.2.0/33", 64500)),
      List(entry("2001:db8::/129", 64500)), List(entry("192.0.2.0/024", 64500)),
      List(entry("192.0.2.0/24", 0)), List(entry("192.0.2.0/24", 4294967296L)),
      List(entry("192.0.2.0/24", 64500), entry("192.0.2.0/24", 64501)),
      List(entry("2001:db8::/32", 64500), entry("2001:0DB8:0:0::/32", 64500)))
    invalid.foreach { values =>
      assertEquals(ProxyAsnEnrichment.snapshotChecksum(values.asJson), None)
      assertEquals(evaluate(List("192.0.2.10"), cache(values)).hcursor.get[String]("disposition"), Right("unavailable_invalid_local_cache"))
    }

  test("checksums input order and duplicate address replay preserve deterministic evidence"):
    val values = List("192.0.2.200", "2001:db8::1", "192.0.2.10")
    val result = evaluate(values)
    val reordered = evaluate(values.reverse, cache(rows.reverse))
    assertEquals(result, reordered)
    assertEquals(ProxyAsnEnrichment.snapshotChecksum(rows.asJson), ProxyAsnEnrichment.snapshotChecksum(rows.reverse.asJson))
    val replay = evaluate(values ++ values)
    assertEquals(addresses(result), addresses(replay))
    assertEquals(replay.hcursor.get[Int]("unique_address_count"), Right(3))

  test("empty cache entries and maximum ASN remain explicit non-threat routing evidence"):
    val empty = evaluate(List("192.0.2.10"), cache(Nil))
    assertEquals(empty.hcursor.get[String]("disposition"), Right("cached_evidence_available"))
    assertEquals(empty.hcursor.get[Int]("matched_address_count"), Right(0))
    val maximum = evaluate(List("192.0.2.10"), cache(List(entry("0.0.0.0/0", 4294967295L))))
    assertEquals(addresses(maximum)("192.0.2.10").hcursor.get[Long]("asn"), Right(4294967295L))
    assert(!maximum.noSpaces.contains("confirmed_threat"))

  test("input and cache-entry bounds fail closed instead of truncating positive results"):
    val excessive = evaluate(List.fill(257)("192.0.2.10"))
    assertEquals(excessive.hcursor.get[String]("disposition"), Right("address_bound_exceeded"))
    assertEquals(excessive.hcursor.get[List[Json]]("addresses"), Right(Nil))
    val tooManyEntries = (0 to 256).map(index => entry(s"10.${index / 256}.${index % 256}.0/24", 64500)).toList
    assertEquals(ProxyAsnEnrichment.snapshotChecksum(tooManyEntries.asJson), None)
    assertEquals(evaluate(List("192.0.2.10"), cache(tooManyEntries)).hcursor.get[String]("disposition"), Right("unavailable_invalid_local_cache"))
