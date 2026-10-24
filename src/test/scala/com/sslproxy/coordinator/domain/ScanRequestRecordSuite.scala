package com.sslproxy.coordinator.domain

import munit.FunSuite
import com.sslproxy.coordinator.util.Sha256Utils

class ScanRequestRecordSuite extends FunSuite:
  private def request(observedAt: String): String =
    s"""{"stream_name":"proxy.events","dedupe_key":"dedupe","payload_ref":"payload.json","observed_at":"$observedAt"}"""

  test("wire decoder accepts RFC3339 timestamps"):
    assert(ScanRequestRecord.decodeWire(request("2026-08-03T12:34:56-04:00")).isRight)

  test("wire decoder rejects malformed timestamps"):
    assert(ScanRequestRecord.decodeWire(request("2026-08-03 12:34:56")).isLeft)

  test("wire decoder rejects identifiers wider than PostgreSQL columns"):
    val streamTooLong =
      s"""{"stream_name":"${"s" * 256}","dedupe_key":"dedupe","payload_ref":"payload.json","observed_at":"2026-08-03T12:34:56Z"}"""
    val dedupeTooLong =
      s"""{"stream_name":"proxy.events","dedupe_key":"${"d" * 256}","payload_ref":"payload.json","observed_at":"2026-08-03T12:34:56Z"}"""

    assert(ScanRequestRecord.decodeWire(streamTooLong).isLeft)
    assert(ScanRequestRecord.decodeWire(dedupeTooLong).isLeft)

  test("wire decoder rejects payload references wider than PostgreSQL MEDIUMTEXT"):
    val payloadRef = "\u20ac" * ((ScanRequestRecord.PayloadRefMaxBytes / 3) + 1)
    val oversized =
      s"""{"stream_name":"proxy.events","dedupe_key":"dedupe","payload_ref":"$payloadRef","observed_at":"2026-08-03T12:34:56Z"}"""

    assert(ScanRequestRecord.decodeWire(oversized).isLeft)

  private def source: ScanRequestRecord =
    ScanRequestRecord.decodeWire(request("2026-08-03T12:34:56Z")).fold(throw _, identity)

  test("resolved factory preserves original strings and UTF-8 hashes"):
    val payload = " \n{\"event_type\":\"  kind  \",\"value\":\"\u20ac\"}\t"
    val record = ResolvedScanRequestRecord.from(source, payload).fold(throw _, identity)
    assertEquals(record.payloadJson, payload)
    assertEquals(record.eventPayloadSha256, Sha256Utils.sha256Hex(payload))
    assertEquals(record.inspection.map(_.eventKind), Right(Some("  kind  ")))
    assert(record.inspection eq record.inspection)

  test("direct construction is inspected and copies inspect their own payload"):
    val record = ResolvedScanRequestRecord(source, """{"type":"original"}""", "hash")
    assertEquals(record.inspection.map(_.eventKind), Right(Some("original")))
    val copied = record.copy(payloadJson = """{"type":"copy"}""")
    assertEquals(copied.inspection.map(_.eventKind), Right(Some("copy")))
    assertEquals(record.inspection.map(_.eventKind), Right(Some("original")))
    assert(record.copy(payloadJson = "null").inspection.isLeft)
    val invalid = ResolvedScanRequestRecord(source, "[invalid]", "hash")
    assert(invalid.inspection.isLeft)
    assert(invalid.copy(payloadJson = "[]").inspection.isRight)

  test("resolved factory rejects null, empty, malformed and trailing documents"):
    List("null", "", "{} {}", "[1,broken]").foreach { payload =>
      assert(ResolvedScanRequestRecord.from(source, payload).isLeft)
    }
