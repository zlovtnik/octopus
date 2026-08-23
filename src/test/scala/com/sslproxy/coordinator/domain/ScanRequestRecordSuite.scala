package com.sslproxy.coordinator.domain

import munit.FunSuite

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
