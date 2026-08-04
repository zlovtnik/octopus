package com.sslproxy.coordinator.domain

import munit.FunSuite

class ScanRequestRecordSuite extends FunSuite:
  private def request(observedAt: String): String =
    s"""{"stream_name":"proxy.events","dedupe_key":"dedupe","payload_ref":"payload.json","observed_at":"$observedAt"}"""

  test("wire decoder accepts RFC3339 timestamps"):
    assert(ScanRequestRecord.decodeWire(request("2026-08-03T12:34:56-04:00")).isRight)

  test("wire decoder rejects malformed timestamps"):
    assert(ScanRequestRecord.decodeWire(request("2026-08-03 12:34:56")).isLeft)
