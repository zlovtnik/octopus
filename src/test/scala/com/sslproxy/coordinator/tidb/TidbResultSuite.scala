package com.sslproxy.coordinator.tidb

import io.circe.parser.decode
import munit.FunSuite

class TidbResultSuite extends FunSuite:
  test("decoder defaults omitted state-dependent result fields"):
    val payload =
      """{"job_id":"job-1","batch_id":"batch-1","status":"success","row_count":2,"finished_at":"2026-08-03T12:00:00Z"}"""

    val result = decode[TidbResult](payload).fold(error => fail(error.getMessage), identity)

    assertEquals(result.checksum, "")
    assertEquals(result.retryable, false)
    assertEquals(result.errorClass, "")
    assertEquals(result.errorText, "")

  test("decoder keeps identifiers and completion fields required"):
    val payload = """{"status":"success","row_count":2}"""

    assert(decode[TidbResult](payload).isLeft)
