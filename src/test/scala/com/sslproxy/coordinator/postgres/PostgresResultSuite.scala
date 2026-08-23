package com.sslproxy.coordinator.postgres

import io.circe.parser.decode
import munit.FunSuite

class PostgresResultSuite extends FunSuite:
  test("decoder defaults omitted state-dependent result fields"):
    val payload =
      """{"job_id":"job-1","batch_id":"batch-1","status":"success","row_count":2,"finished_at":"2026-08-03T12:00:00Z"}"""

    val result = decode[PostgresResult](payload).fold(error => fail(error.getMessage), identity)

    assertEquals(result.checksum, "")
    assertEquals(result.retryable, false)
    assertEquals(result.errorClass, "")
    assertEquals(result.errorText, "")

  test("decoder keeps identifiers and completion fields required"):
    val payload = """{"status":"success","row_count":2}"""

    assert(decode[PostgresResult](payload).isLeft)

  test("decoder derives retryability from a retryable error class"):
    val payload =
      """{"job_id":"job-1","batch_id":"batch-1","status":"failed","row_count":0,"error_class":"retryable","finished_at":"2026-08-03T12:00:00Z"}"""

    val result = decode[PostgresResult](payload).fold(error => fail(error.getMessage), identity)
    assertEquals(result.retryable, true)

  test("decoder rejects retryability that conflicts with the error class"):
    val payload =
      """{"job_id":"job-1","batch_id":"batch-1","status":"failed","row_count":0,"retryable":false,"error_class":"retryable","finished_at":"2026-08-03T12:00:00Z"}"""

    assert(decode[PostgresResult](payload).isLeft)
