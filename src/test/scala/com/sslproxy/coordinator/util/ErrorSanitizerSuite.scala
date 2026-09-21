package com.sslproxy.coordinator.util

import munit.FunSuite

class ErrorSanitizerSuite extends FunSuite:
  test("wrapped cyclic batch failures disclose metadata only"):
    val batch = new java.sql.BatchUpdateException(
      "Batch entry 0 INSERT INTO inventory VALUES ('synthetic-client', 'private-ssid', 'password=hunter2') aborted",
      null,
      7,
      Array.emptyIntArray
    )
    val detail = new java.sql.SQLException("Detail: key synthetic-client timeout-value", "23505", 9)
    batch.setNextException(detail)
    detail.initCause(batch)
    val summary = ErrorSanitizer.message(new RuntimeException("token=outer-secret INSERT private-value", batch))
    assert(summary.contains("SQLSTATE=23505"))
    assert(summary.contains("vendor_code=9"))
    assert(summary.contains("classification=permanent"))
    List("INSERT", "synthetic-client", "private-ssid", "hunter2", "timeout-value", "outer-secret", "private-value")
      .foreach { value =>
        assert(!summary.contains(value), summary)
      }
    assert(summary.length <= 512)

  test("error messages redact secrets remove controls and enforce a length bound"):
    val sanitized = ErrorSanitizer.sanitize(
      "database failed\npassword=hunter2 token:abc123 " + ("x" * 1000)
    )

    assert(!sanitized.contains("hunter2"))
    assert(!sanitized.contains("abc123"))
    assert(!sanitized.exists(_.isControl))
    assert(sanitized.length <= 512)

  test("Bearer tokens are redacted with or without an authorization prefix"):
    val header = ErrorSanitizer.sanitize("Authorization: Bearer header.token-value")
    val standalone = ErrorSanitizer.sanitize("request failed for Bearer standalone-token")

    assertEquals(header, "Authorization: Bearer [REDACTED]")
    assertEquals(standalone, "request failed for Bearer [REDACTED]")
