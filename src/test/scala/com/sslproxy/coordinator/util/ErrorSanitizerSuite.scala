package com.sslproxy.coordinator.util

import munit.FunSuite

class ErrorSanitizerSuite extends FunSuite:
  test("error messages redact secrets remove controls and enforce a length bound"):
    val sanitized = ErrorSanitizer.sanitize(
      "database failed\npassword=hunter2 token:abc123 " + ("x" * 1000)
    )

    assert(!sanitized.contains("hunter2"))
    assert(!sanitized.contains("abc123"))
    assert(!sanitized.exists(_.isControl))
    assert(sanitized.length <= 512)
