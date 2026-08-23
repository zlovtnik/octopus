package com.sslproxy.coordinator.postgres

import munit.*

class PostgresChecksumSuite extends FunSuite:

  test("checksum is stable for same input"):
    val payload = """{"test": "data"}"""
    val a = PostgresChecksum.checksum(PostgresSinkTarget.ProxyEvents, payload)
    val b = PostgresChecksum.checksum(PostgresSinkTarget.ProxyEvents, payload)
    assertEquals(a, b)

  test("checksum differs for different targets"):
    val payload = """{"test": "data"}"""
    val a = PostgresChecksum.checksum(PostgresSinkTarget.ProxyEvents, payload)
    val b = PostgresChecksum.checksum(PostgresSinkTarget.WirelessAuditFrames, payload)
    assertNotEquals(a, b)

  test("checksum differs for different payloads"):
    val a = PostgresChecksum.checksum(PostgresSinkTarget.ProxyEvents, """{"a": 1}""")
    val b = PostgresChecksum.checksum(PostgresSinkTarget.ProxyEvents, """{"a": 2}""")
    assertNotEquals(a, b)

  test("checksum is non-empty"):
    val result = PostgresChecksum.checksum(PostgresSinkTarget.ProxyEvents, """{"test": "data"}""")
    assert(result.nonEmpty)
