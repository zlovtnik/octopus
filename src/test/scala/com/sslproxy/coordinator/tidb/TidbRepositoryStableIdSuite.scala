package com.sslproxy.coordinator.tidb

import munit.FunSuite

class TidbRepositoryStableIdSuite extends FunSuite:
  test("stable UUID encoding is unambiguous when fields contain separators"):
    val left = TidbRepository.stableUuid("job", "a:b", "c")
    val right = TidbRepository.stableUuid("job", "a", "b:c")

    assertNotEquals(left, right)

  test("stable UUID encoding preserves legacy IDs for separator-free fields"):
    val expected = java.util.UUID.nameUUIDFromBytes(
      "job:proxy.events:dedupe".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    ).toString

    assertEquals(TidbRepository.stableUuid("job", "proxy.events", "dedupe"), expected)
