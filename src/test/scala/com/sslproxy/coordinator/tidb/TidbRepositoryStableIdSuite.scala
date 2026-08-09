package com.sslproxy.coordinator.tidb

import munit.FunSuite

class TidbRepositoryStableIdSuite extends FunSuite:
  test("stable UUID encoding is unambiguous when fields contain separators"):
    val left = TidbRepository.stableUuid("job", "a:b", "c")
    val right = TidbRepository.stableUuid("job", "a", "b:c")

    assertNotEquals(left, right)

  test("stable UUID encoding uses one versioned length-prefixed representation"):
    val expected = java.util.UUID.nameUUIDFromBytes(
      "6:job:v212:proxy.events6:dedupe".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    ).toString

    assertEquals(TidbRepository.stableUuid("job", "proxy.events", "dedupe"), expected)
