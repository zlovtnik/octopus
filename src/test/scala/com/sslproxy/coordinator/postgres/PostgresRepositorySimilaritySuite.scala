package com.sslproxy.coordinator.postgres

import munit.FunSuite

class PostgresRepositorySimilaritySuite extends FunSuite:
  test("full similarity pages leave the anchor eligible for another pass"):
    assert(!PostgresRepository.similarityAnchorExhausted(16, 16))
    assert(!PostgresRepository.similarityAnchorExhausted(64, 250))

  test("short similarity pages finish the anchor"):
    assert(PostgresRepository.similarityAnchorExhausted(15, 16))
    assert(PostgresRepository.similarityAnchorExhausted(63, 250))
