package com.sslproxy.coordinator.persistence

import munit.FunSuite

class BatchStatementSuite extends FunSuite:
  test("batch validation accepts bounded batches") {
    assertEquals(BatchStatement.validateBatch(List(1, 2), 2), Right(List(1, 2)))
  }

  test("batch validation rejects invalid limits and oversized batches") {
    assert(BatchStatement.validateBatch(List(1), 0).isLeft)
    assert(BatchStatement.validateBatch(List(1, 2), 1).isLeft)
  }
