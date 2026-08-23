package com.sslproxy.coordinator.persistence

import cats.data.EitherT
import cats.effect.IO
import com.sslproxy.coordinator.domain.DatabaseError
import munit.CatsEffectSuite

class DbResultTSuite extends CatsEffectSuite:
  test("orRaise preserves successful values"):
    val result = EitherT.rightT[IO, DatabaseError](42)
    result.orRaise.map(value => assertEquals(value, 42))

  test("orRaise exposes the typed database operation with a sanitized attached cause"):
    val cause = IllegalStateException("connection lost password=driver-secret")
    val error: DatabaseError = DatabaseError.Retryable("postgres.claim", cause, cause.getMessage)
    val result: DbResultT[IO, Int] = EitherT.leftT[IO, Int](error)

    result.orRaise.attempt.map {
      case Left(failure: DatabaseOperationException) =>
        assertEquals(failure.error, error)
        assertNotEquals(failure.getCause, cause)
        assert(!failure.getCause.getMessage.contains("driver-secret"))
      case other => fail(s"expected DatabaseOperationException, got $other")
    }
