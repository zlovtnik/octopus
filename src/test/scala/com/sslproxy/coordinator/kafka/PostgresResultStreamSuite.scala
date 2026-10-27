package com.sslproxy.coordinator.kafka

import cats.data.EitherT
import cats.effect.{IO, Ref}
import com.sslproxy.coordinator.domain.{BrokerRecordMetadata, DatabaseError, IngestionInvariantViolation}
import com.sslproxy.coordinator.persistence.{DbResultT, ResultStore}
import com.sslproxy.coordinator.postgres.{PostgresLoad, PostgresResult}
import munit.CatsEffectSuite

class PostgresResultStreamSuite extends CatsEffectSuite:
  private val metadata = BrokerRecordMetadata("sync.oracle.result", 0, 1L, "octopus-result-v1", 1, "a" * 64, None, "b" * 64)
  private val result = PostgresResult.success("job", "batch", 1, "checksum", "2026-10-25T00:00:00Z")

  test("contradictory broker evidence fails closed without DLQ parking"):
    val conflict = IngestionInvariantViolation("broker coordinate changed after ingestion")
    checkFailure(DatabaseError.Permanent("result", RuntimeException("wrapped", conflict), "conflict"), expectParked = false)

  test("transient storage failure fails closed without DLQ parking"):
    val cause = java.sql.SQLException("connection reset", "08006")
    checkFailure(DatabaseError.Retryable("result", cause, "connection reset"), expectParked = false)

  test("permanent invalid results are parked before they can be acknowledged"):
    val cause = java.sql.SQLException("invalid result", "23514")
    checkFailure(DatabaseError.Permanent("result", cause, "invalid result"), expectParked = true)

  test("DLQ publication failures prevent acknowledgment"):
    val cause = IllegalArgumentException("invalid result")
    val brokerFailure = IllegalStateException("broker unavailable")
    PostgresResultStream
      .persistResult(
        result,
        metadata,
        new FailedStore(DatabaseError.Permanent("result", cause, "invalid result")),
        _ => IO.raiseError(brokerFailure)
      )
      .attempt
      .map(actual => assertEquals(actual, Left(brokerFailure)))

  private def checkFailure(error: DatabaseError, expectParked: Boolean): IO[Unit] =
    for
      parked <- Ref.of[IO, Int](0)
      recorded <- PostgresResultStream
        .persistResult(result, metadata, new FailedStore(error), _ => parked.update(_ + 1))
        .attempt
      count <- parked.get
    yield
      if expectParked then assertEquals(recorded, Right(false))
      else assert(recorded.isLeft)
      assertEquals(count, if expectParked then 1 else 0)

  private class FailedStore(error: DatabaseError) extends ResultStore[IO]:
    def recordResultWithEvidence(result: PostgresResult, metadata: BrokerRecordMetadata): DbResultT[IO, Unit] =
      EitherT.leftT[IO, Unit](error)

    def recordLoadResultsWithEvidence(
      records: List[(PostgresLoad, PostgresResult, BrokerRecordMetadata)]
    ): DbResultT[IO, Unit] = EitherT.leftT[IO, Unit](error)

    def recordResultsWithEvidence(records: List[(PostgresResult, BrokerRecordMetadata)]): DbResultT[IO, Unit] =
      EitherT.leftT[IO, Unit](error)
