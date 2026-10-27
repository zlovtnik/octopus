package com.sslproxy.coordinator.kafka

import cats.data.EitherT
import cats.effect.{IO, Ref}
import com.sslproxy.coordinator.domain.{BrokerRecordMetadata, DatabaseError, IngestionInvariantViolation}
import com.sslproxy.coordinator.persistence.{DbResultT, ResultStore}
import com.sslproxy.coordinator.postgres.{PostgresLoad, PostgresResult}
import fs2.kafka.ConsumerRecord
import munit.CatsEffectSuite

class PostgresLoadStreamSuite extends CatsEffectSuite:
  private val good = record("good", 0L)
  private val poison = record("poison", 1L)
  private val permanent = databaseError("23514")
  private val retryable = databaseError("08006")

  test("one permanent result failure does not discard healthy peers from the same transaction"):
    for
      writes <- Ref.of[IO, List[List[String]]](Nil)
      parked <- Ref.of[IO, List[String]](Nil)
      store = new RecordingStore(records =>
        writes.update(_ :+ records.map(_._1.batchId)).as(
          if records.exists(_._1.batchId == "poison") then Left(permanent) else Right(())
        )
      )
      durable <- PostgresLoadStream.persistResults(
        List(good, poison),
        store,
        (locked, _) => parked.update(_ :+ locked.decoded.batchId)
      )
      actualWrites <- writes.get
      actualParked <- parked.get
    yield
      assertEquals(actualWrites, List(List("good", "poison"), List("good"), List("poison")))
      assertEquals(actualParked, List("poison"))
      assertEquals(durable.map(_._1.decoded.batchId), List("good"))

  test("a retryable batch failure propagates without parking any record"):
    for
      writes <- Ref.of[IO, Int](0)
      parked <- Ref.of[IO, List[String]](Nil)
      store = new RecordingStore(_ => writes.update(_ + 1).as(Left(retryable)))
      result <- PostgresLoadStream
        .persistResults(List(good, poison), store, (locked, _) => parked.update(_ :+ locked.decoded.batchId))
        .attempt
      actualWrites <- writes.get
      actualParked <- parked.get
    yield
      assert(result.isLeft)
      assertEquals(actualWrites, 1)
      assertEquals(actualParked, Nil)

  test("a DLQ failure propagates instead of acknowledging a poison result"):
    val unavailable = IllegalStateException("broker unavailable")
    val store = new RecordingStore(_ => IO.pure(Left(permanent)))
    PostgresLoadStream
      .persistResults(List(poison), store, (_, _) => IO.raiseError(unavailable))
      .attempt
      .map(result => assertEquals(result, Left(unavailable)))

  test("contradictory broker evidence fails closed without splitting or parking the batch"):
    val conflict = IngestionInvariantViolation("broker coordinate changed after ingestion")
    val error = DatabaseError.Permanent("record_load_result", RuntimeException("wrapped", conflict), "conflict")
    for
      writes <- Ref.of[IO, Int](0)
      parked <- Ref.of[IO, Int](0)
      store = new RecordingStore(_ => writes.update(_ + 1).as(Left(error)))
      result <- PostgresLoadStream
        .persistResults(List(good, poison), store, (_, _) => parked.update(_ + 1))
        .attempt
      actualWrites <- writes.get
      actualParked <- parked.get
    yield
      assert(result.isLeft)
      assertEquals(actualWrites, 1)
      assertEquals(actualParked, 0)

  test("a retryable failure during poison isolation prevents batch acknowledgment"):
    for
      writes <- Ref.of[IO, List[List[String]]](Nil)
      parked <- Ref.of[IO, Int](0)
      store = new RecordingStore(records =>
        writes.update(_ :+ records.map(_._1.batchId)).as(
          if records.size > 1 then Left(permanent) else Left(retryable)
        )
      )
      result <- PostgresLoadStream
        .persistResults(List(good, poison), store, (_, _) => parked.update(_ + 1))
        .attempt
      actualWrites <- writes.get
      actualParked <- parked.get
    yield
      assert(result.isLeft)
      assertEquals(actualWrites, List(List("good", "poison"), List("good")))
      assertEquals(actualParked, 0)

  test("successful batches keep the bounded batch write without per-record retries"):
    for
      writes <- Ref.of[IO, Int](0)
      store = new RecordingStore(_ => writes.update(_ + 1).as(Right(())))
      durable <- PostgresLoadStream.persistResults(
        List(good, poison),
        store,
        (_, _) => IO.raiseError(IllegalStateException("unexpected parking"))
      )
      actualWrites <- writes.get
    yield
      assertEquals(durable, List(good, poison))
      assertEquals(actualWrites, 1)

  private def record(batchId: String, offset: Long): (LockedBrokerRecord[PostgresLoad], PostgresResult) =
    val load = PostgresLoad("job", batchId, Some(0), "proxy.events", "sha256://payload", "0", "1", 1)
    val metadata = BrokerRecordMetadata("sync.oracle.load", 0, offset, "octopus-load-v1", 1, "a" * 64, None, "b" * 64)
    (
      LockedBrokerRecord(ConsumerRecord("sync.oracle.load", 0, offset, batchId, "{}"), load, metadata),
      PostgresResult.success("job", batchId, 1, "checksum", "2026-10-25T00:00:00Z")
    )

  private def databaseError(state: String): DatabaseError =
    val cause = java.sql.SQLException("storage failed", state)
    if state == "08006" then DatabaseError.Retryable("record_load_result", cause, "storage failed")
    else DatabaseError.Permanent("record_load_result", cause, "storage failed")

  private class RecordingStore(
    write: List[(PostgresLoad, PostgresResult, BrokerRecordMetadata)] => IO[Either[DatabaseError, Unit]]
  ) extends ResultStore[IO]:
    def recordLoadResultsWithEvidence(
      records: List[(PostgresLoad, PostgresResult, BrokerRecordMetadata)]
    ): DbResultT[IO, Unit] = EitherT(write(records))

    def recordResultWithEvidence(result: PostgresResult, metadata: BrokerRecordMetadata): DbResultT[IO, Unit] =
      EitherT.leftT[IO, Unit](DatabaseError.Permanent("unused", IllegalStateException("unused"), "unused"))

    def recordResultsWithEvidence(records: List[(PostgresResult, BrokerRecordMetadata)]): DbResultT[IO, Unit] =
      EitherT.leftT[IO, Unit](DatabaseError.Permanent("unused", IllegalStateException("unused"), "unused"))
