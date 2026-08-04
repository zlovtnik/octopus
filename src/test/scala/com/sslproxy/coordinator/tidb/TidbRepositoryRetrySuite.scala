package com.sslproxy.coordinator.tidb

import cats.effect.{Deferred,IO, Ref}
import cats.effect.std.Semaphore
import munit.CatsEffectSuite

import java.sql.SQLException
import scala.concurrent.duration.*

class TidbRepositoryRetrySuite extends CatsEffectSuite:

  test("retryTransient reruns retryable transactions until they succeed"):
    Ref.of[IO, Int](0).flatMap { attempts =>
      val transaction = attempts.updateAndGet(_ + 1).flatMap {
        case attempt if attempt < 3 =>
          IO.raiseError[String](SQLException("Deadlock found", "40001", 1213))
        case _ => IO.pure("ok")
      }

      for
        result <- TidbRepository.retryTransient("tidb.test_retry")(transaction)
        count <- attempts.get
      yield
        assertEquals(result, "ok")
        assertEquals(count, 3)
    }

  test("retryTransient does not rerun permanent failures"):
    Ref.of[IO, Int](0).flatMap { attempts =>
      val transaction =
        attempts.update(_ + 1) *> IO.raiseError[String](SQLException("invalid value", "22001", 1406))

      for
        result <- TidbRepository.retryTransient("tidb.test_permanent")(transaction).attempt
        count <- attempts.get
      yield
        assert(result.isLeft)
        assertEquals(count, 1)
    }

  test("retry backoff releases the database concurrency permit"):
    for
      firstAttempt <- Deferred[IO, Unit]
      attempts <- Ref.of[IO, Int](0)
      semaphore <- Semaphore[IO](1)
      transaction = attempts.updateAndGet(_ + 1).flatMap {
        case 1 =>
          firstAttempt.complete(()) *>
            IO.raiseError[String](SQLException("Deadlock found", "40001", 1213))
        case _ => IO.pure("ok")
      }
      fiber <- TidbRepository
        .retryTransientWithPermit("tidb.test_retry_permit", Some(semaphore))(transaction)
        .start
      _ <- firstAttempt.get
      _ <- IO.sleep(5.millis)
      availableDuringBackoff <- semaphore.available
      result <- fiber.joinWithNever
    yield
      assertEquals(availableDuringBackoff, 1L)
      assertEquals(result, "ok")
