package com.sslproxy.coordinator.postgres

import cats.effect.{Deferred,IO, Ref}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import munit.CatsEffectSuite

import java.sql.SQLException
class PostgresRepositoryRetrySuite extends CatsEffectSuite:

  test("retryTransient reruns retryable transactions until they succeed"):
    Ref.of[IO, Int](0).flatMap { attempts =>
      val transaction = attempts.updateAndGet(_ + 1).flatMap {
        case attempt if attempt < 3 =>
          IO.raiseError[String](SQLException("Deadlock found", "40001", 1213))
        case _ => IO.pure("ok")
      }

      for
        result <- PostgresRepository.retryTransient("postgres.test_retry")(transaction)
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
        result <- PostgresRepository.retryTransient("postgres.test_permanent")(transaction).attempt
        count <- attempts.get
      yield
        assert(result.isLeft)
        assertEquals(count, 1)
    }

  test("retry backoff releases the database concurrency permit"):
    for
      firstAttempt <- Deferred[IO, Unit]
      completed <- Deferred[IO, Unit]
      attempts <- Ref.of[IO, Int](0)
      semaphore <- Semaphore[IO](1)
      transaction = attempts.updateAndGet(_ + 1).flatMap {
        case 1 =>
          firstAttempt.complete(()) *>
            IO.raiseError[String](SQLException("Deadlock found", "40001", 1213))
        case _ => IO.pure("ok")
      }
      fiber <- PostgresRepository
        .retryTransientWithPermit("postgres.test_retry_permit", Some(semaphore))(transaction)
        .guarantee(completed.complete(()).void)
        .start
      _ <- firstAttempt.get
      observeReleasedPermit =
        def loop: IO[Boolean] =
          semaphore.available.flatMap {
            case available if available >= 1L => IO.pure(true)
            case _ =>
              completed.tryGet.flatMap {
                case Some(_) => IO.pure(false)
                case None    => IO.cede *> loop
              }
          }
        loop
      outcome <- (observeReleasedPermit, fiber.joinWithNever).tupled.guarantee(fiber.cancel)
    yield
      assert(outcome._1, "the permit must be observable while retry backoff is in progress")
      assertEquals(outcome._2, "ok")
