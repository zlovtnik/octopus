package com.sslproxy.coordinator.tidb

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite

import java.sql.SQLException

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
