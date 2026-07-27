package com.sslproxy.coordinator.cron

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite

class CronSchedulerSuite extends CatsEffectSuite:

  test("drainBatch stops after the first unavailable dispatch"):
    Ref.of[IO, List[Boolean]](List(true, true, false, true)).flatMap { remaining =>
      val dispatch = remaining.modify {
        case result :: tail => tail -> result
        case Nil            => Nil -> false
      }

      for
        dispatched <- CronScheduler.drainBatch(10)(() => dispatch)
        notAttempted <- remaining.get
      yield
        assertEquals(dispatched, 2)
        assertEquals(notAttempted, List(true))
    }

  test("drainBatch never exceeds its configured batch size"):
    Ref.of[IO, Int](0).flatMap { attempts =>
      val dispatch = attempts.update(_ + 1).as(true)

      for
        dispatched <- CronScheduler.drainBatch(3)(() => dispatch)
        attemptCount <- attempts.get
      yield
        assertEquals(dispatched, 3)
        assertEquals(attemptCount, 3)
    }
