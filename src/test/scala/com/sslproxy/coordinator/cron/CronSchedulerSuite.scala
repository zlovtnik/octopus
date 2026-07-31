package com.sslproxy.coordinator.cron

import cats.effect.{IO, Ref}
import com.sslproxy.coordinator.dispatch.BatchDispatchService.DispatchResult
import munit.CatsEffectSuite

class CronSchedulerSuite extends CatsEffectSuite:

  test("drainBatch stops after the first unavailable dispatch"):
    Ref.of[IO, List[DispatchResult]](
      List(
        DispatchResult.Dispatched,
        DispatchResult.Dispatched,
        DispatchResult.NoWork,
        DispatchResult.Dispatched
      )
    ).flatMap { remaining =>
      val dispatch = remaining.modify {
        case result :: tail => tail -> result
        case Nil            => Nil -> DispatchResult.NoWork
      }

      for
        dispatched <- CronScheduler.drainBatch(10)(() => dispatch)
        notAttempted <- remaining.get
      yield
        assertEquals(dispatched, 2)
        assertEquals(notAttempted, List(DispatchResult.Dispatched))
    }

  test("drainBatch never exceeds its configured batch size"):
    Ref.of[IO, Int](0).flatMap { attempts =>
      val dispatch = attempts.update(_ + 1).as(DispatchResult.Dispatched)

      for
        dispatched <- CronScheduler.drainBatch(3)(() => dispatch)
        attemptCount <- attempts.get
      yield
        assertEquals(dispatched, 3)
        assertEquals(attemptCount, 3)
    }

  test("drainBatch continues after a non-terminal dispatch result"):
    Ref.of[IO, List[DispatchResult]](
      List(
        DispatchResult.ContinueDraining,
        DispatchResult.Dispatched,
        DispatchResult.NoWork
      )
    ).flatMap { remaining =>
      val dispatch = remaining.modify {
        case result :: tail => tail -> result
        case Nil            => Nil -> DispatchResult.NoWork
      }

      for
        dispatched <- CronScheduler.drainBatch(10)(() => dispatch)
        notAttempted <- remaining.get
      yield
        assertEquals(dispatched, 1)
        assertEquals(notAttempted, Nil)
    }
