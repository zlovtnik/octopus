package com.sslproxy.coordinator.metrics

import cats.effect.{IO, Ref}
import munit.CatsEffectSuite

import java.time.Instant
import scala.concurrent.duration.*

class MetricWorkerPoolSuite extends CatsEffectSuite:

  test("failing job does not stop other jobs") {
    for
      ran <- Ref.of[IO, List[String]](Nil)
      pool <- MetricWorkerPool.create(
        {
          case MetricJob.Peaks =>
            ran.update(_ :+ "peaks") *> IO.raiseError(RuntimeException("db down"))
          case job =>
            ran.update(_ :+ job.name)
        },
        workerCount = 2,
        jobTimeout = 2.seconds
      )
      fiber <- pool.workers.start
      _ <- pool.queue.offer(MetricJob.Peaks)
      _ <- pool.queue.offer(MetricJob.Publish)
      _ <- IO.sleep(200.millis)
      _ <- fiber.cancel
      jobs <- ran.get
    yield
      assert(jobs.contains("peaks"))
      assert(jobs.contains("publish"))
  }

  test("fillHourly covers 24 and 168 windows densely") {
    val now = Instant.parse("2026-10-08T12:30:00Z")
    val points = List(ThroughputPoint("2026-10-08T11:00:00Z", 7L))
    val day = StatsMaterializer.fillHourly(points, now, 24)
    val week = StatsMaterializer.fillHourly(points, now, 168)
    assertEquals(day.size, 24)
    assertEquals(week.size, 168)
    assertEquals(day.last.bucketStart, "2026-10-08T11:00:00Z")
    assertEquals(day.last.records, 7L)
    assertEquals(week.last.bucketStart, "2026-10-08T11:00:00Z")
    assert(day.forall(_.records >= 0L))
  }
