package com.sslproxy.coordinator.processor

import cats.effect.IO
import com.sslproxy.coordinator.config.ProcessorConfig
import fs2.Stream
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class ProcessorSupervisorSuite extends CatsEffectSuite:
  test("unknown enabled processor fails before workload startup") {
    val config = ProcessorConfig(List("not-a-processor"), 1L, 10L)
    ProcessorSupervisor.create(config).attempt.map { result =>
      assert(result.isLeft)
    }
  }

  test("enabled processor without a workload fails closed") {
    val config = ProcessorConfig(List(ProcessorId.SyncScanIngestion.value), 1L, 10L)
    ProcessorSupervisor.create(config).flatMap { supervisor =>
      supervisor.run(Nil).compile.drain.attempt.map { result =>
        assert(result.isLeft)
      }
    }
  }

  test("disabled processors are reported disabled") {
    val config = ProcessorConfig(Nil, 1L, 10L)
    ProcessorSupervisor.create(config).flatMap(_.readiness.statuses).map { statuses =>
      assertEquals(statuses.keySet, ProcessorId.octopusOwned.toSet)
      assert(statuses.values.forall(_.lifecycle == ProcessorLifecycle.Disabled))
    }
  }

  test("terminal failure is isolated and visible in readiness") {
    val id = ProcessorId.SyncScanIngestion
    val config = ProcessorConfig(List(id.value), 1L, 10L)
    ProcessorSupervisor.create(config).flatMap { supervisor =>
      val workload = ProcessorWorkload(
        id,
        Stream.raiseError[IO](TerminalProcessorError("invalid retention policy"))
      )
      supervisor.run(List(workload)).compile.drain.start.flatMap { fiber =>
        awaitLifecycle(supervisor, id, ProcessorLifecycle.FailedTerminal).map { statuses =>
            assertEquals(statuses(id).lastError, Some("invalid retention policy"))
        }.guarantee(fiber.cancel)
      }
    }
  }

  test("terminal startup failure is isolated and visible in readiness") {
    val id = ProcessorId.SyncScanIngestion
    val config = ProcessorConfig(List(id.value), 1L, 10L)
    ProcessorSupervisor.create(config).flatMap { supervisor =>
      val workload = ProcessorWorkload(
        id,
        Stream.never[IO],
        startup = IO.raiseError(TerminalProcessorError("invalid startup configuration"))
      )
      supervisor.run(List(workload)).compile.drain.start.flatMap { fiber =>
        awaitLifecycle(supervisor, id, ProcessorLifecycle.FailedTerminal).map { statuses =>
            assertEquals(statuses(id).lastError, Some("invalid startup configuration"))
        }.guarantee(fiber.cancel)
      }
    }
  }

  test("retry delay is exponentially bounded with bounded jitter") {
    assertEquals(ProcessorSupervisor.retryDelay(100L, 1000L, 1, 0.0d), 100.millis)
    assertEquals(ProcessorSupervisor.retryDelay(100L, 1000L, 4, 0.0d), 800.millis)
    assertEquals(ProcessorSupervisor.retryDelay(100L, 1000L, 8, 0.0d), 1000.millis)
    assertEquals(ProcessorSupervisor.retryDelay(100L, 1000L, 1, -1.0d), 80.millis)
    assertEquals(ProcessorSupervisor.retryDelay(100L, 1000L, 1, 1.0d), 120.millis)
  }

  test("enabled processor with a disabled Octopus dependency fails closed") {
    val config = ProcessorConfig(List(ProcessorId.EventRetention.value), 1L, 10L)
    ProcessorSupervisor.create(config).attempt.map { result =>
      assert(result.isLeft)
      assert(result.swap.exists(_.getMessage.contains("event-retention->sync-result-consumer")))
    }
  }

  private def awaitLifecycle(
      supervisor: ProcessorSupervisor,
      id: ProcessorId,
      expected: ProcessorLifecycle
  ): IO[Map[ProcessorId, ProcessorStatus]] =
    supervisor.readiness.statuses.flatMap { statuses =>
      if statuses(id).lifecycle == expected then IO.pure(statuses)
      else IO.sleep(10.millis) *> awaitLifecycle(supervisor, id, expected)
    }.timeout(2.seconds)
