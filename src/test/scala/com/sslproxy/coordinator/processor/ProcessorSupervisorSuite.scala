package com.sslproxy.coordinator.processor

import cats.data.EitherT
import cats.effect.{IO, Ref}
import com.sslproxy.coordinator.config.ProcessorConfig
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.persistence.{DatabaseOperationException, ProcessorStateStore}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
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

  test("persisted processor lifecycle is exported as metrics") {
    val config = ProcessorConfig(Nil, 1L, 10L)
    for
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      metrics = CoordinatorMetrics()
      _ <- ProcessorSupervisor.create(config, RecordingProcessorStateStore(events), Some(metrics))
      scrape = metrics.scrape
    yield
      assert(scrape.contains("coordinator_processor_lifecycle_value"))
      assert(scrape.contains(s"processor=\"${ProcessorId.EventRetention.value}\""))
      assert(scrape.contains("state=\"disabled\""))
      assert(scrape.contains("coordinator_processor_restart_count_value"))
  }

  test("enabled externally owned processors are included in readiness") {
    val external = ProcessorId.EmbeddingLeaseRecovery
    val config = ProcessorConfig(List(external.value), 1L, 10L)

    ProcessorSupervisor.create(config).flatMap(_.readiness.statuses).map { statuses =>
      assertEquals(statuses(external).lifecycle, ProcessorLifecycle.Starting)
      assert(statuses.keySet.contains(external))
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
        awaitLifecycle(supervisor, id, ProcessorLifecycle.FailedTerminal)
          .map { statuses =>
            assertEquals(statuses(id).lastError, Some("invalid retention policy"))
          }
          .guarantee(fiber.cancel)
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
        awaitLifecycle(supervisor, id, ProcessorLifecycle.FailedTerminal)
          .map { statuses =>
            assertEquals(statuses(id).lastError, Some("invalid startup configuration"))
          }
          .guarantee(fiber.cancel)
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

  test("runtime-enabled consumers do not require their producer workloads") {
    val config = ProcessorConfig(Nil, 1L, 10L)
    val runtimeEnabled = Set(
      ProcessorId.SyncScanIngestion,
      ProcessorId.SyncLoadConsumer,
      ProcessorId.SyncResultConsumer,
      ProcessorId.PayloadAuditIngestion
    )

    ProcessorSupervisor
      .create(config, RecordingProcessorStateStore.volatile, None, runtimeEnabled)
      .flatMap(_.readiness.statuses)
      .map { statuses =>
        runtimeEnabled.foreach { id =>
          assertEquals(statuses(id).lifecycle, ProcessorLifecycle.Starting)
        }
      }
  }

  test("retryable database operation failures enter backoff") {
    val id = ProcessorId.SyncScanIngestion
    val config = ProcessorConfig(List(id.value), 100L, 100L)
    val failure = DatabaseOperationException(
      DatabaseError.Retryable(
        "payload_audit.record_scan_requests",
        new java.sql.SQLTransientException("connection unavailable"),
        "connection unavailable"
      )
    )

    ProcessorSupervisor.create(config).flatMap { supervisor =>
      val workload = ProcessorWorkload(id, Stream.raiseError[IO](failure))
      supervisor.run(List(workload)).compile.drain.start.flatMap { fiber =>
        awaitLifecycle(supervisor, id, ProcessorLifecycle.BackingOff)
          .map { statuses =>
            assertEquals(statuses(id).lifecycle, ProcessorLifecycle.BackingOff)
          }
          .guarantee(fiber.cancel)
      }
    }
  }

  test("processor runs and terminal state are persisted") {
    val id = ProcessorId.SyncScanIngestion
    val config = ProcessorConfig(List(id.value), 1L, 10L)
    for
      events <- Ref.of[IO, Vector[String]](Vector.empty)
      store = RecordingProcessorStateStore(events)
      supervisor <- ProcessorSupervisor.create(config, store)
      _ <- supervisor
        .run(
          List(
            ProcessorWorkload(
              id,
              Stream.raiseError[IO](TerminalProcessorError("invalid record"))
            )
          )
        )
        .compile
        .drain
        .start
        .flatMap { fiber =>
          (for
            _ <- awaitLifecycle(supervisor, id, ProcessorLifecycle.FailedTerminal)
            recorded <- events.get
          yield
            assert(recorded.exists(_ == s"state:${id.value}:failed_terminal"))
            assert(recorded.exists(_.startsWith(s"start:${id.value}:")))
            assert(recorded.exists(_.contains(":failed_terminal")))
          ).guarantee(fiber.cancel)
        }
    yield ()
  }

  test("start-run persistence failures stay local and retry") {
    val id = ProcessorId.SyncScanIngestion
    val config = ProcessorConfig(List(id.value), 1L, 10L)
    for
      attempts <- Ref.of[IO, Int](0)
      store = new StartRunOnceFailingStore(attempts)
      supervisor <- ProcessorSupervisor.create(config, store)
      _ <- supervisor
        .run(
          List(
            ProcessorWorkload(
              id,
              Stream.raiseError[IO](TerminalProcessorError("invalid record"))
            )
          )
        )
        .compile
        .drain
        .start
        .flatMap { fiber =>
          (for
            statuses <- awaitLifecycle(supervisor, id, ProcessorLifecycle.FailedTerminal)
            startAttempts <- attempts.get
          yield
            assertEquals(statuses(id).lastError, Some("invalid record"))
            assertEquals(startAttempts, 2)
          ).guarantee(fiber.cancel)
        }
    yield ()
  }

  private def awaitLifecycle(
    supervisor: ProcessorSupervisor,
    id: ProcessorId,
    expected: ProcessorLifecycle
  ): IO[Map[ProcessorId, ProcessorStatus]] =
    supervisor.readiness.statuses
      .flatMap { statuses =>
        if statuses(id).lifecycle == expected then IO.pure(statuses)
        else IO.sleep(10.millis) *> awaitLifecycle(supervisor, id, expected)
      }
      .timeout(2.seconds)

  private final class RecordingProcessorStateStore(
    events: Ref[IO, Vector[String]]
  ) extends ProcessorStateStore[IO]:
    def load = EitherT.rightT[IO, DatabaseError](Map.empty[ProcessorId, ProcessorStatus])

    def persist(
      id: ProcessorId,
      status: ProcessorStatus,
      observedAt: java.time.Instant
    ) = EitherT.liftF(events.update(_ :+ s"state:${id.value}:${status.lifecycle.value}"))

    def startRun(id: ProcessorId, runId: String, startedAt: java.time.Instant) =
      EitherT.liftF(events.update(_ :+ s"start:${id.value}:$runId"))

    def finishRun(
      runId: String,
      status: ProcessorRunStatus,
      errorClass: Option[String],
      errorText: Option[String],
      finishedAt: java.time.Instant
    ) = EitherT.liftF(events.update(_ :+ s"finish:$runId:${status.value}"))

  private object RecordingProcessorStateStore:
    val volatile: ProcessorStateStore[IO] = new ProcessorStateStore[IO]:
      def load = EitherT.rightT[IO, DatabaseError](Map.empty[ProcessorId, ProcessorStatus])
      def persist(id: ProcessorId, status: ProcessorStatus, observedAt: java.time.Instant) =
        EitherT.rightT[IO, DatabaseError](())
      def startRun(id: ProcessorId, runId: String, startedAt: java.time.Instant) =
        EitherT.rightT[IO, DatabaseError](())
      def finishRun(
        runId: String,
        status: ProcessorRunStatus,
        errorClass: Option[String],
        errorText: Option[String],
        finishedAt: java.time.Instant
      ) = EitherT.rightT[IO, DatabaseError](())

  private final class StartRunOnceFailingStore(attempts: Ref[IO, Int]) extends ProcessorStateStore[IO]:
    def load = EitherT.rightT[IO, DatabaseError](Map.empty[ProcessorId, ProcessorStatus])

    def persist(id: ProcessorId, status: ProcessorStatus, observedAt: java.time.Instant) =
      EitherT.rightT[IO, DatabaseError](())

    def startRun(id: ProcessorId, runId: String, startedAt: java.time.Instant) =
      EitherT(attempts.modify { count =>
        val result =
          if count == 0 then
            Left(
              DatabaseError.Retryable(
                "processor.start_run",
                new java.sql.SQLTransientException("connection timeout"),
                "connection timeout"
              )
            )
          else Right(())
        (count + 1, result)
      })

    def finishRun(
      runId: String,
      status: ProcessorRunStatus,
      errorClass: Option[String],
      errorText: Option[String],
      finishedAt: java.time.Instant
    ) = EitherT.rightT[IO, DatabaseError](())
