package com.sslproxy.coordinator

import cats.effect.{Deferred, IO, Resource}
import com.sslproxy.coordinator.config.RuntimeConfig
import com.sslproxy.coordinator.processor.ProcessorId
import com.sslproxy.coordinator.wiring.{DatabaseModule, RuntimeStreams}
import fs2.Stream
import munit.CatsEffectSuite

import scala.concurrent.ExecutionContextExecutorService
import scala.concurrent.duration.*

class MainSuite extends CatsEffectSuite:

  test("database worker permits use the configured connection reserve"):
    assertEquals(DatabaseModule.dbWorkerPermits(20, 2), 18L)
    assertEquals(DatabaseModule.dbWorkerPermits(20, 5), 15L)

  test("database worker permits retain one worker for small pools"):
    assertEquals(DatabaseModule.dbWorkerPermits(2, 1), 1L)
    assertEquals(DatabaseModule.dbWorkerPermits(1, 1), 1L)

  test("database executor keeps daemon thread naming and shuts down on release"):
    DatabaseModule.blockingExecutionContext(2).use { ec =>
      IO {
        val thread = Thread.currentThread()
        assertEquals(thread.getName, "doobie-postgres-pool")
        assert(thread.isDaemon)
        assert(!ec.isShutdown)
        ec
      }.evalOn(ec)
    }.map(ec => assert(ec.isShutdown))

  test("database executor shuts down when later resource acquisition fails"):
    for
      acquired <- Deferred[IO, ExecutionContextExecutorService]
      failure = new IllegalStateException("later acquisition failed")
      outcome <- DatabaseModule.blockingExecutionContext(2)
        .evalTap(ec => acquired.complete(ec).void)
        .flatMap(_ => Resource.eval(IO.raiseError[Unit](failure)))
        .use(_ => IO.unit)
        .attempt
      ec <- acquired.get
    yield
      assertEquals(outcome, Left(failure))
      assert(ec.isShutdown)

  test("database executor shuts down when application use is cancelled"):
    for
      acquired <- Deferred[IO, ExecutionContextExecutorService]
      fiber <- DatabaseModule.blockingExecutionContext(2).use { ec =>
        acquired.complete(ec) *> IO.never[Unit]
      }.start
      _ <- (for
        ec <- acquired.get
        _ <- fiber.cancel
        _ <- IO(assert(ec.isShutdown))
      yield ()).guarantee(fiber.cancel)
    yield ()

  test("active runtime starts supervised, processor-support, and required streams"):
    RuntimeStreams
      .enabledRuntimeStreams(
        RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
        Stream.emit("supervised").covary[IO],
        Stream.emit("support").covary[IO],
        Stream.emit("required").covary[IO]
      )
      .take(3)
      .compile
      .toList
      .map(values => assertEquals(values.toSet, Set("supervised", "support", "required")))

  test("consumer-only runtime excludes processor support"):
    val processorOnly = RuntimeStreams.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = true, consumersEnabled = false),
      Stream.emit("supervised").covary[IO],
      Stream.emit("support").covary[IO],
      Stream.emit("required").covary[IO]
    )
    val consumerOnly = RuntimeStreams.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = false, consumersEnabled = true),
      Stream.emit("supervised").covary[IO],
      Stream.emit("support").covary[IO],
      Stream.emit("required").covary[IO]
    )
    val disabled = RuntimeStreams.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = false, consumersEnabled = false),
      Stream.emit("supervised").covary[IO],
      Stream.emit("support").covary[IO],
      Stream.emit("required").covary[IO]
    )

    for
      processors <- processorOnly.take(3).compile.toList
      consumers <- consumerOnly.take(2).compile.toList
      disabledOutcome <- IO.race(IO.sleep(50.millis), disabled.compile.drain)
    yield
      assertEquals(processors.toSet, Set("supervised", "support", "required"))
      assertEquals(consumers.toSet, Set("supervised", "required"))
      assertEquals(disabledOutcome, Left(()))

  test("consumer runtime supervises locked and auxiliary Kafka consumers"):
    val enabled = RuntimeStreams.runtimeConsumerProcessorIds(
      RuntimeConfig(processorsEnabled = false, consumersEnabled = true)
    )

    assertEquals(enabled, ProcessorId.kafkaConsumers)
    assert(enabled.contains(ProcessorId.SyncScanIngestion))
    assert(enabled.contains(ProcessorId.SyncLoadConsumer))
    assert(enabled.contains(ProcessorId.SyncResultConsumer))
    assertEquals(
      RuntimeStreams.runtimeConsumerProcessorIds(
        RuntimeConfig(processorsEnabled = true, consumersEnabled = false)
      ),
      Set.empty
    )
