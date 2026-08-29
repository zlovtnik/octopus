package com.sslproxy.coordinator

import cats.effect.IO
import com.sslproxy.coordinator.config.RuntimeConfig
import com.sslproxy.coordinator.processor.ProcessorId
import fs2.Stream
import munit.CatsEffectSuite

import scala.concurrent.duration.*

class MainSuite extends CatsEffectSuite:

  test("database worker permits use the configured connection reserve"):
    assertEquals(Main.dbWorkerPermits(20, 2), 18L)
    assertEquals(Main.dbWorkerPermits(20, 5), 15L)

  test("database worker permits retain one worker for small pools"):
    assertEquals(Main.dbWorkerPermits(2, 1), 1L)
    assertEquals(Main.dbWorkerPermits(1, 1), 1L)

  test("active runtime starts supervised, processor-support, and required streams"):
    Main.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      Stream.emit("supervised").covary[IO],
      Stream.emit("support").covary[IO],
      Stream.emit("required").covary[IO]
    ).take(3).compile.toList.map(values =>
      assertEquals(values.toSet, Set("supervised", "support", "required"))
    )

  test("consumer-only runtime excludes processor support"):
    val processorOnly = Main.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = true, consumersEnabled = false),
      Stream.emit("supervised").covary[IO],
      Stream.emit("support").covary[IO],
      Stream.emit("required").covary[IO]
    )
    val consumerOnly = Main.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = false, consumersEnabled = true),
      Stream.emit("supervised").covary[IO],
      Stream.emit("support").covary[IO],
      Stream.emit("required").covary[IO]
    )
    val disabled = Main.enabledRuntimeStreams(
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
    val enabled = Main.runtimeConsumerProcessorIds(
      RuntimeConfig(processorsEnabled = false, consumersEnabled = true)
    )

    assertEquals(enabled, ProcessorId.kafkaConsumers)
    assert(enabled.contains(ProcessorId.SyncScanIngestion))
    assert(enabled.contains(ProcessorId.SyncLoadConsumer))
    assert(enabled.contains(ProcessorId.SyncResultConsumer))
    assert(enabled.contains(ProcessorId.PayloadAuditIngestion))
    assertEquals(
      Main.runtimeConsumerProcessorIds(
        RuntimeConfig(processorsEnabled = true, consumersEnabled = false)
      ),
      Set.empty
    )
