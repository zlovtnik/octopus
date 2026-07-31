package com.sslproxy.coordinator

import cats.effect.IO
import com.sslproxy.coordinator.config.RuntimeConfig
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

  test("runtime flags start both processor and consumer lanes"):
    Main.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      Stream.emit("processor").covary[IO],
      Stream.emit("consumer").covary[IO]
    ).compile.toList.map(values => assertEquals(values.toSet, Set("processor", "consumer")))

  test("runtime flags do not start disabled lanes"):
    val processorOnly = Main.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = true, consumersEnabled = false),
      Stream.emit("processor").covary[IO],
      Stream.emit("consumer").covary[IO]
    )
    val consumerOnly = Main.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = false, consumersEnabled = true),
      Stream.emit("processor").covary[IO],
      Stream.emit("consumer").covary[IO]
    )
    val disabled = Main.enabledRuntimeStreams(
      RuntimeConfig(processorsEnabled = false, consumersEnabled = false),
      Stream.emit("processor").covary[IO],
      Stream.emit("consumer").covary[IO]
    )

    for
      processors <- processorOnly.compile.toList
      consumers <- consumerOnly.compile.toList
      disabledFiber <- disabled.compile.drain.start
      _ <- IO.sleep(50.millis)
      disabledOutcome <- disabledFiber.poll
      _ <- disabledFiber.cancel
    yield
      assertEquals(processors, List("processor"))
      assertEquals(consumers, List("consumer"))
      assertEquals(disabledOutcome, None)
