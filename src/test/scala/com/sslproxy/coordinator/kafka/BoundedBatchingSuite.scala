package com.sslproxy.coordinator.kafka

import cats.effect.IO
import fs2.Stream
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class BoundedBatchingSuite extends CatsEffectSuite:
  test("byte and count limits preserve order including an oversized record") {
    val input = List(3L, 4L, 5L, 11L, 1L, 1L, 1L, 1L)
    BoundedBatching.groupWithin(Stream.emits(input).covary[IO], 3, 10, 1.second)(identity)
      .compile.toList.map { chunks =>
        assertEquals(chunks.map(_.toList), List(List(3L, 4L), List(5L), List(11L), List(1L, 1L, 1L), List(1L)))
        assertEquals(chunks.flatMap(_.toList), input)
      }
  }

  test("a sparse batch flushes on time while upstream remains open") {
    val source = Stream.emit(1L).covary[IO] ++ Stream.never[IO]
    BoundedBatching.groupWithin(source, 500, 1024, 30.millis)(identity)
      .take(1).compile.toList.timeout(2.seconds).map { chunks =>
        assertEquals(chunks.map(_.toList), List(List(1L)))
      }
  }
