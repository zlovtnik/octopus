package com.sslproxy.coordinator.kafka

import cats.effect.IO
import fs2.{Chunk, Pull, Stream}
import scala.concurrent.duration.FiniteDuration

private[kafka] object BoundedBatching:
  /** Oversized records are emitted alone so the durable consumer can park them.
    * Only the current batch plus the upstream fetch chunk are retained.
    */
  def groupWithin[A](source: Stream[IO, A], count: Int, bytes: Int, window: FiniteDuration)(
    sizeOf: A => Long
  ): Stream[IO, Chunk[A]] =
    require(count > 0 && bytes > 0, "batch bounds must be positive")
    source.pull.timed { initial =>
      def emit(values: Vector[A]): Pull[IO, Chunk[A], Unit] =
        if values.isEmpty then Pull.done else Pull.output1(Chunk.from(values))

      def consume(chunk: Chunk[A], index: Int, next: Pull.Timed[IO, A], values: Vector[A], weight: Long)
          : Pull[IO, Chunk[A], Unit] =
        if index == chunk.size then loop(next, values, weight)
        else
          val value = chunk(index)
          val size = sizeOf(value).max(0L)
          if values.nonEmpty && weight + size > bytes then
            emit(values) >> next.timeout(window) >> consume(chunk, index, next, Vector.empty, 0L)
          else
            val updated = values :+ value
            if updated.size >= count || weight + size >= bytes then
              emit(updated) >> next.timeout(window) >> consume(chunk, index + 1, next, Vector.empty, 0L)
            else consume(chunk, index + 1, next, updated, weight + size)

      def loop(next: Pull.Timed[IO, A], values: Vector[A], weight: Long): Pull[IO, Chunk[A], Unit] =
        next.uncons.flatMap {
          case None => emit(values)
          case Some((Left(_), tail)) => emit(values) >> tail.timeout(window) >> loop(tail, Vector.empty, 0L)
          case Some((Right(chunk), tail)) => consume(chunk, 0, tail, values, weight)
        }

      initial.timeout(window) >> loop(initial, Vector.empty, 0L)
    }.stream
