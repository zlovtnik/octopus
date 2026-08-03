package com.sslproxy.coordinator.processor

import cats.effect.kernel.{Clock, Temporal}
import cats.syntax.all.*
import fs2.Stream

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

trait Processor[F[_]]:
  def descriptor: ProcessorDescriptor
  def run: Stream[F, Unit]

final case class ProcessorDescriptor(
    id: ProcessorId,
    mode: ProcessorMode,
    dependencies: Set[ProcessorId]
)

final case class Lease(
    scope: String,
    ownerId: String,
    token: String,
    fence: Long,
    expiresAt: Instant
)

object Lease:
  def expiration[F[_]: Clock: cats.Functor](ttl: FiniteDuration): F[Instant] =
    Clock[F].realTimeInstant.map(_.plusMillis(ttl.toMillis))

final case class RetryPolicy(
    baseDelay: FiniteDuration,
    maxDelay: FiniteDuration,
    maxAttempts: Option[Int]
):
  require(baseDelay.length > 0L, "base delay must be positive")
  require(maxDelay >= baseDelay, "max delay must be at least base delay")
  require(maxAttempts.forall(_ > 0), "max attempts must be positive when configured")

  def delay(id: ProcessorId, attempt: Int): FiniteDuration =
    val safeAttempt = attempt.max(1)
    val exponent = (safeAttempt - 1).min(30)
    val uncapped = BigInt(baseDelay.toMillis) * BigInt(2).pow(exponent)
    val capped = uncapped.min(BigInt(maxDelay.toMillis)).toLong
    val bucket = Math.floorMod(id.value.hashCode * 31 + safeAttempt, 401)
    val jitter = (bucket.toDouble - 200.0d) / 1000.0d
    FiniteDuration(Math.max(1L, Math.round(capped.toDouble * (1.0d + jitter))), scala.concurrent.duration.MILLISECONDS)

  def permits(attempt: Int): Boolean = maxAttempts.forall(attempt <= _)

object ProcessorRunner:
  def continuous[F[_]: Temporal](processor: Processor[F], retryPolicy: RetryPolicy): Stream[F, Unit] =
    def loop(attempt: Int): Stream[F, Unit] =
      processor.run.handleErrorWith { error =>
        if retryPolicy.permits(attempt) then
          Stream.sleep_[F](retryPolicy.delay(processor.descriptor.id, attempt)) ++ loop(attempt + 1)
        else Stream.raiseError[F](error)
      }

    loop(1)

  def periodic[F[_]: Temporal](
      descriptor0: ProcessorDescriptor,
      interval: FiniteDuration
  )(operation: F[Unit]): Processor[F] =
    new Processor[F]:
      val descriptor: ProcessorDescriptor = descriptor0
      val run: Stream[F, Unit] =
        Stream.repeatEval(Clock[F].realTimeInstant.void *> operation).metered(interval)
