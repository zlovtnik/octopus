package com.sslproxy.coordinator.dispatch

import cats.effect.IO
import com.sslproxy.coordinator.config.BackpressureConfig
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import fs2.concurrent.SignallingRef

final class BackpressureService private (
  cfg: BackpressureConfig,
  ingestBatchSize: Int,
  pendingLedgerCount: IO[Either[DatabaseError, Long]],
  metrics: CoordinatorMetrics,
  consumerSuspended: SignallingRef[IO, Boolean]
):
  import BackpressureService.log

  def budget: Long =
    ingestBatchSize.toLong * cfg.budgetMultiplier

  def recoveryThreshold: Long =
    budget / 2

  def isConsumerSuspended: IO[Boolean] =
    consumerSuspended.get

  def awaitConsumerPermit: IO[Unit] =
    consumerSuspended.discrete.dropWhile(identity).head.compile.drain

  def checkAndAct: IO[Long] =
    pendingLedgerCount.flatMap {
      case Left(err) =>
        val sanitized = com.sslproxy.coordinator.util.ErrorSanitizer.sanitize(err.message)
        IO(
          log.warn(
            "backpressure",
            "status" -> "pending_count_failed",
            "operation" -> err.operation,
            "error" -> sanitized
          )
        ) *>
          consumerSuspended.get.flatMap { suspended =>
            IO(metrics.recordBackpressureActive(suspended))
              .as(if suspended then budget else 0L)
          }

      case Right(count) =>
        IO(metrics.recordPendingLedgerCount(count)) *>
          evaluate(count).as(count)
    }

  private def evaluate(pendingCount: Long): IO[Unit] =
    val b = budget
    val rt = recoveryThreshold

    consumerSuspended.get.flatMap { suspended =>
      if pendingCount >= b then
        if !suspended then
          consumerSuspended.set(true) *>
            IO(
              log.info(
                "backpressure",
                "status" -> "throttled",
                "pending_count" -> pendingCount.toString,
                "budget" -> b.toString,
                "multiplier" -> cfg.budgetMultiplier.toString
              )
            )
        else IO.unit
      else if pendingCount <= rt && suspended then
        consumerSuspended.set(false) *>
          IO(
            log.info(
              "backpressure",
              "status" -> "recovered",
              "pending_count" -> pendingCount.toString,
              "recovery_threshold" -> rt.toString
            )
          )
      else IO.unit
    } *> consumerSuspended.get.flatMap(s => IO(metrics.recordBackpressureActive(s)))

object BackpressureService:
  private val log = StructuredLogger(getClass)

  def create(
    cfg: BackpressureConfig,
    ingestBatchSize: Int,
    pendingLedgerCount: IO[Either[DatabaseError, Long]],
    metrics: CoordinatorMetrics
  ): IO[BackpressureService] =
    SignallingRef[IO, Boolean](false).map { state =>
      new BackpressureService(cfg, ingestBatchSize, pendingLedgerCount, metrics, state)
    }
