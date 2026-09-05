package com.sslproxy.coordinator.processor

import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.persistence.{DatabaseOperationException, MaintenanceStore, orRaise}

import java.util.UUID
import scala.concurrent.duration.*

/** Executes one processor tick only while this runtime owns the persisted fence. */
final class FencedWorkRunner[F[_]: Async](
  store: MaintenanceStore[F],
  ownerId: String
):
  private val ResourceType = "processor"

  def runOnce[A](
    processorId: ProcessorId,
    leaseTtl: FiniteDuration
  )(operation: Lease => F[A]): F[Option[A]] =
    val ttlSeconds = leaseTtl.toSeconds.max(1L).min(Int.MaxValue.toLong).toInt
    Async[F].delay(UUID.randomUUID().toString).flatMap { token =>
      store
        .claimLease(
          ResourceType,
          processorId.value,
          ownerId,
          token,
          ttlSeconds
        )
        .orRaise
        .flatMap {
          case None => Async[F].pure(None)
          case Some(lease) => runClaimed(processorId, lease, ttlSeconds, operation).map(Some(_))
        }
    }

  private def runClaimed[A](
    processorId: ProcessorId,
    lease: Lease,
    ttlSeconds: Int,
    operation: Lease => F[A]
  ): F[A] =
    val renewalInterval = (ttlSeconds.toLong.seconds / 3).max(1.second)
    val renewalLoop = (
      Async[F].sleep(renewalInterval) *>
        store
          .renewLease(
            ResourceType,
            processorId.value,
            lease,
            ttlSeconds
          )
          .orRaise
          .flatMap(updated => requireOne(updated, processorId, "renew"))
    ).foreverM

    Async[F]
      .race(operation(lease), renewalLoop)
      .flatMap {
        case Left(value) => Async[F].pure(value)
        case Right(_) =>
          Async[F].raiseError(
            IllegalStateException(
              s"processor lease renewal terminated unexpectedly for ${processorId.value}"
            )
          )
      }
      .guarantee(
        store.releaseLease(ResourceType, processorId.value, lease).orRaise.void
      )

  private def requireOne(updated: Int, processorId: ProcessorId, operation: String): F[Unit] =
    if updated == 1 then Async[F].unit
    else
      val message = s"$operation processor lease for ${processorId.value} affected $updated rows"
      val cause = IllegalStateException(message)
      val error =
        if updated == 0 then
          DatabaseOperationException(DatabaseError.Retryable("processor.renew_lease", cause, message))
        else cause
      Async[F].raiseError(error)
