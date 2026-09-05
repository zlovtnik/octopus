package com.sslproxy.coordinator.processor

import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.persistence.{DatabaseOperationException, MaintenanceStore, orRaise}

import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.duration.*

final case class RetentionCounts(selected: Long, archived: Long, deleted: Long)

final class FencedRetentionRunner[F[_]: Async](
  store: MaintenanceStore[F],
  processorId: ProcessorId,
  ownerId: String,
  policyName: String,
  targetTable: String,
  retentionDays: Int,
  leaseTtlSeconds: Int
):
  private val ResourceType = "maintenance"
  private val ResourceId = processorId.value

  def runOnce(operation: Lease => F[RetentionCounts]): F[Unit] =
    Async[F].delay(UUID.randomUUID().toString).flatMap { token =>
      store
        .claimLease(
          ResourceType,
          ResourceId,
          ownerId,
          token,
          leaseTtlSeconds
        )
        .orRaise
        .flatMap {
          case None => Async[F].unit
          case Some(lease) => runClaimed(lease, operation)
        }
    }

  def renew(lease: Lease): F[Unit] =
    store
      .renewLease(
        ResourceType,
        ResourceId,
        lease,
        leaseTtlSeconds
      )
      .orRaise
      .flatMap { updated =>
        if updated == 0 then
          val message = "renew maintenance lease affected 0 rows"
          Async[F].raiseError(
            DatabaseOperationException(
              DatabaseError.Retryable("maintenance.renew_lease", IllegalStateException(message), message)
            )
          )
        else requireOne(updated, "renew maintenance lease")
      }

  private def runClaimed(
    lease: Lease,
    operation: Lease => F[RetentionCounts]
  ): F[Unit] =
    Async[F].delay(UUID.randomUUID().toString).flatMap { runId =>
      val release = store.releaseLease(ResourceType, ResourceId, lease).orRaise.void

      Async[F].realTimeInstant
        .flatMap { now =>
          val cutoff = now.minus(retentionDays.toLong, ChronoUnit.DAYS)
          store.startRetentionRun(runId, policyName, targetTable, cutoff, lease).orRaise.flatMap { inserted =>
            val work = runWithLeaseRenewal(lease, operation)
              .flatMap { counts =>
                store
                  .finishRetentionRun(
                    runId,
                    "completed",
                    counts.selected,
                    counts.archived,
                    counts.deleted,
                    None
                  )
                  .orRaise
                  .flatMap(finished => requireOne(finished, "finish retention run"))
              }
              .attempt
              .flatMap {
                case Right(_) => Async[F].unit
                case Left(error) =>
                  store
                    .finishRetentionRun(
                      runId,
                      "failed",
                      0L,
                      0L,
                      0L,
                      Some(Option(error.getMessage).getOrElse(error.getClass.getName).take(4096))
                    )
                    .orRaise
                    .attempt
                    .void *> Async[F].raiseError(error)
              }

            val cancel = store
              .finishRetentionRun(
                runId,
                "cancelled",
                0L,
                0L,
                0L,
                Some("retention processor cancelled")
              )
              .orRaise
              .attempt
              .void

            requireOne(inserted, "start retention run") *> work.onCancel(cancel)
          }
        }
        .guarantee(release)
    }

  private def runWithLeaseRenewal(
    lease: Lease,
    operation: Lease => F[RetentionCounts]
  ): F[RetentionCounts] =
    val renewalInterval = (leaseTtlSeconds.toLong.seconds / 3).max(1.second)
    val renewalLoop = (Async[F].sleep(renewalInterval) *> renew(lease)).foreverM

    Async[F].race(operation(lease), renewalLoop).flatMap {
      case Left(counts) => Async[F].pure(counts)
      case Right(_) =>
        Async[F].raiseError(IllegalStateException("maintenance lease renewal terminated unexpectedly"))
    }

  private def requireOne(updated: Int, operation: String): F[Unit] =
    if updated == 1 then Async[F].unit
    else Async[F].raiseError(IllegalStateException(s"$operation affected $updated rows"))
