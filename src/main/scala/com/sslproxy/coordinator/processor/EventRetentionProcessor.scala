package com.sslproxy.coordinator.processor

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.sslproxy.coordinator.persistence.{MaintenanceStore, orRaise}

final class EventRetentionProcessor[F[_]: Async](
    store: MaintenanceStore[F],
    payloadArchiver: PayloadArchiver[F],
    ownerId: String,
    retentionDays: Int,
    tombstoneDays: Int,
    batchSize: Int,
    leaseTtlSeconds: Int
):
  private val ResourceType = "maintenance"
  private val ResourceId = ProcessorId.EventRetention.value
  private val runner = new FencedRetentionRunner[F](
    store,
    ProcessorId.EventRetention,
    ownerId,
    "wireless-event-retention",
    "sync_events",
    retentionDays,
    leaseTtlSeconds
  )

  def runOnce: F[Unit] =
    runner.runOnce { lease =>
      payloadArchiver.runOnce.flatMap { archived =>
        store.retainArchivedEvents(
          retentionDays,
          tombstoneDays,
          batchSize,
          ResourceType,
          ResourceId,
          lease
        ).orRaise.flatMap { case (selected, deleted) =>
          store.pruneExpiredTombstones(batchSize).orRaise.as(
            RetentionCounts(selected, archived.toLong, deleted)
          )
        }
      }
    }
