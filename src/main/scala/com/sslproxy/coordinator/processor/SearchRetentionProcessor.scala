package com.sslproxy.coordinator.processor

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.sslproxy.coordinator.persistence.{MaintenanceStore, orRaise}

final class SearchRetentionProcessor[F[_]: Async](
    store: MaintenanceStore[F],
    ownerId: String,
    retentionDays: Int,
    batchSize: Int,
    leaseTtlSeconds: Int
):
  private val ResourceType = "maintenance"
  private val ResourceId = ProcessorId.SearchRetention.value
  private val runner = new FencedRetentionRunner[F](
    store,
    ProcessorId.SearchRetention,
    ownerId,
    "search-document-retention",
    "atheros_search.search_documents",
    retentionDays,
    leaseTtlSeconds
  )

  def runOnce: F[Unit] =
    runner.runOnce { lease =>
      store.retainSearchDocuments(
        retentionDays,
        batchSize,
        ResourceType,
        ResourceId,
        lease
      ).orRaise.map { case (selected, deleted) =>
        RetentionCounts(selected, 0L, deleted)
      }
    }
