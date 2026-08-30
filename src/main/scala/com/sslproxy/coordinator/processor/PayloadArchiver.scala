package com.sslproxy.coordinator.processor

import cats.effect.kernel.Async
import cats.syntax.all.*
import com.sslproxy.coordinator.archive.{InvalidArchiveCandidate, PayloadArchive}
import com.sslproxy.coordinator.persistence.{MaintenanceStore, orRaise}

final class PayloadArchiver[F[_]: Async](
  store: MaintenanceStore[F],
  archive: PayloadArchive[F],
  hotDays: Int,
  batchSize: Int
):
  def runOnce: F[Int] =
    store.findArchiveCandidates(hotDays, batchSize).orRaise.flatMap { candidates =>
      candidates
        .traverse { candidate =>
          archive
            .archive(candidate)
            .flatMap { receipt =>
              store.recordArchive(candidate, receipt).orRaise.as(1)
            }
            .handleErrorWith {
              case error: InvalidArchiveCandidate =>
                store.quarantineArchiveCandidate(candidate, error.getMessage).orRaise.as(0)
              case error => Async[F].raiseError(error)
            }
        }
        .map(_.sum)
    }
