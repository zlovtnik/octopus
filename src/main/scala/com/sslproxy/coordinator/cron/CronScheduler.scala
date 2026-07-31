package com.sslproxy.coordinator.cron

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.{CronConfig, IngestConfig}
import com.sslproxy.coordinator.dispatch.{BackpressureService, BatchDispatchService}
import com.sslproxy.coordinator.dispatch.BatchDispatchService.DispatchResult
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.tidb.TidbRepository
import com.sslproxy.coordinator.sink.SchemaIntrospector
import fs2.Stream
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

class CronScheduler(
    cfg: CronConfig,
    ingestConfig: IngestConfig,
    repo: TidbRepository,
    backpressureService: BackpressureService,
    batchDispatchService: BatchDispatchService,
    metrics: CoordinatorMetrics,
    schemaIntrospector: SchemaIntrospector,
    dbSemaphore: Semaphore[IO]
):
  import CronScheduler.log

  private val loopCounter: Ref[IO, Long] = Ref.unsafe[IO, Long](0L)
  private val lastShadowAuditMs: Ref[IO, Long] = Ref.unsafe[IO, Long](0L)

  private val knownTables: List[String] = List(
    "proxy_events",
    "proxy_blocked_host_rollups",
    "proxy_payload_audit",
    "wireless_sensors",
    "wireless_audit_frames",
    "wireless_bandwidth_windows",
    "wireless_alerts",
    "wireless_alerts_ledger",
    "wireless_client_inventory",
    "wireless_probe_requests"
  )

  val schemaRefresher: Stream[IO, Unit] =
    schemaIntrospector.startRefresher(knownTables)

  val mainLoop: Stream[IO, Unit] =
    val backpressureStream = Stream
      .awakeEvery[IO](cfg.idleSleepMs.millis)
      .evalMap { _ =>
        dbSemaphore.permit.use { _ =>
          backpressureService.checkAndAct.void
        }.handleErrorWith { err =>
          IO(log.error("cron_backpressure", err, "status" -> "failed"))
        }
      }

    val ingestStream = Stream
      .awakeEvery[IO](cfg.idleSleepMs.millis)
      .evalMap { _ =>
        dbSemaphore.permit.use(_ => processIngest()).handleErrorWith { err =>
          IO(log.error("cron_ingest", err, "status" -> "failed"))
        }
      }

    val recoverAndDispatchStream = Stream
      .awakeEvery[IO](cfg.idleSleepMs.millis)
      .evalMap { _ =>
        (dbSemaphore.permit.use(_ => recoverStaleBatches()) >> dispatchBatches()).handleErrorWith { err =>
          IO(log.error("cron_dispatch", err, "status" -> "failed"))
        }
      }

    val shadowAuditStream = Stream
      .awakeEvery[IO](10.seconds)
      .evalMap { _ =>
        dbSemaphore.permit.use(_ => shadowAudit()).handleErrorWith { err =>
          IO(log.error("cron_shadow_audit", err, "status" -> "failed"))
        }
      }

    val metricsStream = Stream
      .awakeEvery[IO](cfg.heartbeatLogIntervalMs.millis)
      .evalMap { _ =>
        (metrics.heartbeat() >> IO(metrics.incrementLoopCounter()) >> loopCounter.update(_ + 1))
          .handleErrorWith { err =>
            IO(log.error("cron_metrics", err, "status" -> "failed"))
          }
      }

    backpressureStream
      .merge(ingestStream)
      .merge(recoverAndDispatchStream)
      .merge(shadowAuditStream)
      .merge(metricsStream)

  private def processIngest(): IO[Unit] =
    val budget = backpressureService.budget

    repo.pendingLedgerCount().flatMap {
      case Left(err) =>
        IO(log.warn("ingest_ledger", "status" -> "pending_count_failed",
          "operation" -> err.operation, "error" -> err.message)) *>
          IO(metrics.recordIngestInvocation(false))

      case Right(pendingCount) =>
        val logPending = IO(log.info("ingest_ledger", "status" -> "pending", "count" -> pendingCount.toString))
        val throttleCheck = if pendingCount >= budget then
          IO(log.info("backpressure", "status" -> "throttled",
            "pending_count" -> pendingCount.toString, "budget" -> budget.toString,
            "ingest_batch_size" -> cfg.ingestBatchSize.toString))
        else IO.unit

        logPending *> throttleCheck *>
          repo.processIngestLedger(
            ingestConfig.streamNames,
            ingestConfig.loadStreamNames,
            cfg.scanMaxAttempts,
            cfg.scanRetryBackoffSeconds,
            cfg.ingestBatchSize
          ).flatMap {
            case Left(err) =>
              IO(log.error("ingest_ledger", "status" -> "failed",
                "operation" -> err.operation, "error" -> err.message)) *>
                IO(metrics.recordIngestInvocation(false))

            case Right(processed) =>
              IO(metrics.recordIngestInvocation(true)) *>
                IO(metrics.recordIngestProcessed(processed)) *>
                IO.whenA(processed > 0)(
                  IO(log.info("ingest_ledger", "status" -> "processed", "count" -> processed.toString))
                )
          }
    }

  private def recoverStaleBatches(): IO[Unit] =
    repo.recoverExpiredOutboxLeases().flatMap {
      case Left(err) =>
        IO(log.error("outbox_lease_recovery", "status" -> "failed",
          "operation" -> err.operation, "error" -> err.message))
      case Right(count) =>
        IO.whenA(count > 0)(
          IO(log.info("outbox_lease_recovery", "status" -> "recovered", "count" -> count.toString))
        )
    }

  private def dispatchBatches(): IO[Unit] =
    // Ordered outbox UPDATE claims contend on the same leading row/range.
    // Drain sequentially so claim transactions cannot occupy the whole pool.
    CronScheduler.drainBatch(cfg.dispatchBatchSize) { () =>
      batchDispatchService.dispatchNext()
    }.void

  private def shadowAudit(): IO[Unit] =
    val intervalMs = 10_000L

    lastShadowAuditMs.get.flatMap { lastMs =>
      val now = System.currentTimeMillis()
      if now - lastMs < intervalMs then IO.unit
      else
        repo.generateShadowAlerts().flatMap {
          case Left(err) =>
            IO(log.error("shadow_audit", "status" -> "failed",
              "operation" -> err.operation, "error" -> err.message)) *>
              lastShadowAuditMs.set(now)

          case Right(alerts) =>
            IO.whenA(alerts.nonEmpty)(
              IO(log.info("shadow_audit", "status" -> "alerts_generated",
                "count" -> alerts.size.toString))
            ) *> lastShadowAuditMs.set(now)
        }
    }

object CronScheduler:
  private val log = StructuredLogger(getClass)

  private[cron] def drainBatch(
      maxDispatches: Int
  )(dispatchNext: () => IO[DispatchResult]): IO[Int] =
    def loop(remaining: Int, dispatched: Int): IO[Int] =
      if remaining <= 0 then IO.pure(dispatched)
      else
        dispatchNext().flatMap {
          case DispatchResult.Dispatched =>
            loop(remaining - 1, dispatched + 1)
          case DispatchResult.ContinueDraining =>
            loop(remaining - 1, dispatched)
          case DispatchResult.NoWork | DispatchResult.StopDraining =>
            IO.pure(dispatched)
        }

    loop(maxDispatches.max(0), 0)
