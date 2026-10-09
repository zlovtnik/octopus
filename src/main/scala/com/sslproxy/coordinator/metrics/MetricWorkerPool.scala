package com.sslproxy.coordinator.metrics

import cats.effect.IO
import cats.effect.std.Queue
import cats.syntax.all.*
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

enum MetricJob:
  case Peaks
  case LiveStrip
  case HistoryBuckets
  case Publish

  def name: String = this match
    case Peaks          => "peaks"
    case LiveStrip      => "liveStrip"
    case HistoryBuckets => "historyBuckets"
    case Publish        => "publish"

/** Fixed pool of workers draining a metric-job queue. Compute only — never
  * touches processor readiness and never throws into ingest fibers.
  */
final class MetricWorkerPool(
  val queue: Queue[IO, MetricJob],
  runJob: MetricJob => IO[Unit],
  workerCount: Int,
  jobTimeout: FiniteDuration
):
  private val logger = StructuredLogger("metrics.worker")

  def workers: IO[Unit] =
    (1 to workerCount.max(1)).toList.parTraverse_(_ => workerLoop)

  private def workerLoop: IO[Unit] =
    queue.take.flatMap { job =>
      runGuarded(job) >> workerLoop
    }

  private def runGuarded(job: MetricJob): IO[Unit] =
    val run =
      runJob(job).handleErrorWith { error =>
        IO(
          logger.error(
            "metric_job_failed",
            "job" -> job.name,
            "error" -> Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
          )
        )
      }
    IO.race(run, IO.sleep(jobTimeout)).flatMap {
      case Left(_)  => IO.unit
      case Right(_) =>
        IO(
          logger.warn(
            "metric_job_timeout",
            "job" -> job.name,
            "timeoutMs" -> jobTimeout.toMillis.toString
          )
        )
    }

object MetricWorkerPool:
  def create(
    runJob: MetricJob => IO[Unit],
    workerCount: Int,
    jobTimeout: FiniteDuration
  ): IO[MetricWorkerPool] =
    Queue.unbounded[IO, MetricJob].map(q => new MetricWorkerPool(q, runJob, workerCount, jobTimeout))
