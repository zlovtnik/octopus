package com.sslproxy.coordinator.metrics

import cats.effect.IO
import com.sslproxy.coordinator.config.{StatsMaterializerConfig, StatsStoreConfig}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.postgres.PostgresRepository
import fs2.Stream

import scala.concurrent.duration.*

/** Background metric workers. Not a ProcessorId — never feeds readiness. */
object StatsMaterializerStream:
  private val logger = StructuredLogger("metrics.stream")

  def run(
    repo: PostgresRepository,
    metrics: CoordinatorMetrics,
    storeConfig: StatsStoreConfig,
    config: StatsMaterializerConfig
  ): Stream[IO, Unit] =
    if !config.enabled then Stream.empty
    else
      Stream
        .resource(StatsStores.redisResource(storeConfig))
        .flatMap { redis =>
          Stream.resource(StatsStores.minioResource(storeConfig)).flatMap { minio =>
            Stream
              .eval(
                for
                  materializer <- StatsMaterializer.create(repo, metrics, redis, minio)
                  pool <- MetricWorkerPool.create(
                    materializer.runJob,
                    config.workerCount,
                    config.jobTimeoutSeconds.seconds
                  )
                yield (materializer, pool)
              )
              .flatMap { case (_, pool) =>
                val workers = Stream.eval(pool.workers)
                val peaks = Stream
                  .awakeEvery[IO](config.peaksIntervalSeconds.seconds)
                  .evalMap(_ => pool.queue.offer(MetricJob.Peaks))
                val live = Stream
                  .awakeEvery[IO](config.liveIntervalSeconds.seconds)
                  .evalMap(_ => pool.queue.offer(MetricJob.LiveStrip))
                val history = Stream
                  .awakeEvery[IO](config.historyIntervalSeconds.seconds)
                  .evalMap(_ => pool.queue.offer(MetricJob.HistoryBuckets))
                val publish = Stream
                  .awakeEvery[IO](config.publishIntervalSeconds.seconds)
                  .evalMap(_ => pool.queue.offer(MetricJob.Publish))
                val seed =
                  Stream.eval(
                    pool.queue.offer(MetricJob.Peaks) *>
                      pool.queue.offer(MetricJob.LiveStrip) *>
                      pool.queue.offer(MetricJob.HistoryBuckets) *>
                      IO(logger.info("stats_materializer_started"))
                  )
                seed ++ workers.merge(peaks).merge(live).merge(history).merge(publish)
              }
          }
        }
