package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.tidb.{TidbLoadHandler, TidbRepository}
import fs2.Stream
import com.sslproxy.coordinator.observability.StructuredLogger

object TidbLoadStream:
  private val log = StructuredLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      handler: TidbLoadHandler,
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.loadConsumer, cfg.loadTopic, artifact,
      repo.loadConsumerOffsets(cfg.loadConsumer, cfg.loadTopic).flatMap {
        case Left(err) =>
          IO(log.error("tidb_load_consumer",
            "status" -> "offset_load_failed",
            "consumer_group" -> cfg.loadConsumer,
            "topic" -> cfg.loadTopic,
            "operation" -> err.operation,
            "error" -> err.message)) *>
            IO.raiseError[Set[CutoffKey]](
              new RuntimeException("cutover offset authorization unavailable"))
        case Right(cutoffs) =>
          IO.pure(cutoffs)
      }
    ) { locked =>
      dbSemaphore.permit.use { _ =>
        for
          load <- IO.fromEither(KafkaComponents.deserializeLoad(locked.record.value))
          _ <- IO(log.info("tidb_load_consumer", "status" -> "processing",
            "batch_id" -> load.batchId, "stream_name" -> load.streamName,
            "group" -> locked.metadata.consumerGroup,
            "partition" -> locked.metadata.partition.toString,
            "offset" -> locked.metadata.offset.toString))
          result <- handler.handle(load)
          _ <- KafkaDatabaseResult.require(
            repo.recordLoadResultWithEvidence(load, result, locked.metadata)
          )
          _ <- IO(log.info("tidb_load_consumer", "status" -> "durable",
            "batch_id" -> load.batchId, "result_status" -> result.status,
            "row_count" -> result.rowCount.toString))
        yield ()
      }
    }
