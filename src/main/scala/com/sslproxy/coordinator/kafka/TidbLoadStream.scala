package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.tidb.{TidbLoadHandler, TidbRepository}
import fs2.Stream
import fs2.kafka.KafkaProducer
import com.sslproxy.coordinator.observability.StructuredLogger

object TidbLoadStream:
  private val log = StructuredLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      handler: TidbLoadHandler,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.loadConsumer, cfg.loadTopic, artifact, producer,
      KafkaComponents.deserializeLoad,
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
    ) { lockedRecords =>
      dbSemaphore.permit.use { _ =>
        for
          handled <- lockedRecords.traverse { locked =>
            val load = locked.decoded
            IO(log.info("tidb_load_consumer", "status" -> "processing",
              "batch_id" -> load.batchId, "stream_name" -> load.streamName,
              "group" -> locked.metadata.consumerGroup,
              "partition" -> locked.metadata.partition.toString,
              "offset" -> locked.metadata.offset.toString)) *>
              handler.handle(load).map(result => (locked, load, result))
          }
          _ <- KafkaDatabaseResult.require(
            repo.recordLoadResultsWithEvidence(
              handled.map { case (locked, load, result) => (load, result, locked.metadata) }
            )
          )
          _ <- handled.traverse_ { case (_, load, result) =>
            IO(log.info("tidb_load_consumer", "status" -> "durable",
              "batch_id" -> load.batchId, "result_status" -> result.status,
              "row_count" -> result.rowCount.toString))
          }
        yield ()
      }
    }
