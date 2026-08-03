package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.tidb.TidbRepository
import fs2.Stream
import fs2.kafka.KafkaProducer
import com.sslproxy.coordinator.observability.StructuredLogger

object TidbResultStream:
  private val log = StructuredLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.resultConsumer, cfg.resultTopic, artifact, producer,
      KafkaComponents.deserializeResult,
      repo.loadConsumerOffsets(cfg.resultConsumer, cfg.resultTopic).flatMap {
        case Left(err) =>
          IO(log.error("tidb_result_consumer",
            "status" -> "offset_load_failed",
            "consumer_group" -> cfg.resultConsumer,
            "topic" -> cfg.resultTopic,
            "operation" -> err.operation,
            "error" -> err.message)) *>
            IO.raiseError[Set[CutoffKey]](
              new RuntimeException("cutover offset authorization unavailable"))
        case Right(cutoffs) =>
          IO.pure(cutoffs)
      }
    ) { lockedRecords =>
      for
        _ <- dbSemaphore.permit.use { _ =>
          KafkaDatabaseResult.require(
            repo.recordResultsWithEvidence(
              lockedRecords.map(locked => locked.decoded -> locked.metadata)
            )
          )
        }
        _ <- lockedRecords.traverse_ { locked =>
          val result = locked.decoded
          IO(log.info("tidb_result_consumer", "status" -> "recorded",
            "batch_id" -> result.batchId, "result_status" -> result.status,
            "group" -> locked.metadata.consumerGroup,
            "partition" -> locked.metadata.partition.toString,
            "offset" -> locked.metadata.offset.toString))
        }
      yield ()
    }
