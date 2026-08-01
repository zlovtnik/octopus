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
        decoded <- lockedRecords.traverse { locked =>
          IO.fromEither(KafkaComponents.deserializeResult(locked.record.value)).map(locked -> _)
        }
        _ <- dbSemaphore.permit.use { _ =>
          KafkaDatabaseResult.require(
            repo.recordResultsWithEvidence(
              decoded.map { case (locked, result) => result -> locked.metadata }
            )
          )
        }
        _ <- decoded.traverse_ { case (locked, result) =>
          IO(log.info("tidb_result_consumer", "status" -> "recorded",
            "batch_id" -> result.batchId, "result_status" -> result.status,
            "group" -> locked.metadata.consumerGroup,
            "partition" -> locked.metadata.partition.toString,
            "offset" -> locked.metadata.offset.toString))
        }
      yield ()
    }
