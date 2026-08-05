package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.persistence.{IngestionStore, ResultStore}
import com.sslproxy.coordinator.tidb.{TidbErrorClass, TidbLoadHandler}
import fs2.Stream
import fs2.kafka.KafkaProducer
import com.sslproxy.coordinator.observability.StructuredLogger

object TidbLoadStream:
  private val log = StructuredLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      ingestionStore: IngestionStore[IO],
      resultStore: ResultStore[IO],
      handler: TidbLoadHandler,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.loadConsumer, cfg.loadTopic, artifact, producer,
      KafkaComponents.deserializeLoad,
      ingestionStore.loadConsumerOffsets(cfg.loadConsumer, cfg.loadTopic).value.flatMap {
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
      for
        handled <- lockedRecords.traverse { locked =>
          val load = locked.decoded
          IO(log.info("tidb_load_consumer", "status" -> "processing",
            "batch_id" -> load.batchId, "stream_name" -> load.streamName,
            "group" -> locked.metadata.consumerGroup,
            "partition" -> locked.metadata.partition.toString,
            "offset" -> locked.metadata.offset.toString)) *>
            dbSemaphore.permit.use(_ => handler.handle(load)).attempt.flatMap {
              case Right(result) => IO.pure(Some((locked, load, result)))
              case Left(error) if TidbErrorClass.classify(error) == TidbErrorClass.Permanent =>
                LockedTopicConsumer.parkNonRetriable(
                  producer,
                  cfg.loadTopic + cfg.dlqSuffix,
                  cfg.loadConsumer,
                  locked.record,
                  error
                ).as(None)
              case Left(error) => IO.raiseError(error)
            }
        }
        successful = handled.flatten
        _ <-
          if successful.isEmpty then IO.unit
          else
            resultStore.recordLoadResultsWithEvidence(
              successful.map { case (locked, load, result) => (load, result, locked.metadata) }
            ).value.flatMap {
              case Left(error) if TidbErrorClass.classify(error.cause) == TidbErrorClass.Permanent =>
                successful.traverse_ { case (locked, _, _) =>
                  LockedTopicConsumer.parkNonRetriable(
                    producer,
                    cfg.loadTopic + cfg.dlqSuffix,
                    cfg.loadConsumer,
                    locked.record,
                    error.cause
                  )
                }
              case other =>
                KafkaDatabaseResult.require(IO.pure(other)).void
            }
        _ <- successful.traverse_ { case (_, load, result) =>
          IO(log.info("tidb_load_consumer", "status" -> "durable",
            "batch_id" -> load.batchId, "result_status" -> result.status,
            "row_count" -> result.rowCount.toString))
        }
      yield ()
    }
