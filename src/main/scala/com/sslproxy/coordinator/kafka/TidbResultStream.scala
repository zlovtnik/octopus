package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.persistence.ResultStore
import com.sslproxy.coordinator.tidb.TidbErrorClass
import fs2.Stream
import fs2.kafka.KafkaProducer
import com.sslproxy.coordinator.observability.StructuredLogger

object TidbResultStream:
  private val log = StructuredLogger(getClass)

  def run(
    cfg: KafkaCfg,
    resultStore: ResultStore[IO],
    producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(
      cfg,
      cfg.resultConsumer,
      cfg.resultTopic,
      producer,
      KafkaComponents.deserializeResult
    ) { lockedRecords =>
      for
        handled <- lockedRecords.traverse { locked =>
          resultStore.recordResultWithEvidence(locked.decoded, locked.metadata).value.flatMap {
            case Right(_) => IO.pure(Some(locked))
            case Left(error) if TidbErrorClass.classify(error.cause) == TidbErrorClass.Permanent =>
              LockedTopicConsumer
                .parkNonRetriable(
                  producer,
                  cfg.resultTopic + cfg.dlqSuffix,
                  cfg.resultConsumer,
                  locked.record,
                  error.cause
                )
                .as(None)
            case Left(error) =>
              IO.raiseError(
                new RuntimeException(
                  s"${error.operation}: ${Option(error.message).getOrElse("")}",
                  error.cause
                )
              )
          }
        }
        _ <- handled.flatten.traverse_ { locked =>
          val result = locked.decoded
          IO(
            log.info(
              "tidb_result_consumer",
              "status" -> "recorded",
              "batch_id" -> result.batchId,
              "result_status" -> result.status,
              "group" -> locked.metadata.consumerGroup,
              "partition" -> locked.metadata.partition.toString,
              "offset" -> locked.metadata.offset.toString
            )
          )
        }
      yield ()
    }
