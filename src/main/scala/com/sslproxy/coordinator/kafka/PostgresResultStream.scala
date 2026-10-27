package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.BrokerRecordMetadata
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.persistence.ResultStore
import com.sslproxy.coordinator.postgres.PostgresResult
import fs2.Stream
import fs2.kafka.KafkaProducer

object PostgresResultStream:
  private val log = StructuredLogger(getClass)

  def run(
    cfg: KafkaCfg,
    resultStore: ResultStore[IO],
    metrics: CoordinatorMetrics,
    producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(
      cfg,
      cfg.resultConsumer,
      cfg.resultTopic,
      cfg.resultConsumersCount,
      IO.unit, // Results release the backlog that suspends scan admission.
      metrics,
      producer,
      KafkaComponents.deserializeResult
    ) { lockedRecords =>
      for
        handled <- lockedRecords.traverse { locked =>
          persistResult(
            locked.decoded,
            locked.metadata,
            resultStore,
            error =>
              LockedTopicConsumer.parkNonRetriable(
                producer,
                cfg.resultTopic + cfg.dlqSuffix,
                cfg.resultConsumer,
                locked.record,
                error
              )
          ).map(recorded => Option.when(recorded)(locked))
        }
        _ <- handled.flatten.traverse_ { locked =>
          val result = locked.decoded
          IO(
            log.info(
              "postgres_result_consumer",
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

  private[kafka] def persistResult(
    result: PostgresResult,
    metadata: BrokerRecordMetadata,
    resultStore: ResultStore[IO],
    park: Throwable => IO[Unit]
  ): IO[Boolean] =
    resultStore.recordResultWithEvidence(result, metadata).value.flatMap {
      case Right(_) => IO.pure(true)
      case Left(error) if DurableIngestion.isParkable(error) =>
        park(error.cause).as(false)
      case other => KafkaDatabaseResult.require(IO.pure(other)).as(true)
    }
