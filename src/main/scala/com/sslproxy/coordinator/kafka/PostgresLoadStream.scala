package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.{DatabaseError, IngestionInvariantViolation}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, StructuredLogger}
import com.sslproxy.coordinator.persistence.ResultStore
import com.sslproxy.coordinator.postgres.{PostgresErrorClass, PostgresLoad, PostgresLoadHandler, PostgresResult}
import fs2.Stream
import fs2.kafka.KafkaProducer

object PostgresLoadStream:
  private val log = StructuredLogger(getClass)

  def run(
    cfg: KafkaCfg,
    resultStore: ResultStore[IO],
    handler: PostgresLoadHandler,
    producer: KafkaProducer[IO, String, String],
    dbSemaphore: Semaphore[IO],
    metrics: CoordinatorMetrics
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(
      cfg,
      cfg.loadConsumer,
      cfg.loadTopic,
      cfg.loadConsumersCount,
      IO.unit, // Drain outstanding work even while scan admission is suspended.
      metrics,
      producer,
      KafkaComponents.deserializeLoad
    ) { lockedRecords =>
      for
        handled <- lockedRecords.traverse { locked =>
          val load = locked.decoded
          IO(
            log.info(
              "postgres_load_consumer",
              "status" -> "processing",
              "batch_id" -> load.batchId,
              "stream_name" -> load.streamName,
              "group" -> locked.metadata.consumerGroup,
              "partition" -> locked.metadata.partition.toString,
              "offset" -> locked.metadata.offset.toString
            )
          ) *>
            dbSemaphore.permit.use(_ => handler.handle(load)).attempt.flatMap {
              case Right(result) => IO.pure(Some((locked, result)))
              case Left(error)
                  if !PostgresErrorClass.exceptions(error).exists(_.isInstanceOf[IngestionInvariantViolation]) &&
                    PostgresErrorClass.classify(error) == PostgresErrorClass.Permanent =>
                LockedTopicConsumer
                  .parkNonRetriable(
                    producer,
                    cfg.loadTopic + cfg.dlqSuffix,
                    cfg.loadConsumer,
                    locked.record,
                    error
                  )
                  .as(None)
              case Left(error) => IO.raiseError(error)
            }
        }
        durable <- persistResults(
          handled.flatten,
          resultStore,
          (locked, error) =>
            LockedTopicConsumer.parkNonRetriable(
              producer,
              cfg.loadTopic + cfg.dlqSuffix,
              cfg.loadConsumer,
              locked.record,
              error
            )
        )
        _ <- durable.traverse_ { case (locked, result) =>
          IO(
            log.info(
              "postgres_load_consumer",
              "status" -> "durable",
              "batch_id" -> locked.decoded.batchId,
              "result_status" -> result.status,
              "row_count" -> result.rowCount.toString
            )
          )
        }
      yield ()
    }

  private[kafka] def persistResults(
    records: List[(LockedBrokerRecord[PostgresLoad], PostgresResult)],
    resultStore: ResultStore[IO],
    park: (LockedBrokerRecord[PostgresLoad], Throwable) => IO[Unit]
  ): IO[List[(LockedBrokerRecord[PostgresLoad], PostgresResult)]] =
    def writeBatch(batch: List[(LockedBrokerRecord[PostgresLoad], PostgresResult)]): IO[Either[DatabaseError, Unit]] =
      resultStore
        .recordLoadResultsWithEvidence(batch.map { case (locked, result) =>
          (locked.decoded, result, locked.metadata)
        })
        .value

    if records.isEmpty then IO.pure(Nil)
    else
      writeBatch(records)
        .flatMap {
          case Left(error) if DurableIngestion.isParkable(error) && records.size > 1 =>
            // The failed transaction rolled back every result. Isolate poison so
            // healthy peers still obtain their durable result and outbox evidence.
            records.traverseFilter { record =>
              writeBatch(List(record)).flatMap {
                case Right(_) => IO.pure(Some(record))
                case Left(singleError) if DurableIngestion.isParkable(singleError) =>
                  park(record._1, singleError.cause).as(None)
                case other => KafkaDatabaseResult.require(IO.pure(other)).as(Some(record))
              }
            }
          case Left(error) if DurableIngestion.isParkable(error) =>
            records.traverse_ { case (locked, _) => park(locked, error.cause) }.as(Nil)
          case other => KafkaDatabaseResult.require(IO.pure(other)).as(records)
        }
