package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.domain.{IngestionDisposition, ScanRequestRecord}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.persistence.IngestionStore
import com.sslproxy.coordinator.tidb.{TidbErrorClass, TidbPayloadResolver}
import fs2.Stream
import fs2.kafka.KafkaProducer
import com.sslproxy.coordinator.observability.StructuredLogger

object ScanRequestStream:
  private val log = StructuredLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      store: IngestionStore[IO],
      payloadResolver: TidbPayloadResolver,
      metrics: CoordinatorMetrics,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.scanConsumer, cfg.scanTopic, artifact, producer,
      ScanRequestRecord.decodeWire,
      store.loadConsumerOffsets(cfg.scanConsumer, cfg.scanTopic).value.flatMap {
        case Left(err) =>
          IO(log.error("scan_request_consumer",
            "status" -> "offset_load_failed",
            "consumer_group" -> cfg.scanConsumer,
            "topic" -> cfg.scanTopic,
            "operation" -> err.operation,
            "error" -> err.message)) *>
            IO.raiseError[Set[CutoffKey]](
              new RuntimeException("cutover offset authorization unavailable"))
        case Right(cutoffs) =>
          IO.pure(cutoffs)
      }
    ) { lockedRecords =>
      val relevant = lockedRecords.filter(_.decoded.streamName == "proxy.events")
      for
        resolved <- relevant.traverse { locked =>
          IO.blocking(payloadResolver.resolve(locked.decoded)).map((locked, locked.decoded, _))
        }
        handled <- resolved.traverse { case (locked, request, record) =>
          dbSemaphore.permit.use(_ => store.recordScanRequestWithEvidence(record, locked.metadata).value).flatMap {
            case Right(decision) => IO.pure(Some((locked, request, decision)))
            case Left(error) if TidbErrorClass.classify(error.cause) == TidbErrorClass.Permanent =>
              LockedTopicConsumer.parkNonRetriable(
                producer,
                cfg.scanTopic + cfg.dlqSuffix,
                cfg.scanConsumer,
                locked.record,
                error.cause
              ).as(None)
            case Left(error) =>
              IO.raiseError(new RuntimeException(
                s"${error.operation}: ${Option(error.message).getOrElse("")}",
                error.cause
              ))
          }
        }
        _ <- handled.flatten.traverse_ { case (locked, request, decision) =>
          for
            _ <- IO.whenA(decision.disposition == IngestionDisposition.Processed)(
              IO(metrics.recordSyncEventHydrated())
            )
            _ <- IO(log.info("scan_request_consumer",
              "status" -> decision.disposition.databaseValue,
              "stream_name" -> request.streamName,
              "group" -> locked.metadata.consumerGroup,
              "partition" -> locked.metadata.partition.toString,
              "offset" -> locked.metadata.offset.toString))
          yield ()
        }
        _ <- lockedRecords.filter(_.decoded.streamName != "proxy.events")
          .traverse_ { locked =>
            IO(log.debug("scan_request_consumer",
              "status" -> "skipped",
              "stream_name" -> locked.decoded.streamName,
              "group" -> locked.metadata.consumerGroup,
              "partition" -> locked.metadata.partition.toString,
              "offset" -> locked.metadata.offset.toString))
          }
      yield ()
    }
