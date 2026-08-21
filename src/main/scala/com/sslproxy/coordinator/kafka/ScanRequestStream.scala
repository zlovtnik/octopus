package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.{IngestConfig, KafkaCfg}
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
    ingest: IngestConfig,
    store: IngestionStore[IO],
    payloadResolver: TidbPayloadResolver,
    metrics: CoordinatorMetrics,
    producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val configuredStreams = ingest.streamNames.toSet
    LockedTopicConsumer.stream(
      cfg,
      cfg.scanConsumer,
      cfg.scanTopic,
      producer,
      ScanRequestRecord.decodeWire
    ) { lockedRecords =>
      val (configured, unconfigured) = lockedRecords.partition { locked =>
        isConfiguredStream(locked.decoded.streamName, configuredStreams)
      }
      for
        _ <- unconfigured.traverse_ { locked =>
          LockedTopicConsumer.parkNonRetriable(
            producer,
            cfg.scanTopic + cfg.dlqSuffix,
            cfg.scanConsumer,
            locked.record,
            IllegalArgumentException(
              s"scan request stream ${locked.decoded.streamName} is not configured for Octopus ingestion"
            )
          )
        }
        resolved <- configured.traverse { locked =>
          IO.blocking(payloadResolver.resolve(locked.decoded)).map((locked, locked.decoded, _))
        }
        handled <- resolved.traverse { case (locked, request, record) =>
          store.recordScanRequestWithEvidence(record, locked.metadata).value.flatMap {
            case Right(decision) => IO.pure(Some((locked, request, decision)))
            case Left(error) if TidbErrorClass.classify(error.cause) == TidbErrorClass.Permanent =>
              LockedTopicConsumer
                .parkNonRetriable(
                  producer,
                  cfg.scanTopic + cfg.dlqSuffix,
                  cfg.scanConsumer,
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
        _ <- handled.flatten.traverse_ { case (locked, request, decision) =>
          for
            _ <- IO.whenA(decision.disposition == IngestionDisposition.Processed)(
              IO(metrics.recordSyncEventHydrated())
            )
            _ <- IO(
              log.info(
                "scan_request_consumer",
                "status" -> decision.disposition.databaseValue,
                "stream_name" -> request.streamName,
                "group" -> locked.metadata.consumerGroup,
                "partition" -> locked.metadata.partition.toString,
                "offset" -> locked.metadata.offset.toString
              )
            )
          yield ()
        }
      yield ()
    }

  private[kafka] def isConfiguredStream(streamName: String, configuredStreams: Set[String]): Boolean =
    configuredStreams.contains(streamName)
