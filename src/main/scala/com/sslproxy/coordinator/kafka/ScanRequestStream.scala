package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.domain.{IngestionDisposition, ScanRequestRecord}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.tidb.{TidbPayloadResolver, TidbRepository}
import fs2.Stream
import fs2.kafka.KafkaProducer
import com.sslproxy.coordinator.observability.StructuredLogger

object ScanRequestStream:
  private val log = StructuredLogger(getClass)

  def run(
      cfg: KafkaCfg,
      artifact: VerifiedCutoverArtifact,
      repo: TidbRepository,
      payloadResolver: TidbPayloadResolver,
      metrics: CoordinatorMetrics,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    LockedTopicConsumer.stream(cfg, cfg.scanConsumer, cfg.scanTopic, artifact, producer,
      repo.loadConsumerOffsets(cfg.scanConsumer, cfg.scanTopic).flatMap {
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
      for
        decoded <- lockedRecords.traverse { locked =>
          IO.fromEither(ScanRequestRecord.decodeWire(locked.record.value)).map(locked -> _)
        }
        relevant = decoded.filter { case (_, request) => request.streamName == "proxy.events" }
        resolved <- relevant.traverse { case (locked, request) =>
          IO.blocking(payloadResolver.resolve(request)).map((locked, request, _))
        }
        decisions <- dbSemaphore.permit.use { _ =>
          KafkaDatabaseResult.require(
            repo.recordScanRequestsWithEvidence(
              resolved.map { case (locked, _, record) => record -> locked.metadata }
            )
          )
        }
        _ <- IO.raiseUnless(decisions.sizeIs == resolved.size)(
          IllegalStateException(
            s"recordScanRequestsWithEvidence returned ${decisions.size} decisions for ${resolved.size} records"
          )
        )
        _ <- resolved.zip(decisions).traverse_ { case ((locked, request, _), decision) =>
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
        _ <- decoded.filter { case (_, request) => request.streamName != "proxy.events" }
          .traverse_ { case (locked, request) =>
            IO(log.debug("scan_request_consumer",
              "status" -> "skipped",
              "stream_name" -> request.streamName,
              "group" -> locked.metadata.consumerGroup,
              "partition" -> locked.metadata.partition.toString,
              "offset" -> locked.metadata.offset.toString))
          }
      yield ()
    }
