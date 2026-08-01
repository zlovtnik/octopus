package com.sslproxy.coordinator.kafka

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.cutover.{CutoffKey, CutoverError, CutoverOffsetEvidence, VerifiedCutoverArtifact}
import com.sslproxy.coordinator.domain.BrokerRecordMetadata
import com.sslproxy.coordinator.util.Sha256Utils
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, CommittableOffsetBatch, ConsumerRecord, KafkaConsumer, KafkaProducer, ProducerRecord, ProducerRecords}
import io.circe.Json
import org.apache.kafka.common.TopicPartition
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

private[kafka] final case class CutoverOffsetGuardState(
    bootstrapped: Set[CutoffKey]
):
  def contains(key: CutoffKey): Boolean = bootstrapped.contains(key)

private[kafka] object CutoverOffsetGuardState:
  val empty: CutoverOffsetGuardState = CutoverOffsetGuardState(Set.empty)

/** Pure state transition used to distinguish the one exact bootstrap check
  * from ordinary at-or-after authorization for a partition.
  */
private[kafka] object CutoverOffsetGuard:
  def authorize(
      state: CutoverOffsetGuardState,
      artifact: VerifiedCutoverArtifact,
      groupId: String,
      topic: String,
      partition: Int,
      offset: Long
  ): Either[CutoverError, (CutoverOffsetGuardState, CutoverOffsetEvidence)] =
    val key = CutoffKey(groupId, topic, partition)
    artifact.requireCoverage(List(key)).flatMap { _ =>
      val authorization =
        if state.contains(key) then
          artifact.authorizeRecordOffset(groupId, topic, partition, offset)
        else
          artifact.verifyBootstrapPosition(groupId, topic, partition, offset)

      authorization.map { evidence =>
        (CutoverOffsetGuardState(state.bootstrapped + key), evidence)
      }
    }

private[kafka] final case class LockedBrokerRecord(
    record: ConsumerRecord[String, String],
    metadata: BrokerRecordMetadata,
    authorization: CutoverOffsetEvidence
)

/** A single-topic/single-group consumer boundary. Each call allocates a new
  * KafkaConsumer and commits a batch only after `process` has durably handled
  * every authorized record in it. Cutover/topic authorization, processing,
  * and database failures terminate without committing. Invalid payloads are
  * parked only after their broker coordinate has passed cutover authorization.
  */
private[kafka] object LockedTopicConsumer:
  private val log = StructuredLogger(getClass)

  def stream(
      cfg: KafkaCfg,
      groupId: String,
      topic: String,
      artifact: VerifiedCutoverArtifact,
      producer: KafkaProducer[IO, String, String],
      bootstrapped: IO[Set[CutoffKey]] = IO.pure(Set.empty)
  )(
      process: List[LockedBrokerRecord] => IO[Unit]
  ): Stream[IO, Unit] =
    Stream.eval(bootstrapped.flatMap(keys => Ref.of[IO, CutoverOffsetGuardState](CutoverOffsetGuardState(keys)))).flatMap { guard =>
      Stream
        .resource(KafkaConsumer.resource(KafkaComponents.consumerSettings(cfg, groupId)))
        .flatMap { consumer =>
          Stream.eval(preflightAndSubscribe(consumer, artifact, groupId, topic)) >> {
            val coverage = consumer.assignmentStream
              .evalMap(partitions => IO.fromEither(validateCoverage(artifact, groupId, topic, partitions)))
              .drain

            val records = consumer.partitionedStream
              .map { partitionStream =>
                partitionStream
                  .groupWithin(cfg.lockedBatchSize, cfg.lockedBatchWindowMs.millis)
                  .evalMap { committables =>
                    processBatch(
                      guard,
                      artifact,
                      groupId,
                      topic,
                      committables.toList,
                      producer,
                      cfg.dlqSuffix,
                      process
                    )
                  }
              }
              .parJoinUnbounded

            records.concurrently(coverage)
          }
        }
    }

  private def preflightAndSubscribe(
      consumer: KafkaConsumer[IO, String, String],
      artifact: VerifiedCutoverArtifact,
      groupId: String,
      topic: String
  ): IO[Unit] =
    for
      partitions <- consumer.partitionsFor(topic)
      _ <- IO.raiseWhen(partitions.isEmpty)(
        IllegalStateException(s"broker returned no partitions for locked topic $topic")
      )
      keys = partitions.map(info => CutoffKey(groupId, info.topic, info.partition))
      _ <- IO.fromEither(artifact.requireCoverage(keys))
      _ <- consumer.subscribeTo(topic)
    yield ()

  private def processBatch(
      guard: Ref[IO, CutoverOffsetGuardState],
      artifact: VerifiedCutoverArtifact,
      groupId: String,
      expectedTopic: String,
      committables: List[CommittableConsumerRecord[IO, String, String]],
      producer: KafkaProducer[IO, String, String],
      dlqSuffix: String,
      process: List[LockedBrokerRecord] => IO[Unit]
  ): IO[Unit] =
    for
      prepared <- committables.traverse { committable =>
        prepareRecord(guard, artifact, groupId, expectedTopic, committable.record).attempt.flatMap {
          case Right(locked) => IO.pure(Some(locked))
          case Left(error: CutoverError) => IO.raiseError(error)
          case Left(error: IllegalStateException) => IO.raiseError(error)
          case Left(error) =>
            parkNonRetriable(
              producer,
              expectedTopic + dlqSuffix,
              groupId,
              committable.record,
              error
            ).as(None)
        }
      }
      _ <- process(prepared.flatten)
      _ <- CommittableOffsetBatch.fromFoldable(committables.map(_.offset)).commit
    yield ()

  private def prepareRecord(
      guard: Ref[IO, CutoverOffsetGuardState],
      artifact: VerifiedCutoverArtifact,
      groupId: String,
      expectedTopic: String,
      record: ConsumerRecord[String, String]
  ): IO[LockedBrokerRecord] =
    for
      _ <- IO.raiseWhen(record.topic != expectedTopic)(
        IllegalStateException(
          s"consumer group $groupId received unexpected topic ${record.topic}; expected $expectedTopic"
        )
      )
      authorization <- authorize(
        guard,
        artifact,
        groupId,
        record.topic,
        record.partition,
        record.offset
      )
      rawValue <- IO.fromOption(Option(record.value))(
        IllegalArgumentException(
          s"tombstone is not valid for group=$groupId topic=${record.topic} " +
            s"partition=${record.partition} offset=${record.offset}"
        )
      )
      metadata = BrokerRecordMetadata(
        topic = record.topic,
        partition = record.partition,
        offset = record.offset,
        consumerGroup = groupId,
        groupVersion = artifact.artifact.groupVersion,
        artifactSha256 = artifact.artifact.artifactSha256,
        messageKey = Option(record.key),
        payloadSha256 = Sha256Utils.sha256Hex(rawValue)
      )
      _ <- IO(log.debug("locked_consumer_record", "status" -> "authorized",
        "group" -> groupId, "topic" -> record.topic,
        "partition" -> record.partition.toString,
        "offset" -> record.offset.toString,
        "cutoff" -> authorization.cutoffOffset.toString))
    yield LockedBrokerRecord(record, metadata, authorization)

  private def parkNonRetriable(
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String,
      groupId: String,
      record: ConsumerRecord[String, String],
      error: Throwable
  ): IO[Unit] =
    val message = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
    val body = Json.obj(
      "consumer_group" -> Json.fromString(groupId),
      "error" -> Json.fromString(message),
      "error_class" -> Json.fromString(error.getClass.getSimpleName),
      "offset" -> Json.fromLong(record.offset),
      "original" -> Option(record.value).fold(Json.Null)(Json.fromString),
      "partition" -> Json.fromInt(record.partition),
      "topic" -> Json.fromString(record.topic)
    ).noSpaces
    val key = Option(record.key).getOrElse(s"${record.partition}:${record.offset}")

    producer.produce(ProducerRecords.one(ProducerRecord(dlqTopic, key, body))).flatten.void *>
      IO(log.error("locked_consumer_record",
        "status" -> "parked",
        "group" -> groupId,
        "topic" -> record.topic,
        "partition" -> record.partition.toString,
        "offset" -> record.offset.toString,
        "dlq_topic" -> dlqTopic,
        "error" -> message))

  private def authorize(
      guard: Ref[IO, CutoverOffsetGuardState],
      artifact: VerifiedCutoverArtifact,
      groupId: String,
      topic: String,
      partition: Int,
      offset: Long
  ): IO[CutoverOffsetEvidence] =
    guard.modify { state =>
      CutoverOffsetGuard.authorize(state, artifact, groupId, topic, partition, offset) match
        case Right((next, evidence)) => (next, Right(evidence))
        case Left(error)             => (state, Left(error))
    }.flatMap(IO.fromEither)

  private[kafka] def validateCoverage(
      artifact: VerifiedCutoverArtifact,
      groupId: String,
      expectedTopic: String,
      partitions: Iterable[TopicPartition]
  ): Either[Throwable, Unit] =
    val unexpected = partitions.iterator.filter(_.topic != expectedTopic).toList
    if unexpected.nonEmpty then
      Left(IllegalStateException(
        s"consumer group $groupId was assigned unexpected topics: " +
          unexpected.map(_.topic).distinct.sorted.mkString(",")
      ))
    else
      artifact.requireCoverage(
        partitions.iterator.map(partition =>
          CutoffKey(groupId, partition.topic, partition.partition)
        ).toList
      )
