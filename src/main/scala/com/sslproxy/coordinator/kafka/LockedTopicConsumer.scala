package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.{BrokerConsumerContract, BrokerRecordMetadata}
import com.sslproxy.coordinator.observability.{CoordinatorTracing, StructuredLogger}
import com.sslproxy.coordinator.util.{ErrorSanitizer, Sha256Utils}
import fs2.Stream
import fs2.kafka.{
  CommittableConsumerRecord,
  CommittableOffsetBatch,
  ConsumerRecord,
  KafkaConsumer,
  KafkaProducer,
  ProducerRecord,
  ProducerRecords
}
import io.circe.Json
import io.opentelemetry.api.trace.SpanKind
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration.*

private[kafka] final case class LockedBrokerRecord[A](
  record: ConsumerRecord[String, String],
  decoded: A,
  metadata: BrokerRecordMetadata
)

private final case class LockedTopicInvariantViolation(message: String) extends IllegalStateException(message)

/** A single-topic/single-group durable consumer boundary. Each call allocates
  * a Kafka consumer and commits a batch only after every valid record in it has
  * been durably handled. Kafka committed offsets are the restart position.
  * Invalid payloads are parked before the corresponding batch is committed.
  */
private[kafka] object LockedTopicConsumer:
  private val log = StructuredLogger(getClass)

  def stream[A](
    cfg: KafkaCfg,
    groupId: String,
    topic: String,
    producer: KafkaProducer[IO, String, String],
    decode: String => Either[Throwable, A]
  )(
    process: List[LockedBrokerRecord[A]] => IO[Unit]
  ): Stream[IO, Unit] =
    Stream.eval(IO.fromEither(BrokerConsumerContract.from(groupId, topic))).flatMap { contract =>
      Stream
        .resource(KafkaConsumer.resource(KafkaComponents.consumerSettings(cfg, groupId)))
        .flatMap { consumer =>
          Stream.eval(preflightAndSubscribe(consumer, topic)) >> {
            val assignments = consumer.assignmentStream
              .evalMap(partitions => IO.fromEither(validateAssignments(topic, partitions)))
              .drain

            val records = consumer.partitionedStream.map { partitionStream =>
              partitionStream
                .groupWithin(cfg.lockedBatchSize, cfg.lockedBatchWindowMs.millis)
                .evalMap { committables =>
                  processBatch(
                    contract,
                    groupId,
                    topic,
                    committables.toList,
                    producer,
                    cfg.dlqSuffix,
                    decode,
                    process
                  )
                }
            }.parJoinUnbounded

            records.concurrently(assignments)
          }
        }
    }

  private def preflightAndSubscribe(
    consumer: KafkaConsumer[IO, String, String],
    topic: String
  ): IO[Unit] =
    for
      partitions <- consumer.partitionsFor(topic)
      _ <- IO.raiseWhen(partitions.isEmpty)(
        IllegalStateException(s"broker returned no partitions for locked topic $topic")
      )
      _ <- consumer.subscribeTo(topic)
    yield ()

  private def processBatch[A](
    contract: BrokerConsumerContract,
    groupId: String,
    expectedTopic: String,
    committables: List[CommittableConsumerRecord[IO, String, String]],
    producer: KafkaProducer[IO, String, String],
    dlqSuffix: String,
    decode: String => Either[Throwable, A],
    process: List[LockedBrokerRecord[A]] => IO[Unit]
  ): IO[Unit] =
    for
      prepared <- committables.traverse { committable =>
        prepareRecord(contract, groupId, expectedTopic, committable.record, decode).attempt.flatMap {
          case Right(locked) => IO.pure(Some(locked))
          case Left(error: LockedTopicInvariantViolation) => IO.raiseError(error)
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
      _ <- CoordinatorTracing.span(
        "kafka.consume.durable_batch",
        SpanKind.CONSUMER,
        "messaging.system" -> "kafka",
        "messaging.destination.name" -> expectedTopic,
        "messaging.consumer.group.name" -> groupId,
        "messaging.batch.message_count" -> committables.size.toString
      ) {
        process(prepared.flatten) *>
          CommittableOffsetBatch.fromFoldable(committables.map(_.offset)).commit
      }
    yield ()

  private def prepareRecord[A](
    contract: BrokerConsumerContract,
    groupId: String,
    expectedTopic: String,
    record: ConsumerRecord[String, String],
    decode: String => Either[Throwable, A]
  ): IO[LockedBrokerRecord[A]] =
    for
      _ <- IO.raiseWhen(record.topic != expectedTopic)(
        LockedTopicInvariantViolation(
          s"consumer group $groupId received unexpected topic ${record.topic}; expected $expectedTopic"
        )
      )
      rawValue <- IO.fromOption(Option(record.value))(
        IllegalArgumentException(
          s"tombstone is not valid for group=$groupId topic=${record.topic} " +
            s"partition=${record.partition} offset=${record.offset}"
        )
      )
      decoded <- IO.fromEither(decode(rawValue))
      metadata = BrokerRecordMetadata(
        topic = record.topic,
        partition = record.partition,
        offset = record.offset,
        consumerGroup = groupId,
        groupVersion = contract.groupVersion,
        artifactSha256 = contract.contractSha256,
        messageKey = Option(record.key),
        payloadSha256 = Sha256Utils.sha256Hex(rawValue)
      )
      _ <- IO(
        log.debug(
          "locked_consumer_record",
          "status" -> "prepared",
          "group" -> groupId,
          "topic" -> record.topic,
          "partition" -> record.partition.toString,
          "offset" -> record.offset.toString
        )
      )
    yield LockedBrokerRecord(record, decoded, metadata)

  private[kafka] def parkNonRetriable(
    producer: KafkaProducer[IO, String, String],
    dlqTopic: String,
    groupId: String,
    record: ConsumerRecord[String, String],
    error: Throwable
  ): IO[Unit] =
    val message = ErrorSanitizer.message(error)
    val body = Json
      .obj(
        "consumer_group" -> Json.fromString(groupId),
        "error" -> Json.fromString(message),
        "error_class" -> Json.fromString(error.getClass.getSimpleName),
        "offset" -> Json.fromLong(record.offset),
        "original" -> Option(record.value).fold(Json.Null)(Json.fromString),
        "partition" -> Json.fromInt(record.partition),
        "topic" -> Json.fromString(record.topic)
      )
      .noSpaces
    val key = Option(record.key).getOrElse(s"${record.partition}:${record.offset}")

    CoordinatorTracing.span(
      "kafka.publish.dlq",
      SpanKind.PRODUCER,
      "messaging.system" -> "kafka",
      "messaging.destination.name" -> dlqTopic
    ) {
      producer.produce(ProducerRecords.one(ProducerRecord(dlqTopic, key, body))).flatten.void
    } *>
      IO(
        log.error(
          "locked_consumer_record",
          "status" -> "parked",
          "group" -> groupId,
          "topic" -> record.topic,
          "partition" -> record.partition.toString,
          "offset" -> record.offset.toString,
          "dlq_topic" -> dlqTopic,
          "error" -> message
        )
      )

  private[kafka] def validateAssignments(
    expectedTopic: String,
    partitions: Iterable[TopicPartition]
  ): Either[Throwable, Unit] =
    val unexpected = partitions.iterator.filter(_.topic != expectedTopic).toList
    Either.cond(
      unexpected.isEmpty,
      (),
      IllegalStateException(
        s"consumer was assigned unexpected topics: " +
          unexpected.map(_.topic).distinct.sorted.mkString(",")
      )
    )
