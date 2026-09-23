package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.{BrokerConsumerContract, BrokerRecordMetadata}
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, CoordinatorTracing, StructuredLogger}
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

import java.nio.charset.StandardCharsets
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
    partitionConcurrency: Int,
    awaitConsumerPermit: IO[Unit],
    metrics: CoordinatorMetrics,
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

            val records = Stream.eval(Semaphore[IO](partitionConcurrency.toLong)).flatMap { permits =>
              consumer.partitionedStream.map { partitionStream =>
                BoundedBatching.groupWithin(
                  partitionStream.evalTap(_ => awaitConsumerPermit),
                  cfg.lockedBatchSize,
                  cfg.lockedBatchMaxBytes,
                  cfg.lockedBatchWindowMs.millis
                )(record => record.record.serializedValueSize.getOrElse(utf8Bytes(Option(record.record.value).getOrElse(""))).toLong)
                .evalMap { committables =>
                  awaitConsumerPermit *>
                    permits.permit.use(_ => processBatch(
                      contract,
                      groupId,
                      topic,
                      committables.toList,
                      producer,
                      cfg.dlqSuffix,
                      decode,
                      process,
                      cfg.lockedBatchMaxBytes
                    ))
                }
              // Partition streams live until revocation. Capping their join starves
              // every partition beyond the cap; bound batch work with permits instead.
              }.parJoinUnbounded
            }

            val lag = Stream.repeatEval(
              consumer.metrics.flatMap(values => IO(metrics.recordKafkaMetrics(groupId, values)))
                .timeout(5.seconds)
                .handleErrorWith(error => IO(log.warn("consumer_metrics", "group" -> groupId,
                  "error" -> ErrorSanitizer.message(error))))
            ).metered(10.seconds).onFinalize(IO(metrics.clearKafkaMetrics(groupId)))
            records.concurrently(assignments).concurrently(lag)
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
    process: List[LockedBrokerRecord[A]] => IO[Unit],
    maxBatchBytes: Int
  ): IO[Unit] =
    splitByBytes(committables, maxBatchBytes).traverse_(batch =>
      processBoundedBatch(contract, groupId, expectedTopic, batch, producer, dlqSuffix, decode, process, maxBatchBytes)
    )

  private def processBoundedBatch[A](
    contract: BrokerConsumerContract,
    groupId: String,
    expectedTopic: String,
    committables: List[CommittableConsumerRecord[IO, String, String]],
    producer: KafkaProducer[IO, String, String],
    dlqSuffix: String,
    decode: String => Either[Throwable, A],
    process: List[LockedBrokerRecord[A]] => IO[Unit],
    maxBatchBytes: Int
  ): IO[Unit] =
    for
      prepared <- committables.traverse { committable =>
        prepareRecord(contract, groupId, expectedTopic, committable.record, decode, maxBatchBytes).attempt.flatMap {
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

  private[kafka] def splitByBytes[A](
    records: List[A],
    maxBytes: Int,
    sizeOf: A => Int
  ): List[List[A]] =
    require(maxBytes > 0, "maxBytes must be positive")
    val batches = List.newBuilder[List[A]]
    val current = List.newBuilder[A]
    var currentBytes = 0L
    var currentCount = 0
    records.foreach { record =>
      val recordBytes = sizeOf(record).toLong
      if currentCount > 0 && currentBytes + recordBytes > maxBytes then
        batches += current.result()
        current.clear()
        currentBytes = 0L
        currentCount = 0
      current += record
      currentBytes += recordBytes
      currentCount += 1
    }
    if currentCount > 0 then batches += current.result()
    batches.result()

  private def splitByBytes(
    records: List[CommittableConsumerRecord[IO, String, String]],
    maxBytes: Int
  ): List[List[CommittableConsumerRecord[IO, String, String]]] =
    splitByBytes(records, maxBytes, record => utf8Bytes(Option(record.record.value).getOrElse("")))

  private def prepareRecord[A](
    contract: BrokerConsumerContract,
    groupId: String,
    expectedTopic: String,
    record: ConsumerRecord[String, String],
    decode: String => Either[Throwable, A],
    maxBatchBytes: Int
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
      _ <- IO.raiseWhen(utf8Bytes(rawValue) > maxBatchBytes)(
        IllegalArgumentException(
          s"record exceeds kafka.locked-batch-max-bytes=$maxBatchBytes"
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

  private def utf8Bytes(value: String): Int =
    value.getBytes(StandardCharsets.UTF_8).length

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
