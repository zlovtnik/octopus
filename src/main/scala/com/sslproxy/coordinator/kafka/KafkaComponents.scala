package com.sslproxy.coordinator.kafka

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.postgres.{PostgresLoad, PostgresResult}
import fs2.kafka.*
import io.circe.parser.decode as circeDecode
import io.circe.syntax.*
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, CreateTopicsOptions, ListTopicsOptions, NewTopic}
import org.apache.kafka.common.errors.{InvalidReplicationFactorException, RetriableException, TopicExistsException}
import com.sslproxy.coordinator.observability.StructuredLogger

import java.util.{Collections, Properties}
import java.time.Duration as JavaDuration
import java.util.concurrent.{ExecutionException, TimeUnit}
import scala.concurrent.duration.*

final class KafkaComponents(
  val producer: KafkaProducer[IO, String, String],
  val config: KafkaCfg
)

object KafkaComponents:
  private val log = StructuredLogger(getClass)

  def resource(cfg: KafkaCfg): Resource[IO, KafkaComponents] =
    createProducer(cfg).map(producer => KafkaComponents(producer, cfg))

  /** Create the topic on the broker if it does not already exist. */
  private def ensureTopicExists(
    cfg: KafkaCfg,
    topic: String,
    timeout: FiniteDuration
  ): IO[Unit] =
    IO.blocking:
      val timeoutMs = timeout.toMillis.max(1L).min(Int.MaxValue.toLong).toInt
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.bootstrapServers)
      val admin = Admin.create(props)
      try
        val listOptions = new ListTopicsOptions().timeoutMs(timeoutMs)
        val existing = admin.listTopics(listOptions).names().get(timeoutMs.toLong, TimeUnit.MILLISECONDS)
        if !existing.contains(topic) then
          val replicationFactor = cfg.topicReplicationFactor.toShort
          val partitions = provisionedTopicPartitions(cfg, topic)
          val newTopic = new NewTopic(topic, partitions, replicationFactor)
          try
            val createOptions = new CreateTopicsOptions().timeoutMs(timeoutMs)
            admin
              .createTopics(Collections.singletonList(newTopic), createOptions)
              .all()
              .get(timeoutMs.toLong, TimeUnit.MILLISECONDS)
            log.info(
              "topic_provision",
              "status" -> "created",
              "topic" -> topic,
              "partitions" -> partitions.toString,
              "replication" -> replicationFactor.toString
            )
          catch
            case error if isTopicAlreadyExists(error) =>
              log.debug("topic_provision", "status" -> "exists", "topic" -> topic)
            case error if isInvalidReplicationFactor(error) =>
              log.warn(
                "topic_provision",
                "status" -> "replication_factor_mismatch",
                "topic" -> topic,
                "replication" -> replicationFactor.toString,
                "error" -> error.getMessage
              )
        else log.debug("topic_provision", "status" -> "exists", "topic" -> topic)
      finally admin.close(JavaDuration.ofMillis(timeoutMs.toLong))

  private[kafka] def isTopicAlreadyExists(error: Throwable): Boolean =
    error match
      case _: TopicExistsException => true
      case execution: ExecutionException => execution.getCause.isInstanceOf[TopicExistsException]
      case _ => false

  private[kafka] def isInvalidReplicationFactor(error: Throwable): Boolean =
    error match
      case _: InvalidReplicationFactorException => true
      case execution: ExecutionException => execution.getCause.isInstanceOf[InvalidReplicationFactorException]
      case _ => false

  private[kafka] def provisionedTopicPartitions(cfg: KafkaCfg, topic: String): Int =
    val baseTopic =
      if cfg.dlqSuffix.nonEmpty && topic.endsWith(cfg.dlqSuffix) then topic.dropRight(cfg.dlqSuffix.length)
      else topic
    val lockedTopics = Set(cfg.scanTopic, cfg.loadTopic, cfg.resultTopic)
    if lockedTopics.contains(baseTopic) then cfg.topicPartitions else 3

  /** Ensure the topic exists on the broker, then block until it is ready for
    * consuming.  Uses a temporary consumer to probe `partitionsFor` with retry.
    * A single consumer is reused across retries to avoid ephemeral group churn.
    */
  def waitForTopic(
    cfg: KafkaCfg,
    topic: String,
    timeout: FiniteDuration = 30.seconds,
    retryInterval: FiniteDuration = 2.seconds
  ): IO[Unit] =
    val settings = ConsumerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withGroupId(s"preflight-${topic}-${java.util.UUID.randomUUID()}")
      .withProperties("allow.auto.create.topics" -> "false")

    def probe(
      consumer: KafkaConsumer[IO, String, String]
    ): IO[Either[Throwable, List[org.apache.kafka.common.PartitionInfo]]] =
      consumer.partitionsFor(topic).attempt

    def loop(consumer: KafkaConsumer[IO, String, String], deadline: FiniteDuration): IO[Unit] =
      probe(consumer).flatMap {
        case Right(partitions) if partitions.isEmpty =>
          IO(log.warn("topic_preflight", "status" -> "empty", "topic" -> topic)) *>
            retryOrTimeout(deadline) *> loop(consumer, deadline)
        case Right(_) =>
          IO(log.info("topic_preflight", "status" -> "ready", "topic" -> topic))
        case Left(ex: RetriableException) =>
          IO(log.warn("topic_preflight", "status" -> "waiting", "topic" -> topic, "error" -> ex.getMessage)) *>
            retryOrTimeout(deadline) *> loop(consumer, deadline)
        case Left(ex) =>
          IO.raiseError(ex)
      }

    def retryOrTimeout(deadline: FiniteDuration): IO[Unit] =
      IO.monotonic.flatMap { now =>
        if now >= deadline then
          IO.raiseError(new IllegalStateException(s"topic $topic did not become available within $timeout"))
        else IO.sleep(retryInterval)
      }

    IO.monotonic.flatMap { start =>
      ensureTopicExists(cfg, topic, timeout).timeout(timeout) *>
        KafkaConsumer.resource(settings).use(consumer => loop(consumer, start + timeout))
    }

  /** Every processor gets its own KafkaConsumer resource. There is deliberately
    * no shared consumer here: a subscription and group identity are part of the
    * processor's durable contract.
    */
  private[kafka] def consumerSettings(
    cfg: KafkaCfg,
    groupId: String
  ): ConsumerSettings[IO, String, String] =
    ConsumerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)
      .withIsolationLevel(IsolationLevel.ReadCommitted)
      .withMaxPollRecords(cfg.maxPollRecords)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "session.timeout.ms" -> "30000",
        "heartbeat.interval.ms" -> "3000"
      )

  private def createProducer(cfg: KafkaCfg): Resource[IO, KafkaProducer[IO, String, String]] =
    val producerSettings = ProducerSettings[IO, String, String]
      .withBootstrapServers(cfg.bootstrapServers)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "enable.idempotence" -> "true",
        "acks" -> "all",
        "retries" -> "3"
      )

    fs2.kafka.KafkaProducer.resource(producerSettings)

  def deserializeLoad(json: String): Either[Throwable, PostgresLoad] =
    circeDecode[PostgresLoad](json)

  def deserializeResult(json: String): Either[Throwable, PostgresResult] =
    circeDecode[PostgresResult](json)

  def serializeResult(result: PostgresResult): String =
    result.asJson.noSpaces
