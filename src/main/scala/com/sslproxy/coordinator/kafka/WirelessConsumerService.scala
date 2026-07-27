package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.WirelessConfig
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.tidb.TidbRepository
import fs2.Stream
import fs2.kafka.*
import io.circe.Json
import io.circe.parser.parse as parseJson
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

object WirelessConsumerService:
  private val log = StructuredLogger(getClass)
  private val TopicPattern = """[A-Za-z0-9._-]{1,249}""".r
  private val SensorInboxPrefix = "_INBOX.atheros_sensor."
  private val MaxRetries = 3
  private val RetryDelay = 500.millis

  def macLookupStream(
      cfg: WirelessConfig,
      bootstrapServers: String,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.macLookupConsumer, cfg.maxPollRecords, bootstrapServers)
    wirelessStream(settings, cfg.macLookupTopic, cfg.consumersCount, bootstrapServers) { committable =>
      val payload = committable.record.value
      handleMacLookup(payload, cfg.macLookupReplyTopic, pgRepo, producer, dbSemaphore).as(committable.offset)
    }

  def networksAuthorizedStream(
      cfg: WirelessConfig,
      bootstrapServers: String,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.networksAuthorizedConsumer, cfg.maxPollRecords, bootstrapServers)
    wirelessStream(settings, cfg.networksAuthorizedTopic, cfg.consumersCount, bootstrapServers) { committable =>
      val payload = committable.record.value
      handleNetworksAuthorized(
        payload,
        cfg.networksAuthorizedReplyTopic,
        pgRepo,
        producer,
        dbSemaphore
      ).as(committable.offset)
    }

  def probeFlushStream(
      cfg: WirelessConfig,
      bootstrapServers: String,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.probeFlushConsumer, cfg.maxPollRecords, bootstrapServers)
    val dlqTopic = cfg.probeFlushTopic + cfg.dlqSuffix
    wirelessStream(settings, cfg.probeFlushTopic, cfg.consumersCount, bootstrapServers) { committable =>
      val payload = committable.record.value
      handleProbeFlush(payload, pgRepo, producer, dlqTopic, dbSemaphore).as(committable.offset)
    }

  def allStreams(
      cfg: WirelessConfig,
      bootstrapServers: String,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    macLookupStream(cfg, bootstrapServers, pgRepo, producer, dbSemaphore)
      .merge(networksAuthorizedStream(cfg, bootstrapServers, pgRepo, producer, dbSemaphore))
      .merge(probeFlushStream(cfg, bootstrapServers, pgRepo, producer, dbSemaphore))

  private def wirelessStream(
      settings: ConsumerSettings[IO, String, String],
      topic: String,
      consumersCount: Int,
      bootstrapServers: String
  )(
      process: CommittableConsumerRecord[IO, String, String] => IO[CommittableOffset[IO]]
  ): Stream[IO, Unit] =
    Stream
      .eval(KafkaComponents.waitForTopic(bootstrapServers, topic))
      .flatMap(_ =>
        Stream.resource(fs2.kafka.KafkaConsumer.resource(settings))
      )
      .flatMap { consumer =>
        Stream.eval(consumer.subscribeTo(topic)) >>
        consumer.partitionedStream
          .map { partitionStream =>
            partitionStream.evalMap(process)
          }
          .parJoin(consumersCount)
          .through(commitBatch)
      }

  private def handleMacLookup(
      payload: String,
      defaultReplyTopic: String,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): IO[Unit] =
    if payload == null || payload.isEmpty then IO.unit
    else
      val result = for
        mac <- IO.fromOption(extractField(payload, "mac"))(
                 new IllegalArgumentException("missing mac field"))
        _   <- IO(log.warn("mac_lookup", "status" -> "processing", "mac_hash" -> hashMac(mac)))
        lookup <- dbSemaphore.permit.use(_ => pgRepo.lookupDeviceByMac(mac))
        _   <- lookup match
                case Right(Some(reply)) =>
                  val replyTopic = resolveReplyTopic(payload, defaultReplyTopic)
                  IO(log.info("mac_lookup", "status" -> "found",
                    "reply_topic" -> replyTopic, "mac_hash" -> hashMac(mac))) *>
                    publishReply(producer, replyTopic, reply)
                case Right(None) =>
                  IO(log.info("mac_lookup", "status" -> "not_found", "mac_hash" -> hashMac(mac)))
                case Left(err) =>
                  IO(log.error("mac_lookup", "status" -> "db_error", "error" -> err.message))
      yield ()

      result.handleErrorWith { err =>
        IO(log.warn("mac_lookup", "status" -> "skip", "error" -> err.getMessage)) *> IO.unit
      }

  private def handleNetworksAuthorized(
      payload: String,
      defaultReplyTopic: String,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): IO[Unit] =
    if payload == null || payload.isEmpty then IO.unit
    else
      dbSemaphore.permit.use(_ => pgRepo.listAuthorizedNetworks()).flatMap {
        case Right(reply) =>
          val replyTopic = resolveReplyTopic(payload, defaultReplyTopic)
          IO(log.info("networks_authorized", "status" -> "ok", "reply_topic" -> replyTopic)) *>
            publishReply(producer, replyTopic, reply)
        case Left(err) =>
          IO(log.error("networks_authorized", "status" -> "db_error", "error" -> err.message))
      }

  private def handleProbeFlush(
      payload: String,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String,
      dbSemaphore: Semaphore[IO]
  ): IO[Unit] =
    if payload == null || payload.isEmpty then IO.unit
    else attemptWithRetry(payload, pgRepo, MaxRetries, dlqTopic, producer, dbSemaphore)

  private def attemptWithRetry(
      payload: String,
      pgRepo: TidbRepository,
      remaining: Int,
      dlqTopic: String,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): IO[Unit] =
    IO(log.info("probe_flush", "status" -> "processing", "payload_bytes" -> payload.length.toString)) *>
      dbSemaphore.permit.use(_ => pgRepo.flushProbeBatch(payload)).flatMap {
        case Right(count) =>
          IO(log.info("probe_flush", "status" -> "ok",
            "records_inserted" -> count.toString, "payload_bytes" -> payload.length.toString))
        case Left(err) if remaining > 1 =>
          IO(log.warn("probe_flush", "status" -> "retry",
            "attempts_remaining" -> (remaining - 1).toString, "error" -> err.message)) *>
            IO.sleep(RetryDelay) *>
            attemptWithRetry(payload, pgRepo, remaining - 1, dlqTopic, producer, dbSemaphore)
        case Left(err) =>
          IO(log.error("probe_flush", "status" -> "dlq",
            "topic" -> dlqTopic, "error" -> err.message)) *>
            publishDlq(producer, dlqTopic, payload, err)
      }

  private[kafka] def resolveReplyTopic(payload: String, defaultTopic: String): String =
    extractField(payload, "reply_topic") match
      case Some(t) if isValidKafkaTopic(t) && isAllowedReplyTopic(t) =>
        log.debug("resolve_reply_topic", "status" -> "valid", "topic" -> t)
        t
      case _ =>
        defaultTopic

  private[kafka] def isValidKafkaTopic(topic: String): Boolean =
    TopicPattern.matches(topic) && topic != "." && topic != ".."

  private[kafka] def isAllowedReplyTopic(topic: String): Boolean =
    topic == SensorInboxPrefix.dropRight(1) || topic.startsWith(SensorInboxPrefix) ||
      topic == "wireless.mac.lookup.reply" ||
      topic == "wireless.networks.authorized.reply"

  private[kafka] def extractField(payload: String, field: String): Option[String] =
    parseJson(payload).toOption.flatMap { json =>
      json.hcursor.downField(field).as[String].toOption.filter(_.nonEmpty)
  }

  private[kafka] def hashMac(mac: String): String =
    if mac == null || mac.length < 4 then "invalid"
    else mac.take(2) + "***" + mac.takeRight(2)

  private def publishReply(
      producer: KafkaProducer[IO, String, String],
      topic: String,
      value: String
  ): IO[Unit] =
    val record = ProducerRecords.one(ProducerRecord(topic, "", value))
    producer.produce(record).flatten.void

  private def publishDlq(
      producer: KafkaProducer[IO, String, String],
      topic: String,
      original: String,
      err: DatabaseError
  ): IO[Unit] =
    val dlqBody = Json.obj(
      "original" -> Json.fromString(original),
      "error" -> Json.fromString(Option(err.message).getOrElse("")),
      "operation" -> Json.fromString(err.operation)
    ).noSpaces
    val record = ProducerRecords.one(ProducerRecord(topic, "probe_flush", dlqBody))
    producer.produce(record).flatten.void

  private def consumerSettings(
      groupId: String,
      maxPollRecords: Int,
      bootstrapServers: String
  ): ConsumerSettings[IO, String, String] =
    ConsumerSettings[IO, String, String]
      .withBootstrapServers(bootstrapServers)
      .withGroupId(groupId)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withMaxPollRecords(maxPollRecords)
      .withProperties(
        "allow.auto.create.topics" -> "false",
        "session.timeout.ms" -> "30000",
        "heartbeat.interval.ms" -> "3000"
      )

  private def commitBatch: fs2.Pipe[IO, CommittableOffset[IO], Unit] =
    _.groupWithin(500, 15.seconds)
      .evalMap(CommittableOffsetBatch.fromFoldable(_).commit)
