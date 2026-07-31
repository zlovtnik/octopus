package com.sslproxy.coordinator.kafka

import cats.effect.IO
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.{KafkaCfg, WirelessConfig}
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.tidb.TidbRepository
import com.sslproxy.coordinator.util.Sha256Utils
import fs2.Stream
import fs2.kafka.*
import io.circe.Json
import io.circe.parser.parse as parseJson
import com.sslproxy.coordinator.observability.StructuredLogger

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import scala.concurrent.duration.*

object WirelessConsumerService:
  private val log = StructuredLogger(getClass)
  private val TopicPattern = """[A-Za-z0-9._-]{1,249}""".r
  private val SensorInboxPrefix = "_INBOX.atheros_sensor."
  private val MaxRetries = 3
  private val RetryDelay = 500.millis
  private val MacPattern = "(?i)^[0-9a-f]{2}(?::[0-9a-f]{2}){5}$".r
  private val MacHashSalt: Array[Byte] =
    val bytes = new Array[Byte](32)
    new SecureRandom().nextBytes(bytes)
    bytes

  def macLookupStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.macLookupConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.macLookupTopic, cfg.consumersCount, kafkaCfg) { committable =>
      val payload = committable.record.value
      handleMacLookup(payload, cfg.macLookupReplyTopic, pgRepo, producer, dbSemaphore).as(committable.offset)
    }

  def networksAuthorizedStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.networksAuthorizedConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.networksAuthorizedTopic, cfg.consumersCount, kafkaCfg) { committable =>
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
      kafkaCfg: KafkaCfg,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.probeFlushConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    val dlqTopic = cfg.probeFlushTopic + cfg.dlqSuffix
    wirelessStream(settings, cfg.probeFlushTopic, cfg.consumersCount, kafkaCfg) { committable =>
      val payload = committable.record.value
      handleProbeFlush(payload, pgRepo, producer, dlqTopic, dbSemaphore).as(committable.offset)
    }

  def allStreams(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      pgRepo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    macLookupStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore)
      .merge(networksAuthorizedStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore))
      .merge(probeFlushStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore))

  private def wirelessStream(
      settings: ConsumerSettings[IO, String, String],
      topic: String,
      consumersCount: Int,
      kafkaCfg: KafkaCfg
  )(
      process: CommittableConsumerRecord[IO, String, String] => IO[CommittableOffset[IO]]
  ): Stream[IO, Unit] =
    Stream
      .eval(KafkaComponents.waitForTopic(kafkaCfg, topic))
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
      extractField(payload, "mac") match
        case None =>
          IO(log.warn("mac_lookup", "status" -> "skip", "error" -> "missing mac field"))
        case Some(mac) =>
          val macHashFields = hashMac(mac).toList.map("mac_hash" -> _)
          IO(log.debug("mac_lookup", (("status" -> "processing") :: macHashFields)*)) *>
            dbSemaphore.permit.use(_ => pgRepo.lookupDeviceByMac(mac)).attempt.flatMap {
              case Left(err) =>
                IO(log.warn("mac_lookup", "status" -> "skip",
                  "error" -> errorMessage(err)))
              case Right(Right(Some(reply))) =>
                val replyTopic = resolveReplyTopic(payload, defaultReplyTopic)
                IO(log.info("mac_lookup", (("status" -> "found") ::
                  ("reply_topic" -> replyTopic) :: macHashFields)*)) *>
                  publishReply(producer, replyTopic, reply).handleErrorWith { err =>
                    IO(log.error("mac_lookup", "status" -> "reply_publish_failed",
                      "reply_topic" -> replyTopic, "error" -> errorMessage(err)))
                  }
              case Right(Right(None)) =>
                IO(log.info("mac_lookup", (("status" -> "not_found") :: macHashFields)*))
              case Right(Left(err)) =>
                IO(log.error("mac_lookup", "status" -> "db_error", "error" -> err.message))
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
            publishReply(producer, replyTopic, reply).handleErrorWith { err =>
              IO(log.error("networks_authorized", "status" -> "reply_publish_failed",
                "reply_topic" -> replyTopic, "error" -> errorMessage(err)))
            }
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
            publishDlq(producer, dlqTopic, payload, err).handleErrorWith { publishError =>
              IO(log.error("probe_flush", "status" -> "dlq_publish_failed",
                "topic" -> dlqTopic, "error" -> errorMessage(publishError)))
            }
      }

  private def errorMessage(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName)

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

  private[kafka] def hashMac(mac: String): Option[String] =
    Option(mac).map(_.trim.toLowerCase(java.util.Locale.ROOT)).filter(MacPattern.matches).map { normalized =>
      val input = MacHashSalt ++ normalized.getBytes(StandardCharsets.UTF_8)
      Sha256Utils.sha256Hex(input).take(24)
    }

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
