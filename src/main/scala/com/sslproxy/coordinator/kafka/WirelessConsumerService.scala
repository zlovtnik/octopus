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
import io.circe.syntax.*
import com.sslproxy.coordinator.observability.StructuredLogger

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.time.temporal.ChronoUnit
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

  def backlogSaveStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.backlogSaveConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.backlogSaveTopic, cfg.consumersCount, kafkaCfg) { committable =>
      handleBacklogSave(
        committable.record.value,
        repo,
        producer,
        cfg.backlogSaveTopic + cfg.dlqSuffix,
        dbSemaphore
      ).as(committable.offset)
    }

  def backlogListStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.backlogListConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.backlogListTopic, cfg.consumersCount, kafkaCfg) { committable =>
      handleBacklogList(
        committable.record.value,
        cfg.backlogListReplyTopic,
        repo,
        producer,
        cfg.backlogListTopic + cfg.dlqSuffix,
        dbSemaphore
      ).as(committable.offset)
    }

  def backlogSyncedStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.backlogSyncedConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.backlogSyncedTopic, cfg.consumersCount, kafkaCfg) { committable =>
      handleBacklogSynced(
        committable.record.value,
        repo,
        producer,
        cfg.backlogSyncedTopic + cfg.dlqSuffix,
        dbSemaphore
      ).as(committable.offset)
    }

  def backlogPruneStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dbSemaphore: Semaphore[IO]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.backlogPruneConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.backlogPruneTopic, cfg.consumersCount, kafkaCfg) { committable =>
      handleBacklogPrune(
        committable.record.value,
        cfg.backlogPruneReplyTopic,
        repo,
        producer,
        cfg.backlogPruneTopic + cfg.dlqSuffix,
        dbSemaphore
      ).as(committable.offset)
    }

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
    backlogSaveStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore)
      .merge(backlogListStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore))
      .merge(backlogSyncedStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore))
      .merge(backlogPruneStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore))
      .merge(macLookupStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore))
      .merge(networksAuthorizedStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore))
      .merge(probeFlushStream(cfg, kafkaCfg, pgRepo, producer, dbSemaphore))

  private def handleBacklogSave(
      payload: String,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String,
      dbSemaphore: Semaphore[IO]
  ): IO[Unit] =
    parseBacklogIdentity(payload) match
      case Left(error) => publishInvalidPayload(producer, dlqTopic, payload, "backlog_save", error)
      case Right((json, dedupeKey, streamName)) =>
        val stage = json.hcursor.get[String]("failure_stage").getOrElse("pre_publish")
        if !Set("pre_publish", "post_publish").contains(stage) then
          publishInvalidPayload(producer, dlqTopic, payload, "backlog_save", "invalid failure_stage")
        else
          val storedPayload = json.hcursor.downField("payload").focus.getOrElse(json)
          retryDatabase("backlog_save", payload, dlqTopic, producer) {
            dbSemaphore.permit.use(_ => repo.saveWirelessBacklog(dedupeKey, streamName, storedPayload, stage))
          }(_ => IO.unit)

  private def handleBacklogList(
      payload: String,
      defaultReplyTopic: String,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String,
      dbSemaphore: Semaphore[IO]
  ): IO[Unit] =
    retryDatabase("backlog_list", payload, dlqTopic, producer) {
      dbSemaphore.permit.use(_ => repo.listPendingWirelessBacklog(100))
    } { entries =>
      val body = Json.obj("entries" -> entries.map { entry =>
        Json.obj(
          "dedupe_key" -> entry.dedupeKey.asJson,
          "stream_name" -> entry.streamName.asJson,
          "payload" -> entry.payload,
          "failure_stage" -> entry.failureStage.asJson,
          "attempt_count" -> entry.attemptCount.asJson,
          "created_at" -> entry.createdAt.toString.asJson
        )
      }.asJson).noSpaces
      publishReply(producer, resolveReplyTopic(payload, defaultReplyTopic), body)
    }

  private def handleBacklogSynced(
      payload: String,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String,
      dbSemaphore: Semaphore[IO]
  ): IO[Unit] =
    parseBacklogIdentity(payload) match
      case Left(error) => publishInvalidPayload(producer, dlqTopic, payload, "backlog_synced", error)
      case Right((_, dedupeKey, streamName)) =>
        retryDatabase("backlog_synced", payload, dlqTopic, producer) {
          dbSemaphore.permit.use(_ => repo.markWirelessBacklogSynced(dedupeKey, streamName))
        }(_ => IO.unit)

  private def handleBacklogPrune(
      payload: String,
      defaultReplyTopic: String,
      repo: TidbRepository,
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String,
      dbSemaphore: Semaphore[IO]
  ): IO[Unit] =
    cats.effect.Clock[IO].realTimeInstant.flatMap { now =>
      retryDatabase("backlog_prune", payload, dlqTopic, producer) {
        dbSemaphore.permit.use(_ => repo.pruneWirelessBacklog(now.minus(7L, ChronoUnit.DAYS)))
      } { pruned =>
        val body = Json.obj("pruned" -> pruned.asJson, "retention_days" -> 7.asJson).noSpaces
        publishReply(producer, resolveReplyTopic(payload, defaultReplyTopic), body)
      }
    }

  private[kafka] def parseBacklogIdentity(payload: String): Either[String, (Json, String, String)] =
    for
      json <- parseJson(Option(payload).getOrElse("")).left.map(_.getMessage)
      dedupeKey <- json.hcursor.get[String]("dedupe_key").left.map(_.getMessage).filterOrElse(_.nonEmpty, "blank dedupe_key")
      streamName <- json.hcursor.get[String]("stream_name").left.map(_.getMessage).filterOrElse(_.nonEmpty, "blank stream_name")
    yield (json, dedupeKey, streamName)

  private def retryDatabase[A](
      operation: String,
      payload: String,
      dlqTopic: String,
      producer: KafkaProducer[IO, String, String],
      remaining: Int = MaxRetries
  )(database: => IO[Either[DatabaseError, A]])(onSuccess: A => IO[Unit]): IO[Unit] =
    database.flatMap {
      case Right(value) => onSuccess(value)
      case Left(_) if remaining > 1 =>
        IO.sleep(RetryDelay * (1L << (MaxRetries - remaining))) *>
          retryDatabase(operation, payload, dlqTopic, producer, remaining - 1)(database)(onSuccess)
      case Left(error) => publishDlq(producer, dlqTopic, operation, payload, error)
    }

  private def publishInvalidPayload(
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String,
      payload: String,
      operation: String,
      message: String
  ): IO[Unit] =
    val cause = IllegalArgumentException(message)
    publishDlq(producer, dlqTopic, operation, payload, DatabaseError.Permanent(operation, cause, message))

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
      extractField(payload, "mac").flatMap(normalizeMac) match
        case None =>
          IO(log.warn("mac_lookup", "status" -> "skip", "error" -> "missing or invalid mac field"))
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
            IO.sleep(RetryDelay * (1L << (MaxRetries - remaining))) *>
            attemptWithRetry(payload, pgRepo, remaining - 1, dlqTopic, producer, dbSemaphore)
        case Left(err) =>
          IO(log.error("probe_flush", "status" -> "dlq",
            "topic" -> dlqTopic, "error" -> err.message)) *>
            publishDlq(producer, dlqTopic, "probe_flush", payload, err).handleErrorWith { publishError =>
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
      topic == "wireless.backlog.list.reply" ||
      topic == "wireless.backlog.prune.reply" ||
      topic == "wireless.mac.lookup.reply" ||
      topic == "wireless.networks.authorized.reply"

  private[kafka] def extractField(payload: String, field: String): Option[String] =
    parseJson(payload).toOption.flatMap { json =>
      json.hcursor.downField(field).as[String].toOption.filter(_.nonEmpty)
  }

  private[kafka] def hashMac(mac: String): Option[String] =
    normalizeMac(mac).map { normalized =>
      val input = MacHashSalt ++ normalized.getBytes(StandardCharsets.UTF_8)
      Sha256Utils.sha256Hex(input).take(24)
    }

  private[kafka] def normalizeMac(mac: String): Option[String] =
    Option(mac)
      .map(_.trim.toLowerCase(java.util.Locale.ROOT))
      .filter(MacPattern.matches)

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
      operation: String,
      original: String,
      err: DatabaseError
  ): IO[Unit] =
    val dlqBody = Json.obj(
      "original" -> Json.fromString(original),
      "error" -> Json.fromString(Option(err.message).getOrElse("")),
      "operation" -> Json.fromString(err.operation)
    ).noSpaces
    val record = ProducerRecords.one(ProducerRecord(topic, operation, dlqBody))
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
