package com.sslproxy.coordinator.kafka

import cats.effect.IO
import com.sslproxy.coordinator.config.{KafkaCfg, WirelessConfig}
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.persistence.WirelessStore
import com.sslproxy.coordinator.util.{ErrorSanitizer,Sha256Utils}
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
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.backlogSaveConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.backlogSaveTopic, cfg.backlogSaveConsumer, cfg.consumersCount, kafkaCfg, producer) { committable =>
      handleBacklogSave(
        committable.record.value,
        store,
        producer,
        cfg.backlogSaveTopic + kafkaCfg.dlqSuffix
      ).as(committable.offset)
    }

  def backlogListStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.backlogListConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.backlogListTopic, cfg.backlogListConsumer, cfg.consumersCount, kafkaCfg, producer) { committable =>
      handleBacklogList(
        committable.record.value,
        cfg.backlogListReplyTopic,
        configuredReplyTopics(cfg),
        store,
        producer,
        cfg.backlogListTopic + kafkaCfg.dlqSuffix
      ).as(committable.offset)
    }

  def backlogSyncedStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.backlogSyncedConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.backlogSyncedTopic, cfg.backlogSyncedConsumer, cfg.consumersCount, kafkaCfg,
      producer) { committable =>
      handleBacklogSynced(
        committable.record.value,
        store,
        producer,
        cfg.backlogSyncedTopic + kafkaCfg.dlqSuffix
      ).as(committable.offset)
    }

  def backlogPruneStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.backlogPruneConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.backlogPruneTopic, cfg.backlogPruneConsumer, cfg.consumersCount, kafkaCfg, producer) { committable =>
      handleBacklogPrune(
        committable.record.value,
        cfg.backlogPruneReplyTopic,
        configuredReplyTopics(cfg),
        store,
        producer,
        cfg.backlogPruneTopic + kafkaCfg.dlqSuffix
      ).as(committable.offset)
    }

  def macLookupStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.macLookupConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.macLookupTopic, cfg.macLookupConsumer, cfg.consumersCount, kafkaCfg, producer) { committable =>
      val payload = committable.record.value
      handleMacLookup(payload, cfg.macLookupReplyTopic, configuredReplyTopics(cfg), store, producer).as(committable.offset)
    }

  def networksAuthorizedStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.networksAuthorizedConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    wirelessStream(settings, cfg.networksAuthorizedTopic, cfg.networksAuthorizedConsumer, cfg.consumersCount, kafkaCfg,
      producer) { committable =>
      val payload = committable.record.value
      handleNetworksAuthorized(
        payload,
        cfg.networksAuthorizedReplyTopic,
        configuredReplyTopics(cfg),
        store,
        producer
      ).as(committable.offset)
    }

  def probeFlushStream(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    val settings = consumerSettings(cfg.probeFlushConsumer, cfg.maxPollRecords, kafkaCfg.bootstrapServers)
    val dlqTopic = cfg.probeFlushTopic + kafkaCfg.dlqSuffix
    wirelessStream(settings, cfg.probeFlushTopic, cfg.probeFlushConsumer, cfg.consumersCount, kafkaCfg, producer) { committable =>
      val payload = committable.record.value
      handleProbeFlush(payload, store, producer, dlqTopic).as(committable.offset)
    }

  def allStreams(
      cfg: WirelessConfig,
      kafkaCfg: KafkaCfg,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): Stream[IO, Unit] =
    backlogSaveStream(cfg, kafkaCfg, store, producer)
      .merge(backlogListStream(cfg, kafkaCfg, store, producer))
      .merge(backlogSyncedStream(cfg, kafkaCfg, store, producer))
      .merge(backlogPruneStream(cfg, kafkaCfg, store, producer))
      .merge(macLookupStream(cfg, kafkaCfg, store, producer))
      .merge(networksAuthorizedStream(cfg, kafkaCfg, store, producer))
      .merge(probeFlushStream(cfg, kafkaCfg, store, producer))

  private def handleBacklogSave(
      payload: String,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String
  ): IO[Unit] =
    parseBacklogIdentity(payload) match
      case Left(error) => publishInvalidPayload(producer, dlqTopic, payload, "backlog_save", error)
      case Right((json, dedupeKey, streamName)) =>
        val stage = json.hcursor.get[String]("failure_stage").getOrElse("pre_publish")
        if !Set("pre_publish", "post_publish").contains(stage) then
          publishInvalidPayload(producer, dlqTopic, payload, "backlog_save", "invalid failure_stage")
        else
          val storedPayload = json.hcursor.downField("payload").focus.getOrElse(json)
          retryDatabase("backlog_save", payload, dlqTopic, producer) { store.saveBacklog(dedupeKey, streamName, storedPayload, stage).value
          }(_ => IO.unit)

  private def handleBacklogList(
      payload: String,
      defaultReplyTopic: String,
      allowedReplyTopics: Set[String],
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String
  ): IO[Unit] =
    retryDatabase("backlog_list", payload, dlqTopic, producer) { store.listPendingBacklog(100).value
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
      publishReply(producer, resolveReplyTopic(payload, defaultReplyTopic, allowedReplyTopics), body)
    }

  private def handleBacklogSynced(
      payload: String,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String
  ): IO[Unit] =
    parseBacklogIdentity(payload) match
      case Left(error) => publishInvalidPayload(producer, dlqTopic, payload, "backlog_synced", error)
      case Right((_, dedupeKey, streamName)) =>
        retryDatabase("backlog_synced", payload, dlqTopic, producer) { store.markBacklogSynced(dedupeKey, streamName).value
        }(_ => IO.unit)

  private def handleBacklogPrune(
      payload: String,
      defaultReplyTopic: String,
      allowedReplyTopics: Set[String],
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String
  ): IO[Unit] =
    cats.effect.Clock[IO].realTimeInstant.flatMap { now =>
      retryDatabase("backlog_prune", payload, dlqTopic, producer) { store.pruneBacklog(now.minus(7L, ChronoUnit.DAYS)).value
      } { pruned =>
        val body = Json.obj("pruned" -> pruned.asJson, "retention_days" -> 7.asJson).noSpaces
        publishReply(producer, resolveReplyTopic(payload, defaultReplyTopic, allowedReplyTopics), body)
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
      case Left(error) =>
        publishDlq(producer, dlqTopic, operation, payload, error).handleErrorWith { publishError =>
          IO(log.error(operation, "status" -> "dlq_publish_failed",
            "topic" -> dlqTopic, "error" -> errorMessage(publishError)))
        }
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
    consumerGroup: String,
      consumersCount: Int,
      kafkaCfg: KafkaCfg,
    producer: KafkaProducer[IO, String, String]
  )(
      process: CommittableConsumerRecord[IO, String, String] => IO[CommittableOffset[IO]]
  ): Stream[IO, Unit] =
    Stream
      .eval(
        KafkaComponents.waitForTopic(kafkaCfg, topic) *>
          KafkaComponents.waitForTopic(kafkaCfg, topic + kafkaCfg.dlqSuffix)
      )
      .flatMap(_ =>
        Stream.resource(fs2.kafka.KafkaConsumer.resource(settings))
      )
      .flatMap { consumer =>
        Stream.eval(consumer.subscribeTo(topic)) >>
        consumer.partitionedStream
          .map { partitionStream =>
            partitionStream.evalMap { committable =>
                process(committable).handleErrorWith { error =>
                  LockedTopicConsumer
                    .parkNonRetriable(
                      producer,
                      topic + kafkaCfg.dlqSuffix,
                      consumerGroup,
                      committable.record,
                      error
                    )
                    .as(committable.offset)
                }
              }
          }
          .parJoin(consumersCount)
          .through(commitBatch)
      }

  private def handleMacLookup(
      payload: String,
      defaultReplyTopic: String,
      allowedReplyTopics: Set[String],
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): IO[Unit] =
    if payload == null || payload.isEmpty then IO.unit
    else
      extractField(payload, "mac").flatMap(normalizeMac) match
        case None =>
          IO(log.warn("mac_lookup", "status" -> "skip", "error" -> "missing or invalid mac field"))
        case Some(mac) =>
          val macHashFields = hashMac(mac).toList.map("mac_hash" -> _)
          IO(log.debug("mac_lookup", (("status" -> "processing") :: macHashFields)*)) *> store.lookupDeviceByMac(mac).value.attempt.flatMap {
              case Left(err) =>
                IO(log.warn("mac_lookup", "status" -> "skip",
                  "error" -> errorMessage(err)))
              case Right(Right(Some(reply))) =>
                val replyTopic = resolveReplyTopic(payload, defaultReplyTopic, allowedReplyTopics)
                IO(log.info("mac_lookup", (("status" -> "found") ::
                  ("reply_topic" -> replyTopic) :: macHashFields)*)) *>
                  publishReply(producer, replyTopic, reply).handleErrorWith { err =>
                    IO(log.error("mac_lookup", "status" -> "reply_publish_failed",
                      "reply_topic" -> replyTopic, "error" -> errorMessage(err)))
                  }
              case Right(Right(None)) =>
                IO(log.info("mac_lookup", (("status" -> "not_found") :: macHashFields)*))
              case Right(Left(err)) =>
                IO(log.error("mac_lookup", "status" -> "db_error", "error" -> ErrorSanitizer.sanitize( err.message)))
            }

  private def handleNetworksAuthorized(
      payload: String,
      defaultReplyTopic: String,
      allowedReplyTopics: Set[String],
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String]
  ): IO[Unit] =
    if payload == null || payload.isEmpty then IO.unit
    else store.listAuthorizedNetworks.value.flatMap {
        case Right(reply) =>
          val replyTopic = resolveReplyTopic(payload, defaultReplyTopic, allowedReplyTopics)
          IO(log.info("networks_authorized", "status" -> "ok", "reply_topic" -> replyTopic)) *>
            publishReply(producer, replyTopic, reply).handleErrorWith { err =>
              IO(log.error("networks_authorized", "status" -> "reply_publish_failed",
                "reply_topic" -> replyTopic, "error" -> errorMessage(err)))
            }
        case Left(err) =>
          IO(log.error("networks_authorized", "status" -> "db_error", "error" -> ErrorSanitizer.sanitize( err.message)))
      }

  private def handleProbeFlush(
      payload: String,
      store: WirelessStore[IO],
      producer: KafkaProducer[IO, String, String],
      dlqTopic: String
  ): IO[Unit] =
    if payload == null || payload.isEmpty then IO.unit
    else attemptWithRetry(payload, store, MaxRetries, dlqTopic, producer)

  private def attemptWithRetry(
      payload: String,
      store: WirelessStore[IO],
      remaining: Int,
      dlqTopic: String,
      producer: KafkaProducer[IO, String, String]
  ): IO[Unit] =
    IO(log.info("probe_flush", "status" -> "processing", "payload_bytes" -> payload.length.toString)) *> store.flushProbeBatch(payload).value.flatMap {
        case Right(count) =>
          IO(log.info("probe_flush", "status" -> "ok",
            "records_inserted" -> count.toString, "payload_bytes" -> payload.length.toString))
        case Left(err) if remaining > 1 =>
          IO(log.warn("probe_flush", "status" -> "retry",
            "attempts_remaining" -> (remaining - 1).toString, "error" -> ErrorSanitizer.sanitize( err.message))) *>
            IO.sleep(RetryDelay * (1L << (MaxRetries - remaining))) *>
            attemptWithRetry(payload, store, remaining - 1, dlqTopic, producer)
        case Left(err) =>
          IO(log.error("probe_flush", "status" -> "dlq",
            "topic" -> dlqTopic, "error" -> ErrorSanitizer.sanitize( err.message))) *>
            publishDlq(producer, dlqTopic, "probe_flush", payload, err).handleErrorWith { publishError =>
              IO(log.error("probe_flush", "status" -> "dlq_publish_failed",
                "topic" -> dlqTopic, "error" -> errorMessage(publishError)))
            }
      }

  private def errorMessage(error: Throwable): String =
    ErrorSanitizer.message(error)

  private[kafka] def resolveReplyTopic(
      payload: String,
      defaultTopic: String,
      configuredReplyTopics: Set[String]
  ): String =
    extractField(payload, "reply_topic") match
      case Some(t) if isValidKafkaTopic(t) && isAllowedReplyTopic(t, configuredReplyTopics) =>
        log.debug("resolve_reply_topic", "status" -> "valid", "topic" -> t)
        t
      case _ =>
        defaultTopic

  private[kafka] def isValidKafkaTopic(topic: String): Boolean =
    TopicPattern.matches(topic) && topic != "." && topic != ".."

  private[kafka] def isAllowedReplyTopic(topic: String, configuredReplyTopics: Set[String]): Boolean =
      topic == SensorInboxPrefix.dropRight(1) || topic.startsWith(SensorInboxPrefix) ||
      configuredReplyTopics.contains(topic)

  private def configuredReplyTopics(cfg: WirelessConfig): Set[String] =
    Set(
      cfg.backlogListReplyTopic,
      cfg.backlogPruneReplyTopic,
      cfg.macLookupReplyTopic,
      cfg.networksAuthorizedReplyTopic
    )

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
      "error" -> Json.fromString(ErrorSanitizer.sanitize(err.message)),
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
