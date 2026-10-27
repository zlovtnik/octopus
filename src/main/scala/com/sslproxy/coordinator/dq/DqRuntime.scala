package com.sslproxy.coordinator.dq

import cats.Monad
import cats.effect.std.Semaphore
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.observability.{CoordinatorMetrics, CoordinatorTracing, StructuredLogger}
import com.sslproxy.coordinator.postgres.sql.DqProjectionSql
import com.sslproxy.coordinator.util.Sha256Utils
import doobie.implicits.*
import fs2.Stream
import fs2.kafka.*
import io.circe.Json
import io.circe.parser.parse
import io.opentelemetry.api.trace.SpanKind
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.config.ConfigResource

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.time.Duration as JavaDuration
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Claim steps are database-fenced across Octopus replicas. Storage and broker
  * operations happen outside DB permits and transactions; acknowledgements are
  * durable before offsets advance. Each partition retries its current record.
  */
final class DqRuntime(
  config: DqConfig,
  kafka: KafkaCfg,
  store: DqStore,
  artifacts: DqArtifacts,
  dbSemaphore: Semaphore[IO],
  producer: KafkaProducer[IO, String, String],
  minimumSimilarity: Double,
  readiness: Ref[IO, Boolean],
  metrics: CoordinatorMetrics
):
  import DqArtifacts.required
  private val log = StructuredLogger(getClass)
  private val owner = java.util.UUID.randomUUID().toString
  private val group = "octopus-dq-worker-reports-v1"

  private def database[A](effect: IO[A]): IO[A] = dbSemaphore.permit.use(_ => effect)

  def initialize: IO[Unit] = artifacts.initialize *> DqRuntime.brokerPreflight(kafka)

  private def prepare(run: Json): IO[Unit] =
    val runId = required(run, "run_id")
    val work = for
      snapshot <- database(IO.defer(DqSnapshot.prepare(run, minimumSimilarity).transact(store.xa)))
      reference <- artifacts.putSnapshot(run, snapshot)
      _ <- database(store.completePreparation(runId, owner, Json.arr(reference), DqSnapshot.revision(snapshot)))
    yield ()
    DqRuntime.traced("dq.prepare", run, SpanKind.INTERNAL)(work).handleErrorWith { error =>
      val (code, permanent) = error match
        case value: DqPermanentError => (value.code, true)
        case _: DqStaleSource => ("source_revision_changed", true)
        case _ => ("preparation_dependency_failure", false)
      database(store.failPreparation(runId, owner, code, permanent)) *>
        IO(log.warn("dq_preparation", "status" -> code)) *>
        (if permanent then IO.unit else IO.raiseError(error))
    }

  private def applyOutput(run: Json): IO[Unit] =
    val runId = required(run, "run_id")
    def single(name: String): Json = run.hcursor
      .get[Vector[Json]](name)
      .toOption
      .filter(_.size == 1)
      .flatMap(_.headOption)
      .getOrElse(throw DqPermanentError(s"invalid_$name"))
    val work = for
      snapshot <- IO.defer(artifacts.read(single("input_refs"), run, output = false))
      proposal <- IO.defer(artifacts.read(single("output_refs"), run, output = true))
      _ <- database(
        IO.defer(DqApply.transaction(store, owner, run, snapshot, proposal, minimumSimilarity).transact(store.xa))
      )
    yield ()
    DqRuntime.traced("dq.apply", run, SpanKind.INTERNAL)(work).handleErrorWith { error =>
      val (code, permanent, fresh) = error match
        case value: DqPermanentError => (value.code, true, false)
        case _: DqStaleSource => ("source_revision_changed", false, true)
        case _ => ("application_dependency_failure", false, false)
      database(store.failOutput(runId, owner, code, permanent, fresh)) *>
        IO(log.warn("dq_application", "status" -> code)) *>
        (if permanent || fresh then IO.unit else IO.raiseError(error))
    }

  private def publish(item: DqOutbox): IO[Unit] =
    val raw = DqJson.canonical(item.payload)
    (IO(DqJson.bounded(item.payload)) *>
      DqRuntime.traced("dq.publish", item.payload, SpanKind.PRODUCER) {
        IO {
          val headers = new org.apache.kafka.common.header.internals.RecordHeaders()
          CoordinatorTracing.injectCurrent(headers)
          val converted = fs2.kafka.Headers.fromIterable(
            headers.asScala.map(header => fs2.kafka.Header(header.key(), header.value())).toList
          )
          ProducerRecord(item.topic, item.key, raw).withHeaders(converted)
        }.flatMap(record => producer.produce(ProducerRecords.one(record)).flatten.void)
      }).attempt.flatMap {
      case Right(_) => database(store.acknowledgeOutbox(item)).void
      case Left(error) => database(store.failOutbox(item)) *> IO.raiseError(error)
    }

  private def admission(scope: String, operation: String, key: String, candidate: Option[String]): Json =
    val parameters = Json
      .obj(
        "authorized_scope" -> Json.fromString(scope),
        "policy_version" -> Json.fromString(config.policyVersion),
        "max_candidates" -> Json.fromInt(1000)
      )
      .deepMerge(candidate.fold(Json.obj())(id => Json.obj("candidate_id" -> Json.fromString(id))))
    Json.obj(
      "installation_id" -> Json.fromString(config.installationId),
      "namespace" -> Json.fromString(config.namespace),
      "processor_id" -> Json.fromString("atheros.dedup"),
      "processor_version" -> Json.fromString("0.1.0"),
      "operation" -> Json.fromString(operation),
      "parameters" -> parameters,
      "idempotency_key" -> Json.fromString(key)
    )

  private val automation = DqPrincipal("octopus-dq-discovery", Set("automation"), config.scopeModes.keySet)
  private def discoverAdmission(request: Json): IO[Unit] =
    database(store.admit(automation, request)).void.handleErrorWith {
      case DqError(429, "admission_capacity") => IO.unit
      case error => IO.raiseError(error)
    }

  private def discover: IO[Unit] =
    // Registration is dynamic. Readiness remains down until the approved pinned
    // processor is available, while the control API still accepts registration.
    database(store.manifest(DqPrincipal.internal, "atheros.dedup", "0.1.0")) *>
      config.scopeModes.toList.sortBy(_._1).traverse_ { case (scope, mode) =>
        val request = admission(scope, "prepare_candidates", "discovery", None)
          .deepMerge(Json.obj("policy_version" -> Json.fromString(config.policyVersion)))
        database(IO.defer(DqSnapshot.prepare(request, minimumSimilarity).transact(store.xa))).flatMap { snapshot =>
          val hasPairs = snapshot.hcursor.get[Vector[Json]]("pairs").exists(_.nonEmpty)
          val key = DqArtifacts.digest(
            Json.arr(
              Json.fromString("candidate-preparation"),
              Json.fromString(scope),
              Json.fromString(mode),
              Json.fromString(config.policyVersion),
              Json.fromString(DqSnapshot.revision(snapshot))
            )
          )
          IO.whenA(hasPairs)(discoverAdmission(admission(scope, "prepare_candidates", key, None)))
        }
      } *> database(
        DqProjectionSql.decisions(config.installationId, config.namespace, config.batchSize).to[List].transact(store.xa)
      )
        .flatMap(_.traverse_ { case (candidateId, decision, scope) =>
          val key = DqArtifacts.digest(
            Json.arr(
              Json.fromString("decision-reconciliation"),
              Json.fromString(candidateId),
              Json.fromString(decision),
              Json.fromString(scope),
              Json.fromString(config.policyVersion),
              Json.fromString(config.scopeModes.getOrElse(scope, "disabled"))
            )
          )
          discoverAdmission(admission(scope, "reconcile_decision", key, Some(candidateId)))
        })

  private def tick: IO[Unit] =
    for
      _ <- artifacts.initialize
      _ <- database(store.expireAttempts)
      // Start a lease immediately before doing its work. Leasing a whole batch
      // before serial processing spends the later items' lease while they wait.
      _ <- DqRuntime.attemptStages(
        List(
          DqRuntime.drain(
            config.batchSize,
            database(store.claimPreparation(owner, config.leaseSeconds, 1)).map(_.headOption)
          )(prepare),
          DqRuntime.drain(
            config.batchSize,
            database(store.claimOutput(owner, config.leaseSeconds, 1)).map(_.headOption)
          )(applyOutput),
          DqRuntime.drain(
            config.batchSize,
            database(store.claimOutbox(owner, config.leaseSeconds, 1)).map(_.headOption)
          )(publish),
          // Drain previously admitted work before discovery. A full admission queue
          // must never prevent preparation, application or outbox progress.
          discover
        )
      )
    yield ()

  private def guardedTick(health: DqRuntime.Readiness): IO[Unit] = tick.attempt.flatMap {
    case Right(_) => health.maintenance(true)
    case Left(error) =>
      health.maintenance(false) *> IO(
        log.warn(
          "dq_runtime",
          "status" -> (error match
            case value: DqError => value.code
            case value: DqPermanentError => value.code
            case _ => "dependency_unavailable"
          ),
          "error_class" -> error.getClass.getSimpleName,
          "sql_state" -> (error match
            case value: java.sql.SQLException => Option(value.getSQLState).getOrElse("unknown")
            case _ => "none"
          )
        )
      )
  }

  private def reports(health: DqRuntime.Readiness): Stream[IO, Unit] =
    val settings = ConsumerSettings[IO, String, Array[Byte]]
      .withBootstrapServers(kafka.bootstrapServers)
      .withGroupId(group)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)
      .withIsolationLevel(IsolationLevel.ReadCommitted)
      .withMaxPollRecords(config.batchSize)
      .withMaxPrefetchBatches(1)
      .withProperties("allow.auto.create.topics" -> "false", "max.partition.fetch.bytes" -> "131072")
    Stream.resource(KafkaConsumer.resource(settings)).flatMap { consumer =>
      val records = Stream.eval(consumer.subscribeTo("dq.worker-reports.v1")) >>
        consumer.partitionedStream.map { partition =>
          partition.evalMap { record =>
            val receipt = DqRuntime.receipt(
              config,
              group,
              record.record.topic,
              record.record.partition,
              record.record.offset,
              Option(record.record.key).getOrElse(""),
              Option(record.record.value).getOrElse(Array.emptyByteArray)
            )
            Monad[IO].tailRecM[Int, Unit](0) { retry =>
              DqRuntime
                .traced(
                  "dq.receive_report",
                  receipt.hcursor.downField("envelope").focus.getOrElse(Json.obj()),
                  SpanKind.CONSUMER
                ) {
                  database(store.receiveReceipt(receipt))
                }
                .attempt
                .flatMap {
                  case Right(_) =>
                    record.offset.commit *> health.reports(true) *> health.partition(record.record.partition, true)
                      .as(Right(()))
                  case Left(_) =>
                    health.partition(record.record.partition, false) *>
                      IO.sleep((1000L * (1L << retry.min(5))).millis).as(Left((retry + 1).min(5)))
                }
            }
          }
        }.parJoinUnbounded
      val lag = Stream
        .repeatEval(
          consumer.metrics
            .flatMap(values => IO(metrics.recordKafkaMetrics(group, values)))
            .timeout(5.seconds)
            .handleErrorWith(_ => IO(metrics.recordLagRefreshFailure(group)))
        )
        .metered(5.seconds)
        .onFinalize(IO(metrics.clearKafkaMetrics(group)))
      val assignments = consumer.assignmentStream.evalMap(partitions => health.assignments(partitions.map(_.partition)))
      records.concurrently(lag).concurrently(assignments)
    }

  private def reportsForever(health: DqRuntime.Readiness): IO[Unit] = Monad[IO].tailRecM[Int, Unit](0) { retry =>
    reports(health).compile.drain.guarantee(health.reports(false)).attempt.flatMap {
      case Right(_) => IO.pure(Right(()))
      case Left(_) =>
        IO
          .sleep((1000L * (1L << retry.min(5))).millis)
          .as(Left((retry + 1).min(5)))
    }
  }

  def stream: Stream[IO, Unit] =
    Stream.eval(DqRuntime.Readiness.create(readiness)).flatMap { health =>
      (Stream.eval(guardedTick(health)) ++
        Stream.awakeEvery[IO](config.pollIntervalMs.millis).evalMap(_ => guardedTick(health)))
        .merge(Stream.eval(reportsForever(health)))
        .concurrently(
          Stream
            .repeatEval(
              DqMetrics
                .sample(store, metrics, dbSemaphore)
                .handleErrorWith(_ => IO(log.warn("dq_metrics", "status" -> "sample_unavailable")))
            )
            .metered(5.seconds)
        )
        .onFinalize(readiness.set(false))
    }

object DqRuntime:
  private final case class Health(maintenance: Boolean, reports: Boolean, failedPartitions: Set[Int]):
    def ready: Boolean = maintenance && reports && failedPartitions.isEmpty

  private[dq] final class Readiness private (
    state: Ref[IO, Health],
    gate: Semaphore[IO],
    output: Ref[IO, Boolean]
  ):
    // Serialize both the component update and its published aggregate. A stale
    // healthy tick must never overwrite a newer report-consumer failure.
    private def update(f: Health => Health): IO[Unit] =
      gate.permit.use(_ => state.updateAndGet(f).flatMap(value => output.set(value.ready)))

    def maintenance(healthy: Boolean): IO[Unit] = update(_.copy(maintenance = healthy))
    def reports(healthy: Boolean): IO[Unit] =
      update(value => value.copy(reports = healthy, failedPartitions = if healthy then value.failedPartitions else Set.empty))
    def partition(id: Int, healthy: Boolean): IO[Unit] =
      update(value => value.copy(failedPartitions =
        if healthy then value.failedPartitions - id else value.failedPartitions + id
      ))
    def assignments(partitions: Set[Int]): IO[Unit] =
      update(value => value.copy(failedPartitions = value.failedPartitions.intersect(partitions)))

  private[dq] object Readiness:
    def create(output: Ref[IO, Boolean]): IO[Readiness] =
      for
        state <- Ref.of[IO, Health](Health(false, false, Set.empty))
        gate <- Semaphore[IO](1)
        _ <- output.set(false)
      yield new Readiness(state, gate, output)

  private[dq] def attemptStages(stages: List[IO[Unit]]): IO[Unit] =
    stages.traverse(_.attempt).flatMap(_.collectFirst { case Left(error) => error }.fold(IO.unit)(IO.raiseError))

  private[dq] def drain[A](limit: Int, claim: IO[Option[A]])(consume: A => IO[Unit]): IO[Unit] =
    Monad[IO].tailRecM[Int, Unit](0) { processed =>
      if processed >= limit then IO.pure(Right(()))
      else
        claim.flatMap {
          case None => IO.pure(Right(()))
          case Some(item) => consume(item).as(Left(processed + 1))
        }
    }

  private[dq] def traced[A](name: String, record: Json, kind: SpanKind)(operation: IO[A]): IO[A] =
    val headers = new org.apache.kafka.common.header.internals.RecordHeaders()
    List("traceparent", "tracestate").foreach { key =>
      record.hcursor.get[String](key).toOption.filter(_.nonEmpty).foreach { value =>
        val _ = headers.add(key, value.getBytes(StandardCharsets.UTF_8))
      }
    }
    CoordinatorTracing.spanWithParent(
      name,
      kind,
      Some(CoordinatorTracing.extractParent(headers)),
      "dq.operation" -> CoordinatorTracing.Attr.S(record.hcursor.get[String]("operation").getOrElse("control"))
    )(operation)

  private[dq] def receipt(
    config: DqConfig,
    group: String,
    topic: String,
    partition: Int,
    offset: Long,
    key: String,
    raw: Array[Byte]
  ): Json =
    val hash = Sha256Utils.sha256Hex(raw)
    val decoded = Either
      .catchNonFatal(
        StandardCharsets.UTF_8
          .newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(raw))
          .toString
      )
      .flatMap(value => parse(value).leftMap(identity[Throwable]))
    val permitted = decoded.toOption.filter { envelope =>
      raw.length <= 65536 && envelope.isObject &&
      envelope.hcursor.get[String]("installation_id").contains(config.installationId) &&
      envelope.hcursor.get[String]("namespace").contains(config.namespace) &&
      envelope.hcursor
        .get[String]("run_id")
        .toOption
        .exists(id => key == s"${config.installationId}/${config.namespace}/$id")
    }
    val eventId = permitted
      .flatMap(_.hcursor.get[String]("event_id").toOption)
      .filter(value => value.nonEmpty && value.length <= 256)
      .getOrElse(hash)
    Json
      .obj(
        "installation_id" -> Json.fromString(config.installationId),
        "namespace" -> Json.fromString(config.namespace),
        "consumer_group" -> Json.fromString(group),
        "topic" -> Json.fromString(topic),
        "partition" -> Json.fromInt(partition),
        "offset" -> Json.fromLong(offset),
        "event_id" -> Json.fromString(eventId),
        "payload_sha256" -> Json.fromString(hash)
      )
      .deepMerge(
        permitted.fold(Json.obj("quarantine_reason" -> Json.fromString("invalid_record")))(value =>
          Json.obj("envelope" -> value)
        )
      )

  def brokerPreflight(config: KafkaCfg): IO[Unit] = IO.blocking {
    val policies = Map(
      "dq.commands.v1" -> ("delete", 7),
      "dq.worker-reports.v1" -> ("delete", 7),
      "dq.events.v1" -> ("delete", 30),
      "dq.dlq.v1" -> ("delete", 30),
      "dq.registry.v1" -> ("compact", 0),
      "dq.audit.v1" -> ("delete", 180)
    )
    val properties = new Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    val admin = Admin.create(properties)
    try
      val topics = admin.describeTopics(policies.keys.toList.asJava).allTopicNames().get(15L, TimeUnit.SECONDS)
      if topics.asScala.values.exists(
          _.partitions().asScala.exists(_.replicas().size() < config.topicReplicationFactor)
        )
      then throw DqPermanentError("dq_topic_replication_inadequate")
      val resources = policies.keys.map(topic => new ConfigResource(ConfigResource.Type.TOPIC, topic)).toList
      val configs = admin.describeConfigs(resources.asJava).all().get(15L, TimeUnit.SECONDS)
      policies.foreachEntry { (topic, policy) =>
        val (cleanup, days) = policy
        val configured = configs.get(new ConfigResource(ConfigResource.Type.TOPIC, topic))
        if configured.get("cleanup.policy").value() != cleanup ||
          (days > 0 && configured.get("retention.ms").value().toLong != days.toLong * 86400000L)
        then throw DqPermanentError("dq_topic_retention_mismatch")
      }
    finally admin.close(JavaDuration.ofSeconds(5))
  }
