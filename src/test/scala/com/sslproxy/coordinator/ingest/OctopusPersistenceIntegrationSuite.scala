package com.sslproxy.coordinator.ingest

import cats.effect.IO
import cats.effect.implicits.*
import cats.syntax.all.*
import com.sslproxy.coordinator.config.TiDbConfig
import com.sslproxy.coordinator.domain.{
  BrokerRecordMetadata,
  DatabaseError,
  IngestionDecision,
  IngestionDisposition,
  ScanRequestRecord
}
import com.sslproxy.coordinator.tidb.{TidbRepository, TidbSchemaPreflight, TidbTransactor}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.Transactor
import doobie.implicits.*
import fs2.kafka.ConsumerRecord
import io.circe.parser as circeParser
import munit.CatsEffectSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.MySQLContainer
import org.testcontainers.utility.DockerImageName

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class OctopusPersistenceIntegrationSuite extends CatsEffectSuite:
  private val ArtifactSha256 = "a" * 64
  private val ProxyMaxAuditBodyBytes = 65_536
  private var containerStarted = false

  private final class TestMySqlContainer(image: DockerImageName)
      extends MySQLContainer[TestMySqlContainer](image)

  private lazy val mysql =
    new TestMySqlContainer(DockerImageName.parse("mysql:8.4"))
      .withDatabaseName("octopus_core")
      .withUsername("octopus_test")
      .withPassword("octopus_test")
      .withCommand(
        "--character-set-server=utf8mb4",
        "--collation-server=utf8mb4_0900_ai_ci"
      )

  private lazy val dataSource =
    val config = new HikariConfig()
    config.setJdbcUrl(mysql.getJdbcUrl)
    config.setUsername(mysql.getUsername)
    config.setPassword(mysql.getPassword)
    config.setDriverClassName(mysql.getDriverClassName)
    config.setMaximumPoolSize(4)
    config.setConnectionInitSql("SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    new HikariDataSource(config)

  private lazy val doobieExecutor = Executors.newFixedThreadPool(2)

  private lazy val xa: Transactor[IO] =
    Transactor.fromDataSource[IO](
      dataSource,
      ExecutionContext.fromExecutorService(doobieExecutor)
    )

  private lazy val repository = new TidbRepository(xa)
  private lazy val schemaConfig = TiDbConfig(
    host = mysql.getHost,
    port = mysql.getFirstMappedPort.intValue(),
    database = mysql.getDatabaseName,
    user = mysql.getUsername,
    password = mysql.getPassword,
    poolSize = 4,
    connectionTimeoutMs = 5000L,
    statementTimeoutSecs = 30,
    enabled = true,
    warnOnly = false
  )
  private lazy val schemaTransactor = TidbTransactor.fromDataSource(dataSource, schemaConfig)
  private lazy val dockerAvailable = DockerClientFactory.instance().isDockerAvailable

  override def beforeAll(): Unit =
    super.beforeAll()
    if dockerAvailable then
      mysql.start()
      containerStarted = true
      applyCanonicalManifest()

  override def afterAll(): Unit =
    if containerStarted then
      dataSource.close()
      doobieExecutor.shutdown()
      mysql.stop()
    super.afterAll()

  test("load acknowledgement binds batch_id without a JSON collation comparison"):
    requireDocker()
    val rawJson =
      """{"observed_at":"2026-07-25T20:00:00Z","host":"ack.example","body":{"ok":true}}"""
    val record = translatedAudit(rawJson, offset = 1L)

    for
      decision <- persist(record, offset = 1L)
      claimedResult <- repository.claimOutbox(
        ownerId = "ack-integration-test",
        destinationTopics = List("sync.oracle.load"),
        leaseSeconds = 60
      )
      claimed = requireRight(claimedResult).getOrElse(fail("expected a claimed load outbox record"))
      acknowledged <- repository.acknowledgeOutbox(claimed)
      _ = assertEquals(acknowledged, Right(true))
      state <- sql"""SELECT o.status, b.status, b.attempt_count, j.status,
                            COUNT(a.outbox_id)
                     FROM outbox_events o
                     JOIN sync_batches b ON b.outbox_id = o.outbox_id
                     JOIN sync_jobs j ON j.job_id = b.job_id
                     LEFT JOIN outbox_publish_attempts a ON a.outbox_id = o.outbox_id
                     WHERE b.batch_id = ${decision.batchId}
                     GROUP BY o.status, b.status, b.attempt_count, j.status"""
        .query[(String, String, Int, String, Long)]
        .unique
        .transact(xa)
    yield assertEquals(state, ("published", "dispatched", 1, "running", 1L))

  test("invalid load batch_id aborts acknowledgement before any durable mutation"):
    requireDocker()
    val rawJson =
      """{"observed_at":"2026-07-25T20:01:00Z","host":"invalid-batch.example"}"""
    val record = translatedAudit(rawJson, offset = 2L)

    for
      decision <- persist(record, offset = 2L)
      claimedResult <- repository.claimOutbox(
        ownerId = "invalid-batch-integration-test",
        destinationTopics = List("sync.oracle.load"),
        leaseSeconds = 60
      )
      claimed = requireRight(claimedResult).getOrElse(fail("expected a claimed load outbox record"))
      acknowledged <- repository.acknowledgeOutbox(
        claimed.copy(payload = """{"batch_id":"not-a-uuid"}""")
      )
      _ = acknowledged match
        case Left(_: DatabaseError.Permanent) => ()
        case other => fail(s"expected a permanent validation failure, found $other")
      state <- sql"""SELECT o.status, b.status, j.status,
                            (SELECT COUNT(*) FROM outbox_publish_attempts a
                             WHERE a.outbox_id = o.outbox_id)
                     FROM outbox_events o
                     JOIN sync_batches b ON b.batch_id = ${decision.batchId}
                     JOIN sync_jobs j ON j.job_id = b.job_id
                     WHERE o.outbox_id = ${claimed.outboxId}
                     LIMIT 1"""
        .query[(String, String, String, Long)]
        .unique
        .transact(xa)
    yield assertEquals(state, ("leased", "pending", "pending", 0L))

  test("maximum accepted payload audit persists a payload_ref beyond the TEXT limit"):
    requireDocker()
    val prefix = """{"observed_at":"2026-07-25T20:02:00Z","body":""""
    val suffix = "\"}"
    val filler = "x" * (ProxyMaxAuditBodyBytes - prefix.length - suffix.length)
    val rawJson = prefix + filler + suffix
    val record = translatedAudit(rawJson, offset = 3L)
    val expectedPayloadRef = circeParser.parse(record.requestJson).toOption
      .flatMap(_.hcursor.get[String]("payload_ref").toOption)
      .getOrElse(fail("translated payload audit must contain payload_ref"))

    assertEquals(rawJson.getBytes(StandardCharsets.UTF_8).length, ProxyMaxAuditBodyBytes)
    assert(expectedPayloadRef.getBytes(StandardCharsets.UTF_8).length > 65_535)

    for
      decision <- persist(record, offset = 3L)
      eventRef <- sql"""SELECT payload_ref, OCTET_LENGTH(payload_ref)
                        FROM sync_events
                        WHERE dedupe_key = ${decision.dedupeKey}
                          AND stream_name = 'proxy.payload_audit'"""
        .query[(String, Long)]
        .unique
        .transact(xa)
      batchRef <- sql"""SELECT payload_ref, OCTET_LENGTH(payload_ref)
                        FROM sync_batches
                        WHERE batch_id = ${decision.batchId}"""
        .query[(String, Long)]
        .unique
        .transact(xa)
      evidenceCount <- sql"""SELECT COUNT(*)
                             FROM ingestion_evidence
                             WHERE topic = 'proxy.payload_audit'
                               AND partition_id = 0
                               AND record_offset = 3
                               AND group_id = 'octopus-payload-audit-tidb-v1'"""
        .query[Long]
        .unique
        .transact(xa)
    yield
      assertEquals(eventRef, (expectedPayloadRef, expectedPayloadRef.length.toLong))
      assertEquals(batchRef, eventRef)
      assertEquals(evidenceCount, 1L)

  test("canonical manifest is repeatable and retains MEDIUMTEXT payload capacity"):
    requireDocker()
    IO.blocking(applyCanonicalManifest()) *>
      sql"""SELECT table_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'octopus_core'
              AND column_name = 'payload_ref'
              AND table_name IN ('sync_batches', 'sync_events')
            ORDER BY table_name"""
        .query[(String, String, Long)]
        .to[List]
        .transact(xa)
        .map { columns =>
          assertEquals(
            columns,
            List(
              ("sync_batches", "mediumtext", 16_777_215L),
              ("sync_events", "mediumtext", 16_777_215L)
            )
          )
        }

  test("startup schema preflight accepts canonical payload_ref column types"):
    requireDocker()
    new TidbSchemaPreflight(schemaTransactor, schemaConfig).validate()

  test("concurrent outbox claims lease each eligible row exactly once"):
    requireDocker()
    val topic = "test.concurrent.claim"
    val outboxIds = (1 to 12).toList.map { index =>
      java.util.UUID.nameUUIDFromBytes(s"$topic:$index".getBytes(StandardCharsets.UTF_8)).toString
    }

    val insertRows = outboxIds.zipWithIndex.traverse_ { case (outboxId, index) =>
      sql"""INSERT INTO outbox_events (
              outbox_id, source_type, source_id, event_type,
              destination_topic, message_key, payload, status,
              attempt_count, max_attempts, next_attempt_at, created_at, updated_at
            ) VALUES (
              $outboxId, 'test', $outboxId, 'test.concurrent.claim',
              $topic, ${s"message-$index"}, '{"test":true}', 'pending',
              0, 5, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
            )""".update.run.void
    }

    for
      _ <- insertRows.transact(xa)
      claims <- outboxIds.indices.toList.parTraverseN(4) { index =>
        repository.claimOutbox(s"concurrent-owner-$index", List(topic), leaseSeconds = 60)
      }
      records = claims.map(requireRight).flatten
    yield
      assertEquals(records.size, outboxIds.size)
      assertEquals(records.map(_.outboxId).toSet, outboxIds.toSet)

  test("column type preflight reports wrong and missing payload_ref columns"):
    requireDocker()
    schemaTransactor.preflightCheckColumnTypes(List(
      ("sync_events" -> "payload_ref") -> "longtext",
      ("sync_events" -> "missing_payload_ref") -> "mediumtext"
    )).map { invalid =>
      assertEquals(
        invalid,
        List(
          "sync_events.payload_ref (expected longtext, found mediumtext)",
          "sync_events.missing_payload_ref (expected mediumtext, found missing)"
        )
      )
    }

  private def translatedAudit(rawJson: String, offset: Long): ScanRequestRecord =
    PayloadAuditConsumer.translateRecord(
      ConsumerRecord[String, String]("proxy.payload_audit", 0, offset, null, rawJson)
    ).fold(error => fail(s"expected valid payload audit, found $error"), identity)

  private def persist(record: ScanRequestRecord, offset: Long): IO[IngestionDecision] =
    val metadata = BrokerRecordMetadata(
      topic = "proxy.payload_audit",
      partition = 0,
      offset = offset,
      consumerGroup = "octopus-payload-audit-tidb-v1",
      groupVersion = 1,
      artifactSha256 = ArtifactSha256,
      messageKey = None,
      payloadSha256 = record.payloadSha256
    )

    repository.recordScanRequestWithEvidence(record, metadata).map { result =>
      val decision = requireRight(result)
      assertEquals(decision.disposition, IngestionDisposition.Processed)
      decision
    }

  private def requireRight[A](result: Either[DatabaseError, A]): A =
    result.fold(error => fail(s"${error.operation}: ${error.message}"), identity)

  private def requireDocker(): Unit =
    assume(dockerAvailable, "Docker is required for the MySQL integration suite")

  private def applyCanonicalManifest(): Unit =
    val manifest = schemaRoot.resolve("manifest.yaml")
    val applyOrder = Files.readAllLines(manifest).asScala
      .dropWhile(_ != "apply_order:")
      .drop(1)
      .takeWhile(_.startsWith("  - "))
      .map(_.drop(4).trim)

    val connection = mysql.createConnection("")
    try
      val statement = connection.createStatement()
      try
        applyOrder.foreach { relative =>
          val executable = Files.readString(schemaRoot.resolve(relative)).linesIterator
            .filterNot(_.trim.startsWith("--"))
            .mkString("\n")
          executable.split(";").iterator.map(_.trim).filter(_.nonEmpty).foreach { sqlStatement =>
            val _ = statement.execute(sqlStatement)
          }
        }
      finally statement.close()
    finally connection.close()

  private def schemaRoot: Path =
    Iterator.iterate(Path.of("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .map(_.resolve("sql/tidb/octopus_core"))
      .find(path => Files.isDirectory(path))
      .getOrElse(throw IllegalStateException("cannot locate canonical octopus_core schema"))
