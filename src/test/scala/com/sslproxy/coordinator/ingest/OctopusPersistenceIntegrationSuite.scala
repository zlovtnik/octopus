package com.sslproxy.coordinator.ingest

import cats.effect.IO
import cats.effect.implicits.*
import cats.syntax.all.*
import com.sslproxy.coordinator.config.PostgresConfig
import com.sslproxy.coordinator.domain.{
  BrokerRecordMetadata,
  DatabaseError,
  IngestionDecision,
  IngestionDisposition,
  ResolvedScanRequestRecord,
  ScanRequestRecord
}
import com.sslproxy.coordinator.postgres.{
  PostgresPayloadResolver,
  PostgresRepository,
  PostgresSchemaPreflight,
  PostgresTransactor
}
import com.sslproxy.coordinator.util.Sha256Utils
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.Transactor
import doobie.implicits.*
import fs2.kafka.ConsumerRecord
import io.circe.{Printer, parser as circeParser}
import munit.CatsEffectSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import org.yaml.snakeyaml.Yaml

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.sql.DriverManager
import java.util.Base64
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class OctopusPersistenceIntegrationSuite extends CatsEffectSuite:
  private val ArtifactSha256 = "a" * 64
  private val ProxyMaxAuditBodyBytes = 65_536
  private var containerStarted = false

  private final class TestPostgresContainer(image: DockerImageName)
      extends GenericContainer[TestPostgresContainer](image)

  private lazy val postgres =
    new TestPostgresContainer(DockerImageName.parse("pgvector/pgvector:pg16"))
      .withExposedPorts(5432)
      .withEnv("POSTGRES_USER", "postgres")
      .withEnv("POSTGRES_PASSWORD", "postgres")
      .withEnv("POSTGRES_DB", "sync")

  private def jdbcUrl: String =
    s"jdbc:postgresql://${postgres.getHost}:${postgres.getMappedPort(5432)}/sync?sslmode=disable"

  private lazy val dataSource =
    val config = new HikariConfig()
    config.setJdbcUrl(jdbcUrl)
    config.setUsername("postgres")
    config.setPassword("postgres")
    config.setDriverClassName("org.postgresql.Driver")
    config.setMaximumPoolSize(4)
    config.setConnectionInitSql("SET TIME ZONE 'UTC'; SET search_path TO octopus_core, atheros_search")
    config.addDataSourceProperty("stringtype", "unspecified")
    new HikariDataSource(config)

  private lazy val doobieExecutor = Executors.newFixedThreadPool(2)

  private lazy val xa: Transactor[IO] =
    Transactor.fromDataSource[IO](
      dataSource,
      ExecutionContext.fromExecutorService(doobieExecutor)
    )

  private lazy val repository = new PostgresRepository(xa)
  private lazy val schemaConfig = PostgresConfig(
    host = postgres.getHost,
    port = postgres.getMappedPort(5432).intValue(),
    database = "sync",
    user = "postgres",
    password = "postgres",
    poolSize = 4,
    healthcheckReserve = 1,
    connectionTimeoutMs = 5000L,
    statementTimeoutSecs = 30,
    enabled = true,
    warnOnly = false,
    manifestSha256 = manifestValue("manifest_sha256")
  )
  private lazy val schemaTransactor = PostgresTransactor.fromDataSource(dataSource, schemaConfig)
  private lazy val dockerAvailable = DockerClientFactory.instance().isDockerAvailable

  override def beforeAll(): Unit =
    super.beforeAll()
    if dockerAvailable then
      postgres.start()
      containerStarted = true
      applyCanonicalManifest()

  override def afterAll(): Unit =
    if containerStarted then
      dataSource.close()
      doobieExecutor.shutdown()
      postgres.stop()
    super.afterAll()

  test("wireless scan ingestion hydrates payload hashes and projected columns"):
    requireDocker()
    val payload =
      """{
        |"schema_version":2,
        |"event_type":"wifi_data_frame",
        |"observed_at":"2026-07-27T12:00:00Z",
        |"sensor_id":"sensor-projection",
        |"location_id":"lab",
        |"channel":36,
        |"frame_type":"data",
        |"frame_subtype":"qos_data",
        |"source_mac":"AA:BB:CC:DD:EE:01",
        |"destination_mac":"AA:BB:CC:DD:EE:02",
        |"bssid":"AA:BB:CC:DD:EE:03",
        |"ssid":"test-network",
        |"signal_dbm":-42,
        |"noise_dbm":null,
        |"frame_control_flags":0,
        |"more_data":false,
        |"retry":false,
        |"power_save":false,
        |"protected":true,
        |"security_flags":7,
        |"risk_score":0.6,
        |"tags":["wifi","network:ipv4"],
        |"wps_manufacturer":"Example Devices",
        |"device_fingerprint":"device-fingerprint",
        |"handshake_captured":false,
        |"username":"test-user",
        |"identity_source":"observed_identity",
        |"mac":{"fragment_number":3},
        |"rf":{"frequency_mhz":5180,"channel_flags":{"raw":256},"data_rate_kbps":54000,
        |      "antenna_id":1,"tsft":123456,"raw_len":512,"signal_status":"present"},
        |"qos":{"tid":5,"eosp":false,"ack_policy":1,"ack_policy_label":"no_ack","amsdu":true},
        |"llc_snap":{"oui":"000000","ethertype":2048,"ethertype_name":"ipv4"},
        |"network":{"src_ip":"192.0.2.10","dst_ip":"198.51.100.20","ttl":64,
        |           "protocol":17,"protocol_name":"udp"},
        |"transport":{"protocol":"udp","src_port":5353,"dst_port":1900,"length":128,"checksum":99},
        |"application":{"protocol":"ssdp","ssdp":{"message_type":"M-SEARCH","st":"ssdp:all","mx":"2","usn":"uuid:test"},
        |               "dns":{"query_names":["example.test"],"answer_names":[]}},
        |"correlation":{"session_key":"session-key","retransmit_key":"retransmit-key",
        |               "frame_fingerprint":"frame-fingerprint","payload_visibility":"plaintext",
        |               "tsft_delta_us":10,"wall_clock_delta_ms":2},
        |"anomalies":{"large_frame":false,"mixed_encryption":null,
        |             "dedupe_or_replay_suspect":false,"reasons":[]}
        |}""".stripMargin.replaceAll("\\s+", "")
    val resolved = resolvedWireless(payload)
    val metadata = BrokerRecordMetadata(
      topic = "sync.scan.request",
      partition = 0,
      offset = 41L,
      consumerGroup = "octopus-scan-postgres-v1",
      groupVersion = 1,
      artifactSha256 = ArtifactSha256,
      messageKey = None,
      payloadSha256 = resolved.sourceRecordSha256
    )

    for
      decision <- repository
        .recordScanRequestWithEvidence(resolved, metadata)
        .map(requireRight)
      _ = assertEquals(decision.disposition, IngestionDisposition.Processed)
      hashes <- sql"""SELECT payload_sha256, payload IS NOT NULL
                     FROM sync_events
                     WHERE dedupe_key = ${resolved.dedupeKey}
                       AND stream_name = 'wireless.audit'"""
        .query[(String, Boolean)]
        .unique
        .transact(xa)
      evidenceHash <- sql"""SELECT payload_sha256
                            FROM ingestion_evidence
                            WHERE topic = 'sync.scan.request'
                              AND partition_id = 0
                              AND record_offset = 41
                              AND group_id = 'octopus-scan-postgres-v1'"""
        .query[String]
        .unique
        .transact(xa)
      projectionJson <- sql"""SELECT jsonb_build_object(
                                'event_type', event_type,
                                'sensor_id', sensor_id,
                                'source_mac', source_mac,
                                'destination_bssid', destination_bssid,
                                'signal_dbm', signal_dbm,
                                'noise_dbm', noise_dbm,
                                'frequency_mhz', frequency_mhz,
                                'channel_flags', channel_flags,
                                'qos_tid', qos_tid,
                                'qos_eosp', qos_eosp,
                                'src_ip', src_ip,
                                'dst_port', dst_port,
                                'app_protocol', app_protocol,
                                'session_key', session_key,
                                'frame_fingerprint', frame_fingerprint,
                                'retry', retry,
                                'protected', protected,
                                'risk_score', risk_score,
                                'identity_source', identity_source,
                                'wireless_search_text', wireless_search_text
                              )
                              FROM sync_events
                              WHERE dedupe_key = ${resolved.dedupeKey}
                                AND stream_name = 'wireless.audit'"""
        .query[String]
        .unique
        .transact(xa)
      projection = circeParser.parse(projectionJson).fold(throw _, identity).hcursor
    yield
      assertEquals(hashes, (resolved.eventPayloadSha256, true))
      assertEquals(evidenceHash, resolved.sourceRecordSha256)
      assertEquals(projection.get[String]("event_type"), Right("wifi_data_frame"))
      assertEquals(projection.get[String]("sensor_id"), Right("sensor-projection"))
      assertEquals(projection.get[String]("source_mac"), Right("aa:bb:cc:dd:ee:01"))
      assertEquals(projection.get[String]("destination_bssid"), Right("aa:bb:cc:dd:ee:02"))
      assertEquals(projection.get[Int]("signal_dbm"), Right(-42))
      assertEquals(projection.get[Option[Int]]("noise_dbm"), Right(None))
      assertEquals(projection.get[Int]("frequency_mhz"), Right(5180))
      assertEquals(projection.get[Int]("channel_flags"), Right(256))
      assertEquals(projection.get[Int]("qos_tid"), Right(5))
      assertEquals(projection.get[Boolean]("qos_eosp"), Right(false))
      assertEquals(projection.get[String]("src_ip"), Right("192.0.2.10"))
      assertEquals(projection.get[Int]("dst_port"), Right(1900))
      assertEquals(projection.get[String]("app_protocol"), Right("ssdp"))
      assertEquals(projection.get[String]("session_key"), Right("session-key"))
      assertEquals(projection.get[String]("frame_fingerprint"), Right("frame-fingerprint"))
      assertEquals(projection.get[Boolean]("retry"), Right(false))
      assertEquals(projection.get[Boolean]("protected"), Right(true))
      assertEquals(projection.get[Double]("risk_score"), Right(0.6))
      assertEquals(projection.get[String]("identity_source"), Right("observed_identity"))
      assert(projection.get[String]("wireless_search_text").toOption.exists(_.contains("sensor-projection")))

  test("historical hydration normalizes null-like projections without changing durable payloads"):
    requireDocker()
    val payload =
      """{"event_type":"wifi_management_frame","observed_at":"2026-07-27T12:01:00Z","sensor_id":"backfill-sensor","location_id":"lab","frame_type":"management","frame_subtype":"beacon","source_mac":"AA:BB:CC:DD:EE:10","ssid":"null","signal_dbm":null,"noise_dbm":"null","frequency_mhz":"malformed","channel_flags":9223372036854775808,"raw_len":null,"frame_control_flags":"null","retry":null,"protected":"null","risk_score":"null","mixed_encryption":null,"tags":[],"rf":{"signal_dbm":"-42","noise_dbm":null,"frequency_mhz":"5180","channel_flags":{"raw":"256"},"raw_len":"null"},"mac":{"retry":"1"}}"""
    val payloadHash = Sha256Utils.sha256Hex(payload)
    val payloadRef = inlineRef(payload)
    val nullSchemaPayload =
      """{"schema_version":null,"event_type":"wifi_management_frame","observed_at":"2026-07-27T12:01:30Z","sensor_id":"null-schema-sensor","source_mac":"AA:BB:CC:DD:EE:11"}"""
    val nullSchemaHash = Sha256Utils.sha256Hex(nullSchemaPayload)
    val nullSchemaRef = inlineRef(nullSchemaPayload)
    val archivedHash = "f" * 64
    val tombstonedHash = "e" * 64

    for
      _ <- sql"""INSERT INTO sync_events (
                   dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                   payload, status, producer, payload_archived
                 ) VALUES (
                   $payloadHash, 'wireless.audit', TIMESTAMP '2026-07-27 12:01:00',
                   $payloadRef, $payloadHash, $payload, 'completed', 'ssl-proxy', false
                 )""".update.run.transact(xa)
      _ <- sql"""INSERT INTO sync_events (
                   dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                   payload, status, producer, payload_archived
                 ) VALUES (
                   $nullSchemaHash, 'wireless.audit', TIMESTAMP '2026-07-27 12:01:30',
                   $nullSchemaRef, $nullSchemaHash, $nullSchemaPayload,
                   'completed', 'ssl-proxy', false
                 )""".update.run.transact(xa)
      _ <- sql"""INSERT INTO sync_events (
                   dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                   status, producer, payload_archived
                 ) VALUES (
                   $archivedHash, 'proxy.events', TIMESTAMP '2026-07-27 12:02:00',
                   $payloadRef, ${"1" * 64}, 'completed', 'ssl-proxy', true
                 )""".update.run.transact(xa)
      _ <- sql"""INSERT INTO sync_events (
                   dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                   status, producer, payload_archived
                 ) VALUES (
                   $tombstonedHash, 'proxy.events', TIMESTAMP '2026-07-27 12:03:00',
                   $payloadRef, ${"2" * 64}, 'completed', 'ssl-proxy', false
                 )""".update.run.transact(xa)
      _ <- sql"""INSERT INTO sync_event_tombstones (
                   dedupe_key, stream_name, payload_sha256, observed_at, expires_at
                 ) VALUES (
                   $tombstonedHash, 'proxy.events', ${"2" * 64},
                   TIMESTAMP '2026-07-27 12:03:00',
                   CURRENT_TIMESTAMP + INTERVAL '1 day'
                 )""".update.run.transact(xa)
      rawBefore <- sql"""SELECT CAST(payload AS TEXT), payload_sha256, payload_ref
                          FROM sync_events
                          WHERE dedupe_key = $payloadHash
                            AND stream_name = 'wireless.audit'"""
        .query[(String, String, String)]
        .unique
        .transact(xa)
      candidates <- repository.findSyncEventsNeedingHydration(None, 100).map(requireRight)
      candidate = candidates
        .find(_.dedupeKey == payloadHash)
        .getOrElse(fail("expected sparse wireless row in hydration page"))
      nullSchemaCandidate = candidates
        .find(_.dedupeKey == nullSchemaHash)
        .getOrElse(fail("expected JSON-null schema version row in hydration page"))
      _ = assert(!candidates.exists(_.dedupeKey == archivedHash))
      _ = assert(!candidates.exists(_.dedupeKey == tombstonedHash))
      hydrated <- repository.hydrateExistingSyncEvent(candidate, payload).map(requireRight)
      nullSchemaHydrated <- repository
        .hydrateExistingSyncEvent(
          nullSchemaCandidate,
          nullSchemaPayload
        )
        .map(requireRight)
      rawAfter <- sql"""SELECT CAST(payload AS TEXT), payload_sha256, payload_ref
                         FROM sync_events
                         WHERE dedupe_key = $payloadHash
                           AND stream_name = 'wireless.audit'"""
        .query[(String, String, String)]
        .unique
        .transact(xa)
      state <- sql"""SELECT schema_version, ssid, signal_dbm, noise_dbm,
                            frequency_mhz, channel_flags, raw_len, frame_control_flags,
                            retry, risk_score, mixed_encryption, protected
                     FROM sync_events
                     WHERE dedupe_key = $payloadHash
                       AND stream_name = 'wireless.audit'"""
        .query[(Int, String, Int, Option[Int], Int, Int, Int, Int, Boolean, Option[Double], Option[Boolean], Boolean)]
        .unique
        .transact(xa)
      nullSchemaVersion <- sql"""SELECT schema_version
                                  FROM sync_events
                                  WHERE dedupe_key = $nullSchemaHash
                                    AND stream_name = 'wireless.audit'"""
        .query[Int]
        .unique
        .transact(xa)
      remaining <- repository.findSyncEventsNeedingHydration(None, 100).map(requireRight)
    yield
      assert(hydrated)
      assert(nullSchemaHydrated)
      assertEquals(rawAfter, rawBefore)
      assertEquals(rawAfter._2, payloadHash)
      assertEquals(rawAfter._3, payloadRef)
      assertEquals(
        state,
        (1, "null", -42, None, 5180, 256, 0, 0, true, None, None, false)
      )
      assertEquals(nullSchemaVersion, 1)
      assert(!remaining.exists(_.dedupeKey == payloadHash))
      assert(!remaining.exists(_.dedupeKey == nullSchemaHash))

  test("shadow alert generation skips null-like signals and uses maintained projections"):
    requireDocker()
    val nullSignalPayload = """{"signal_dbm":"null"}"""
    val validSignalPayload = """{"signal_dbm":null,"rf":{"signal_dbm":"-40"}}"""
    val nullSignalKey = Sha256Utils.sha256Hex(nullSignalPayload)
    val validSignalKey = Sha256Utils.sha256Hex(validSignalPayload)

    for
      _ <- sql"""INSERT INTO sync_events (
                   dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                   payload, status, producer, source_mac, signal_dbm
                 ) VALUES (
                   $nullSignalKey, 'wireless.audit', CURRENT_TIMESTAMP,
                   ${inlineRef(nullSignalPayload)}, $nullSignalKey, $nullSignalPayload,
                   'completed', 'ssl-proxy', 'aa:bb:cc:dd:ee:20', NULL
                 )""".update.run.transact(xa)
      _ <- sql"""INSERT INTO sync_events (
                   dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                   payload, status, producer, source_mac, signal_dbm
                 ) VALUES (
                   $validSignalKey, 'wireless.audit', CURRENT_TIMESTAMP,
                   ${inlineRef(validSignalPayload)}, $validSignalKey, $validSignalPayload,
                   'completed', 'ssl-proxy', 'aa:bb:cc:dd:ee:21', -40
                 )""".update.run.transact(xa)
      alerts <- repository.generateShadowAlerts(100).map(requireRight)
      stored <- sql"""SELECT source_mac, signal_dbm
                       FROM wireless_shadow_alerts
                       WHERE source_mac IN ('aa:bb:cc:dd:ee:20', 'aa:bb:cc:dd:ee:21')
                       ORDER BY source_mac"""
        .query[(String, Int)]
        .to[List]
        .transact(xa)
    yield
      assertEquals(stored, List(("aa:bb:cc:dd:ee:21", -40)))
      assert(alerts.exists(_.contains("aa:bb:cc:dd:ee:21")))
      assert(!alerts.exists(_.contains("aa:bb:cc:dd:ee:20")))

  test("load acknowledgement binds batch_id without a JSON collation comparison"):
    requireDocker()
    val rawJson =
      """{"observed_at":"2026-07-25T20:00:00Z","host":"ack.example","body":{"ok":true}}"""
    val record = translatedAudit(rawJson, offset = 1L)

    for
      _ <- parkPendingLoadOutboxes()
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

  test("invalid load batch_id is parked after publication without dispatching its batch"):
    requireDocker()
    val rawJson =
      """{"observed_at":"2026-07-25T20:01:00Z","host":"invalid-batch.example"}"""
    val record = translatedAudit(rawJson, offset = 2L)

    for
      _ <- parkPendingLoadOutboxes()
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
      _ = assertEquals(acknowledged, Right(true))
      state <- sql"""SELECT o.status, o.published_at IS NOT NULL, o.last_error,
                            (SELECT b.status FROM sync_batches b
                             WHERE b.batch_id = ${decision.batchId}),
                            (SELECT j.status
                             FROM sync_jobs j
                             JOIN sync_batches b ON b.job_id = j.job_id
                             WHERE b.batch_id = ${decision.batchId}),
                            (SELECT COUNT(*) FROM outbox_publish_attempts a
                             WHERE a.outbox_id = o.outbox_id
                               AND a.status = 'failed')
                     FROM outbox_events o
                     WHERE o.outbox_id = ${claimed.outboxId}
                     """
        .query[(String, Boolean, String, String, String, Long)]
        .unique
        .transact(xa)
    yield
      assertEquals(state._1, "failed")
      assertEquals(state._2, true)
      assert(state._3.contains("load outbox batch_id must be a UUID"))
      assertEquals(state._4, "pending")
      assertEquals(state._5, "pending")
      assertEquals(state._6, 1L)

  test("maximum accepted payload audit persists a compact payload_ref and stores full payload in payload column"):
    requireDocker()
    val prefix = """{"observed_at":"2026-07-25T20:02:00Z","body":""""
    val suffix = "\"}"
    val filler = "x" * (ProxyMaxAuditBodyBytes - prefix.length - suffix.length)
    val rawJson = prefix + filler + suffix
    val record = translatedAudit(rawJson, offset = 3L)
    val expectedPayload = circeParser
      .parse(rawJson)
      .fold(error => fail(s"expected valid payload audit JSON, found $error"), identity)
    val expectedPayloadRef = circeParser
      .parse(record.requestJson)
      .toOption
      .flatMap(_.hcursor.get[String]("payload_ref").toOption)
      .getOrElse(fail("translated payload audit must contain payload_ref"))

    assertEquals(rawJson.getBytes(StandardCharsets.UTF_8).length, ProxyMaxAuditBodyBytes)
    assert(expectedPayloadRef.startsWith("sha256://"))
    assert(expectedPayloadRef.getBytes(StandardCharsets.UTF_8).length < 128)

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
      storedPayload <- sql"""SELECT CAST(payload AS TEXT)
                             FROM sync_events
                             WHERE dedupe_key = ${decision.dedupeKey}
                               AND stream_name = 'proxy.payload_audit'"""
        .query[String]
        .unique
        .transact(xa)
      evidenceCount <- sql"""SELECT COUNT(*)
                             FROM ingestion_evidence
                             WHERE topic = 'proxy.payload_audit'
                               AND partition_id = 0
                               AND record_offset = 3
                               AND group_id = 'octopus-payload-audit-postgres-v1'"""
        .query[Long]
        .unique
        .transact(xa)
    yield
      assertEquals(eventRef, (expectedPayloadRef, expectedPayloadRef.length.toLong))
      assertEquals(batchRef, eventRef)
      val parsedStoredPayload = circeParser
        .parse(storedPayload)
        .fold(error => fail(s"expected stored payload JSON, found $error"), identity)
      assertEquals(
        Printer.noSpacesSortKeys.print(parsedStoredPayload),
        Printer.noSpacesSortKeys.print(expectedPayload)
      )
      assertEquals(evidenceCount, 1L)

  test("canonical manifest parsing is repeatable and retains TEXT payload capacity"):
    requireDocker()
    IO.blocking {
      val first = canonicalStatements().map { case (path, statements) =>
        path -> statements.size
      }
      val second = canonicalStatements().map { case (path, statements) =>
        path -> statements.size
      }
      assertEquals(second, first)
    } *>
      sql"""SELECT table_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'octopus_core'
              AND column_name = 'payload_ref'
              AND table_name IN ('sync_batches', 'sync_events')
            ORDER BY table_name"""
        .query[(String, String, Option[Long])]
        .to[List]
        .transact(xa)
        .map { columns =>
          assertEquals(
            columns,
            List(
              ("sync_batches", "text", None),
              ("sync_events", "text", None)
            )
          )
        }

  test("startup schema preflight accepts canonical payload_ref column types"):
    requireDocker()
    new PostgresSchemaPreflight(schemaTransactor, schemaConfig).validate()

  test("scan request evidence batch rolls back atomically when one record is invalid"):
    requireDocker()
    val group = "octopus-scan-batch-atomic-v1"
    val first = translatedAudit(
      """{"observed_at":"2026-07-31T20:10:00Z","host":"batch-first.example"}""",
      offset = 9101L
    )
    val second = translatedAudit(
      """{"observed_at":"2026-07-31T20:10:01Z","host":"batch-second.example"}""",
      offset = 9102L
    )
    val firstMetadata = BrokerRecordMetadata(
      topic = "sync.scan.request",
      partition = 7,
      offset = 9101L,
      consumerGroup = group,
      groupVersion = 1,
      artifactSha256 = ArtifactSha256,
      messageKey = None,
      payloadSha256 = first.sourceRecordSha256
    )
    val invalidSecondMetadata = firstMetadata.copy(
      offset = 9102L,
      payloadSha256 = "f" * 64
    )

    for
      result <- repository.recordScanRequestsWithEvidence(
        List(
          first -> firstMetadata,
          second -> invalidSecondMetadata
        )
      )
      evidenceCount <- sql"""SELECT COUNT(*) FROM ingestion_evidence
                              WHERE group_id = $group
                                AND topic = 'sync.scan.request'
                                AND partition_id = 7
                                AND record_offset IN (9101, 9102)"""
        .query[Long]
        .unique
        .transact(xa)
      eventCount <- (fr"""SELECT COUNT(*) FROM sync_events
                            WHERE stream_name = 'proxy.payload_audit'
                              AND dedupe_key IN (""" ++
        List(first.dedupeKey, second.dedupeKey).map(value => fr0"$value").intercalate(fr",") ++
        fr")").query[Long].unique.transact(xa)
    yield
      assert(result.isLeft)
      assertEquals(evidenceCount, 0L)
      assertEquals(eventCount, 0L)

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
              0, 5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
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
    schemaTransactor
      .preflightCheckColumnTypes(
        List(
          ("sync_events" -> "payload_ref") -> "longtext",
          ("sync_events" -> "missing_payload_ref") -> "text"
        )
      )
      .map { invalid =>
        assertEquals(
          invalid,
          List(
            "sync_events.payload_ref (expected longtext, found text)",
            "sync_events.missing_payload_ref (expected text, found missing)"
          )
        )
      }

  private def resolvedWireless(payload: String): ResolvedScanRequestRecord =
    val payloadSha256 = Sha256Utils.sha256Hex(payload)
    val requestJson = io.circe.Json
      .obj(
        "stream_name" -> io.circe.Json.fromString("wireless.audit"),
        "dedupe_key" -> io.circe.Json.fromString(payloadSha256),
        "payload_ref" -> io.circe.Json.fromString(inlineRef(payload)),
        "observed_at" -> io.circe.Json.fromString(
          circeParser
            .parse(payload)
            .toOption
            .flatMap(_.hcursor.get[String]("observed_at").toOption)
            .getOrElse(fail("wireless payload requires observed_at"))
        )
      )
      .noSpaces
    val source = ScanRequestRecord.decodeWire(requestJson).fold(throw _, identity)
    new PostgresPayloadResolver("/unused").resolve(source)

  private def inlineRef(payload: String): String =
    "inline://json/" + Base64.getUrlEncoder.withoutPadding
      .encodeToString(payload.getBytes(StandardCharsets.UTF_8))

  private def translatedAudit(rawJson: String, offset: Long): ResolvedScanRequestRecord =
    PayloadAuditConsumer
      .translateRecord(
        ConsumerRecord[String, String]("proxy.payload_audit", 0, offset, null, rawJson)
      )
      .fold(error => fail(s"expected valid payload audit, found $error"), identity)

  private def persist(record: ResolvedScanRequestRecord, offset: Long): IO[IngestionDecision] =
    val metadata = BrokerRecordMetadata(
      topic = "proxy.payload_audit",
      partition = 0,
      offset = offset,
      consumerGroup = "octopus-payload-audit-postgres-v1",
      groupVersion = 1,
      artifactSha256 = ArtifactSha256,
      messageKey = None,
      payloadSha256 = record.sourceRecordSha256
    )

    repository.recordScanRequestWithEvidence(record, metadata).flatMap { result =>
      val decision = requireRight(result)
      assertEquals(decision.disposition, IngestionDisposition.Processed)
      repository
        .prepareLoadDispatch(List(record.streamName), maxAttempts = 5, limit = 100)
        .map(dispatch =>
          requireRight(dispatch)
          decision
        )
    }

  private def parkPendingLoadOutboxes(): IO[Unit] =
    sql"""UPDATE outbox_events
           SET status = 'failed',
               last_error = 'superseded by integration test isolation',
               updated_at = CURRENT_TIMESTAMP
           WHERE destination_topic = 'sync.oracle.load'
             AND status = 'pending'""".update.run.void.transact(xa)

  private def requireRight[A](result: Either[DatabaseError, A]): A =
    result.fold(error => fail(s"${error.operation}: ${error.message}"), identity)

  private def requireDocker(): Unit =
    assume(dockerAvailable, "Docker is required for the PostgreSQL integration suite")

  private def applyCanonicalManifest(): Unit =
    val manifest = canonicalManifest()
    val parsedStatements = canonicalStatements(manifest)

    val connection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")
    try
      val statement = connection.createStatement()
      try
        parsedStatements.foreach { case (_, statements) =>
          statements.foreach { sqlStatement =>
            val _ = statement.execute(sqlStatement)
          }
        }
        val readiness = connection.prepareStatement(
          """UPDATE octopus_core.schema_readiness
            |SET required_version = ?, applied_version = ?,
            |    required_checksum = ?, applied_checksum = ?, ready = true,
            |    checked_at = CURRENT_TIMESTAMP
            |WHERE domain = 'octopus_core'""".stripMargin
        )
        try
          val version = manifest.schemaVersion
          val checksum = manifest.manifestSha256
          readiness.setString(1, version)
          readiness.setString(2, version)
          readiness.setString(3, checksum)
          readiness.setString(4, checksum)
          val _ = readiness.executeUpdate()
        finally readiness.close()
      finally statement.close()
    finally connection.close()

  private def manifestValue(key: String): String =
    val manifest = canonicalManifest()
    key match
      case "schema_version" => manifest.schemaVersion
      case "manifest_sha256" => manifest.manifestSha256
      case _ => throw IllegalStateException(s"unsupported canonical manifest key $key")

  private final case class CanonicalManifest(
    schemaVersion: String,
    manifestSha256: String,
    applyOrder: List[String]
  )

  private def canonicalManifest(): CanonicalManifest =
    val input = Files.newInputStream(schemaRoot.resolve("manifest.yaml"))
    val root =
      try Option(new Yaml().load[java.util.Map[String, Object]](input))
      finally input.close()
    val values = root.getOrElse(throw IllegalStateException("canonical manifest is empty"))

    def requiredScalar(key: String): String =
      Option(values.get(key))
        .map(_.toString.trim)
        .filter(_.nonEmpty)
        .getOrElse(throw IllegalStateException(s"missing or empty $key in canonical manifest"))

    val applyOrder = Option(values.get("apply_order"))
      .map(_.asInstanceOf[java.util.List[Object]].asScala.toList)
      .getOrElse(Nil)
      .map(_.toString.trim)
      .filter(_.nonEmpty)
    if applyOrder.isEmpty then
      throw IllegalStateException("canonical manifest apply_order must not be missing or empty")

    CanonicalManifest(
      requiredScalar("schema_version"),
      requiredScalar("manifest_sha256"),
      applyOrder
    )

  private def canonicalStatements(): List[(String, List[String])] =
    canonicalStatements(canonicalManifest())

  private def canonicalStatements(
    manifest: CanonicalManifest
  ): List[(String, List[String])] =
    manifest.applyOrder.map { relative =>
      val source = Files.readString(schemaRoot.resolve(relative))
      val statements = splitSqlStatements(source, relative)
      if statements.isEmpty then
        throw IllegalStateException(s"canonical schema file $relative contains no complete SQL statements")
      relative -> statements
    }

  private def splitSqlStatements(source: String, relative: String): List[String] =
    val statements = List.newBuilder[String]
    val current = new StringBuilder
    var index = 0
    var quote: Char = 0.toChar
    var lineComment = false
    var blockComment = false

    while index < source.length do
      val char = source.charAt(index)
      val next = if index + 1 < source.length then source.charAt(index + 1) else 0.toChar

      if lineComment then
        if char == '\n' || char == '\r' then
          lineComment = false
          current.append(' ')
      else if blockComment then
        if char == '*' && next == '/' then
          blockComment = false
          index += 1
      else if quote != 0.toChar then
        current.append(char)
        if char == '\\' && index + 1 < source.length then
          current.append(next)
          index += 1
        else if char == quote then
          if next == quote then
            current.append(next)
            index += 1
          else quote = 0.toChar
      else if char == '-' && next == '-' then
        lineComment = true
        index += 1
      else if char == '#' then lineComment = true
      else if char == '/' && next == '*' then
        blockComment = true
        index += 1
      else if char == '\'' || char == '"' || char == '`' then
        quote = char
        current.append(char)
      else if char == ';' then
        val statement = current.result().trim
        if statement.nonEmpty then statements += statement
        current.clear()
      else current.append(char)

      index += 1

    if quote != 0.toChar then
      throw IllegalStateException(s"unterminated quoted literal in canonical schema file $relative")
    if blockComment then throw IllegalStateException(s"unterminated block comment in canonical schema file $relative")
    if current.result().trim.nonEmpty then
      throw IllegalStateException(s"incomplete SQL statement without semicolon in canonical schema file $relative")

    statements.result()

  private def schemaRoot: Path =
    Iterator
      .iterate(Path.of("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .map(_.resolve("sql/postgres/octopus_core"))
      .find(path => Files.isDirectory(path))
      .getOrElse(throw IllegalStateException("cannot locate canonical octopus_core schema"))
