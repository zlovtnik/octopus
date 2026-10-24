package com.sslproxy.coordinator.ingest

import cats.effect.implicits.*
import cats.effect.{Deferred, IO}
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
import com.sslproxy.coordinator.postgres.sql.{
  DispatchAuthorizationSql,
  IdentityGraphSql,
  IngestionSql,
  MaintenanceSql,
  ProjectionSql,
  ResultSql
}
import com.sslproxy.coordinator.postgres.{
  BlockedEventInsert,
  PostgresClock,
  PostgresLoad,
  PostgresLoadHandler,
  PostgresPayloadResolver,
  PostgresRepository,
  PostgresResult,
  PostgresSchemaPreflight,
  PostgresTransactor,
  PostgresTransformService,
  ProxyEventInsert,
  WirelessBandwidthInsert,
  WirelessRogueApInsert,
  WirelessSignalAnomalyInsert
}
import com.sslproxy.coordinator.util.Sha256Utils
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.Transactor
import doobie.implicits.*
import fs2.kafka.ConsumerRecord
import io.circe.{Decoder, Printer, parser as circeParser}
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
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class OctopusPersistenceIntegrationSuite extends CatsEffectSuite:
  private val ArtifactSha256 = "a" * 64
  private val ProxyMaxAuditBodyBytes = 65_536
  private var containerStarted = false

  private final case class CompletionSample(
    streamName: String,
    eventStatus: String,
    jobStatus: String,
    batchStatus: String,
    attemptCount: Int,
    maxAttempts: Int,
    rowCount: Int
  )

  private given Decoder[CompletionSample] = Decoder.forProduct7(
    "stream_name",
    "event_status",
    "job_status",
    "batch_status",
    "attempt_count",
    "max_attempts",
    "row_count"
  )(CompletionSample.apply)

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
    config.setConnectionInitSql(
      "SET TIME ZONE 'UTC'; SET search_path TO octopus_core, atheros_search"
    )
    config.addDataSourceProperty("stringtype", "unspecified")
    new HikariDataSource(config)

  private lazy val doobieExecutor = Executors.newFixedThreadPool(2)

  private lazy val xa: Transactor[IO] =
    Transactor.fromDataSource[IO](
      dataSource,
      ExecutionContext.fromExecutorService(doobieExecutor)
    )

  private lazy val repository = new PostgresRepository(xa)

  test("stream wireless projection deduplicates replays without raw rows and expires only weekly evidence") {
    requireDocker()
    val now = java.time.Instant.now().minusSeconds(30)
    val calibration = com.sslproxy.coordinator.config.SensorCalibration(
      "stream-test",
      "test-lab",
      "reviewed-v1",
      -40,
      1,
      2,
      3,
      List(2412),
      List(1),
      3,
      4,
      now.minusSeconds(3600),
      now.plusSeconds(3600)
    )
    val json = s"""[{"sensorId":"stream-test","locationId":"test-lab","version":"reviewed-v1",
      "referenceRssi":-40,"referenceMeters":1,"pathLossExponent":2,"uncertaintyDb":3,"frequenciesMhz":[2412],
      "channels":[1],"minimumSamples":3,"maximumStddevDb":4,"validFrom":"${calibration.validFrom}",
      "validUntil":"${calibration.validUntil}","accessPoints":["02:00:00:00:fa:01"]}]"""
    val cfg = com.sslproxy.coordinator.config.WirelessProjectionConfig("projected", true, json)
    val repo = new PostgresRepository(xa, wirelessProjection = cfg)
    val event = com.sslproxy.coordinator.processor.WirelessObservation(
      "stream-test",
      now,
      Some("02:00:00:00:fa:02"),
      Some("02:00:00:00:fa:01"),
      Some("02:00:00:00:fa:02"),
      Some("test-ap"),
      Some(-60),
      Some(2412),
      Some(1),
      "data",
      "dns",
      false,
      true,
      false
    )
    val metadata =
      BrokerRecordMetadata("wireless.audit", 91, 100, "wireless-audit-projection-v1", 1, ArtifactSha256, None, "1" * 64)
    for
      rawBefore <- sql"SELECT COUNT(*) FROM sync_events".query[Long].unique.transact(xa)
      framesBefore <- sql"SELECT COUNT(*) FROM wireless_frames".query[Long].unique.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.wireless_projection_receipts
        (topic, partition_id, offset_id, payload_sha256, disposition, observed_at)
        VALUES (${metadata.topic}, ${metadata.partition}, ${metadata.offset}, ${"e" * 64}, 'projected', CURRENT_TIMESTAMP)""".update.run
        .transact(xa)
      _ <- (0 until 3).toList.traverse_(i =>
        repo
          .projectWirelessRecord(event, metadata.copy(offset = 100 + i, payloadSha256 = (i + 1).toString * 64))
          .flatMap(result => IO(assertEquals(result, Right(true))))
      )
      duplicate <- repo.projectWirelessRecord(event, metadata)
      replay <- repo.projectWirelessRecord(event, metadata.copy(offset = 200))
      conflict <- repo.projectWirelessRecord(event, metadata.copy(payloadSha256 = "f" * 64))
      summaries <-
        sql"""SELECT summary_id::text, frame_count, radio::text FROM atheros_search.wireless_observation_summaries
        WHERE sensor_id = 'stream-test'""".query[(String, Long, String)].to[List].transact(xa)
      newHistory = metadata.copy(consumerGroup = "wireless-audit-projection-v2", groupVersion = 2)
      repeatedPayload <- repo.projectWirelessRecord(event, newHistory)
      reusedCoordinate <- repo.projectWirelessRecord(event, newHistory.copy(offset = 101, payloadSha256 = "f" * 64))
      newHistoryConflict <- repo.projectWirelessRecord(event, newHistory.copy(offset = 101, payloadSha256 = "a" * 64))
      groupedHashes <- sql"""SELECT group_id, payload_sha256 FROM octopus_core.wireless_projection_group_receipts
        WHERE topic = ${metadata.topic} AND partition_id = ${metadata.partition} AND offset_id = 101
        ORDER BY group_id""".query[(String, String)].to[List].transact(xa)
      legacyHash <- sql"""SELECT payload_sha256 FROM octopus_core.wireless_projection_receipts
        WHERE topic = ${metadata.topic} AND partition_id = ${metadata.partition} AND offset_id = ${metadata.offset}"""
        .query[String]
        .unique
        .transact(xa)
      _ <- repo.buildSearchDocuments(100)
      _ <- repo.prepareEmbeddingJobs(100, "nomic-embed-text-v2-moe")
      jobsBefore <- sql"SELECT COUNT(*) FROM atheros_search.embedding_jobs".query[Long].unique.transact(xa)
      _ <- repo.buildSearchDocuments(100)
      _ <- repo.prepareEmbeddingJobs(100, "nomic-embed-text-v2-moe")
      jobsAfter <- sql"SELECT COUNT(*) FROM atheros_search.embedding_jobs".query[Long].unique.transact(xa)
      rangeCount <- sql"""SELECT COUNT(*) FROM atheros_search.wireless_topology_edges
        WHERE edge_kind = 'calibrated_range' AND target_node_id = 'device:02:00:00:00:fa:02'"""
        .query[Long]
        .unique
        .transact(xa)
      rawAfter <- sql"SELECT COUNT(*) FROM sync_events".query[Long].unique.transact(xa)
      framesAfter <- sql"SELECT COUNT(*) FROM wireless_frames".query[Long].unique.transact(xa)
      _ <-
        sql"""UPDATE atheros_search.wireless_observation_summaries SET window_start = window_start - INTERVAL '8 days',
        window_end = window_end - INTERVAL '8 days', expires_at = expires_at - INTERVAL '8 days'
        WHERE sensor_id = 'stream-test'""".update.run.transact(xa)
      expired <- repo.expireWirelessProjections(100)
      windowDocs <- sql"""SELECT COUNT(*) FROM atheros_search.search_documents
        WHERE source_kind = 'observation_window' AND sensor_id = 'stream-test'""".query[Long].unique.transact(xa)
      edgesAfter <- sql"""SELECT COUNT(*) FROM atheros_search.wireless_topology_edges
        WHERE target_node_id IN ('device:02:00:00:00:fa:02', 'ap:02:00:00:00:fa:01') AND expires_at IS NOT NULL"""
        .query[Long]
        .unique
        .transact(xa)
      entities <- sql"""SELECT COUNT(*) FROM atheros_search.wireless_topology_nodes
        WHERE node_id IN ('device:02:00:00:00:fa:02', 'ap:02:00:00:00:fa:01')""".query[Long].unique.transact(xa)
    yield
      assertEquals(duplicate, Right(false))
      assertEquals(replay, Right(false))
      assert(conflict.isLeft)
      assertEquals(repeatedPayload, Right(false))
      assertEquals(reusedCoordinate, Right(true))
      assert(newHistoryConflict.isLeft)
      assertEquals(
        groupedHashes,
        List("wireless-audit-projection-v1" -> ("2" * 64), "wireless-audit-projection-v2" -> ("f" * 64))
      )
      assertEquals(legacyHash, "e" * 64)
      assertEquals(summaries.map(_._2), List(3L))
      assert(summaries.forall(_._3.contains("mean_dbm")))
      assertEquals(jobsAfter, jobsBefore)
      assertEquals(rangeCount, 1L)
      assertEquals(rawAfter, rawBefore)
      assertEquals(framesAfter, framesBefore)
      assertEquals(expired, Right(1))
      assertEquals(windowDocs, 0L)
      assertEquals(edgesAfter, 0L)
      assertEquals(entities, 2L)
  }

  test("wireless projection expiry drains multi-batch receipt and hash backlogs") {
    requireDocker()
    for
      _ <- sql"""INSERT INTO octopus_core.wireless_projection_group_receipts
        (group_id, topic, partition_id, offset_id, payload_sha256, disposition, observed_at, expires_at)
        SELECT 'wireless-audit-projection-v2', 'wireless.audit', 1, i, lpad(to_hex(i), 64, '0'), 'received',
          CURRENT_TIMESTAMP - INTERVAL '10 days', CURRENT_TIMESTAMP - INTERVAL '1 day'
        FROM generate_series(1, 5001) i""".update.run.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.wireless_projection_receipts
        (topic, partition_id, offset_id, payload_sha256, disposition, observed_at, expires_at)
        SELECT 'wireless.audit', 1, i, lpad(to_hex(i), 64, '0'), 'received',
          CURRENT_TIMESTAMP - INTERVAL '10 days', CURRENT_TIMESTAMP - INTERVAL '1 day'
        FROM generate_series(1, 5001) i""".update.run.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.wireless_projection_hashes (payload_sha256, expires_at)
        SELECT lpad(to_hex(i), 64, '0'), CURRENT_TIMESTAMP - INTERVAL '1 day'
        FROM generate_series(1, 5001) i""".update.run.transact(xa)
      expired <- repository.expireWirelessProjections(100)
      receiptsLeft <- sql"""SELECT COUNT(*) FROM octopus_core.wireless_projection_receipts
        WHERE expires_at <= CURRENT_TIMESTAMP""".query[Long].unique.transact(xa)
      hashesLeft <- sql"""SELECT COUNT(*) FROM octopus_core.wireless_projection_hashes
        WHERE expires_at <= CURRENT_TIMESTAMP""".query[Long].unique.transact(xa)
      groupReceiptsLeft <- sql"""SELECT COUNT(*) FROM octopus_core.wireless_projection_group_receipts
        WHERE expires_at <= CURRENT_TIMESTAMP""".query[Long].unique.transact(xa)
    yield
      assertEquals(expired, Right(0))
      assertEquals(receiptsLeft, 0L)
      assertEquals(hashesLeft, 0L)
      assertEquals(groupReceiptsLeft, 0L)
  }

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
  private lazy val schemaTransactor =
    PostgresTransactor.fromDataSource(dataSource, schemaConfig)
  private lazy val dockerAvailable =
    DockerClientFactory.instance().isDockerAvailable
  private val dockerRequired =
    sys.env.get("OCTOPUS_REQUIRE_DOCKER").contains("true")

  override def beforeAll(): Unit =
    super.beforeAll()
    if dockerRequired && !dockerAvailable then
      throw IllegalStateException(
        "OCTOPUS_REQUIRE_DOCKER=true but Docker is unavailable"
      )
    else if dockerAvailable then
      postgres.start()
      containerStarted = true
      applyCanonicalManifest()

  override def afterAll(): Unit =
    if containerStarted then
      dataSource.close()
      doobieExecutor.shutdown()
      postgres.stop()
    super.afterAll()

  test(
    "legacy batch sinks satisfy canonical proxy rollup and wireless alert constraints"
  ):
    requireDocker()
    val observedAt = java.time.OffsetDateTime.parse("2026-09-28T20:55:55Z")
    val host = "postgres-load-regression.invalid"
    val proxyEvent = ProxyEventInsert(
      eventId = "0d996392-24a6-4f21-a2d2-e3b711ea05c1",
      eventTime = observedAt,
      eventType = "request",
      host = host,
      peerIp = None,
      wgPubkey = None,
      deviceId = None,
      identitySource = "unknown",
      peerHostname = None,
      clientUa = None,
      bytesUp = 10L,
      bytesDown = 20L,
      statusCode = Some(403L),
      blocked = true,
      classification = "ads_tracker",
      obfuscationProfile = None,
      correlationId = None,
      parentEventId = None,
      eventSequence = None,
      durationMs = None,
      reason = Some("policy"),
      rawJson = """{"type":"request","blocked":true}"""
    )
    val blocked = BlockedEventInsert(
      rowSequence = 0L,
      host = host,
      blockedBytes = 30L,
      frequencyHz = Some(1.0d),
      riskScore = Some(0.5d),
      category = Some("ads_tracker"),
      verdict = "BLOCKED",
      tarpitHeldMs = 0L,
      iatMs = None,
      consecutiveBlocks = Some(1L),
      lastVerdict = Some("BLOCKED"),
      tlsVer = None,
      alpn = None,
      ja3Lite = None,
      resolvedIp = None,
      asnOrg = None
    )
    val batchId = "postgres-load-alert-regression"
    val bandwidth = WirelessBandwidthInsert(
      rowSequence = 1L,
      schemaVersion = 1L,
      windowStart = observedAt,
      windowEnd = observedAt.plusMinutes(1L),
      sensorId = "sensor-regression",
      locationId = "lab",
      iface = "wlan0",
      channel = 36L,
      sourceMac = "02:00:00:00:00:02",
      destinationBssid = "02:00:00:00:00:01",
      ssid = Some("test-network"),
      bytes = 128L,
      frameCount = 1L,
      retryCount = 0L,
      moreDataCount = 0L,
      powerSaveCount = 0L,
      strongestSignalDbm = Some(-40L),
      histUnder100 = 0L,
      hist100500 = 1L,
      hist5001000 = 0L,
      hist10001500 = 0L,
      interArrivalP50Ms = None,
      externalBssid = true,
      thresholdExceeded = true,
      wallClockDeltaMs = None,
      windowIsPartial = false,
      publishedAt = Some(observedAt.plusMinutes(1L))
    )
    val rogue = WirelessRogueApInsert(
      0L,
      observedAt,
      "sensor-regression",
      "lab",
      "wlan0",
      36L,
      "02:00:00:00:00:01",
      Some("test-network"),
      Some(-40L),
      1L,
      Some("""{"event_type":"wireless_rogue_ap"}""")
    )
    val signal = WirelessSignalAnomalyInsert(
      0L,
      observedAt,
      "sensor-regression",
      "lab",
      "02:00:00:00:00:02",
      Some("02:00:00:00:00:01"),
      Some("test-network"),
      36L,
      -60L,
      -40L,
      20L,
      15L
    )

    for
      _ <- schemaTransactor.insertProxyEvents(
        "proxy-regression-1",
        List(proxyEvent),
        List(blocked),
        0L
      )
      _ <- schemaTransactor.insertProxyEvents(
        "proxy-regression-1",
        List(proxyEvent),
        List(blocked),
        0L
      )
      _ <- schemaTransactor.insertProxyEvents(
        "proxy-regression-2",
        List(proxyEvent.copy(eventId = "91f92a7e-7670-4fb1-8fe4-9cd5bc2ca497")),
        List(blocked),
        0L
      )
      _ <- schemaTransactor.insertWirelessRogueAp(batchId, List(rogue))
      _ <- schemaTransactor.insertWirelessSignalAnomaly(batchId, List(signal))
      _ <- schemaTransactor.insertWirelessBandwidth(batchId, List(bandwidth))
      _ <- schemaTransactor.insertWirelessRogueAp(batchId, List(rogue))
      _ <- schemaTransactor.insertWirelessSignalAnomaly(batchId, List(signal))
      _ <- schemaTransactor.insertWirelessBandwidth(batchId, List(bandwidth))
      rollup <- sql"""SELECT blocked_attempts, blocked_bytes
                       FROM octopus_core.proxy_blocked_host_rollups
                       WHERE host = $host"""
        .query[(Long, Long)]
        .unique
        .transact(xa)
      alertCount <- sql"""SELECT COUNT(*)
                           FROM octopus_core.wireless_alerts
                           WHERE batch_id = $batchId
                             AND alert_id IS NOT NULL
                             AND subject_kind IS NOT NULL
                             AND subject_id IS NOT NULL
                             AND severity IS NOT NULL
                             AND evidence IS NOT NULL
                             AND source_event_id IS NOT NULL"""
        .query[Long]
        .unique
        .transact(xa)
      bandwidthBytes <- sql"""SELECT bytes
                               FROM octopus_core.wireless_alerts
                               WHERE batch_id = $batchId
                                 AND alert_type = 'bandwidth_threshold'"""
        .query[Long]
        .unique
        .transact(xa)
    yield
      assertEquals(rollup, 2L -> 60L)
      assertEquals(alertCount, 3L)
      assertEquals(bandwidthBytes, 128L)

  test("streaming inspection preserves scan metadata, replay evidence and reference backfill state"):
    requireDocker()
    val payloads = List(
      """{"event_type":"old","event_type":"primary","type":"fallback"}""",
      """{"event_type":"old","event_type":false,"type":"  fallback  "}""",
      """{"event_type":"","type":"escaped\nkind","nested":{"event_type":"ignored"}}""",
      """[{"event_type":"nested"},null,1]""", "true", "42", "\"scalar\""
    )
    payloads.zipWithIndex.traverse_ { (payload, index) =>
      val key = s"streaming-inspection-$index"
      val sourceBody = s"streaming-source-$index"
      val source = ScanRequestRecord(sourceBody, Sha256Utils.sha256Hex(sourceBody), "proxy.events", key,
        "2026-07-27T12:00:00Z", inlineRef(payload))
      val record = ResolvedScanRequestRecord.from(source, payload).fold(throw _, identity)
      val metadata = BrokerRecordMetadata("sync.scan.request", 0, 11000L + index, "streaming-inspection", 1,
        ArtifactSha256, None, source.sourceRecordSha256)
      val json = circeParser.parse(payload).fold(throw _, identity)
      val expectedKind = json.hcursor.get[String]("event_type").toOption.filter(_.nonEmpty)
        .orElse(json.hcursor.get[String]("type").toOption.filter(_.nonEmpty))
      def state = sql"""SELECT e.payload::text, e.payload_sha256, e.event_kind,
                               e.status, j.status, b.status, b.row_count
                        FROM sync_events e JOIN sync_jobs j USING (stream_name, dedupe_key)
                        JOIN sync_batches b USING (stream_name, dedupe_key, job_id)
                        WHERE e.stream_name = 'proxy.events' AND e.dedupe_key = $key"""
        .query[(String, String, Option[String], String, String, String, Long)].unique.transact(xa)
      for
        first <- repository.recordScanRequestWithEvidence(record, metadata).map(requireRight)
        before <- state
        replay <- repository.recordScanRequestWithEvidence(record, metadata).map(requireRight)
        next <- repository.recordScanRequestsWithEvidence(List(record -> metadata.copy(offset = 12000L + index)))
          .map(requireRight)
        duplicateCount <- repository.recordScanRequests(List(record)).map(requireRight)
        afterReplay <- state
        _ <- sql"""UPDATE sync_events SET payload = NULL, payload_sha256 = NULL, event_kind = NULL
                    WHERE stream_name = 'proxy.events' AND dedupe_key = $key""".update.run.transact(xa)
        candidates <- repository.findSyncEventsNeedingHydration(None, 1000).map(requireRight)
        candidate = candidates.find(c => c.dedupeKey == key && c.streamName == "proxy.events")
          .getOrElse(fail("expected reference hydration candidate"))
        resolved = new PostgresPayloadResolver("/unused").resolvePayload(candidate.payloadRef)
        hydrated <- repository.hydrateExistingSyncEvent(candidate, resolved).map(requireRight)
        afterBackfill <- state
        evidence <- sql"""SELECT payload_sha256, disposition FROM ingestion_evidence
                           WHERE group_id = 'streaming-inspection' AND dedupe_key = $key
                           ORDER BY record_offset""".query[(String, String)].to[List].transact(xa)
        counts <- sql"""SELECT (SELECT COUNT(*) FROM sync_jobs WHERE dedupe_key = $key),
                                (SELECT COUNT(*) FROM sync_batches WHERE dedupe_key = $key)"""
          .query[(Long, Long)].unique.transact(xa)
      yield
        assertEquals(first.disposition, IngestionDisposition.Processed)
        assertEquals(replay, first.copy(disposition = IngestionDisposition.Deduplicated))
        assertEquals(next, List(replay))
        assertEquals(duplicateCount, 0)
        assertEquals(before._3, expectedKind)
        assertEquals(before._2, Sha256Utils.sha256Hex(payload))
        assertEquals(circeParser.parse(before._1), Right(json))
        assertEquals((before._4, before._5, before._6, before._7), ("batched", "pending", "pending", 1L))
        assertEquals(afterReplay, before)
        assert(hydrated)
        assertEquals(afterBackfill, before)
        assertEquals(evidence, List(source.sourceRecordSha256 -> "processed", source.sourceRecordSha256 -> "duplicate"))
        assertEquals(counts, 1L -> 1L)
    }

  test("repository entry points reject directly constructed and copied invalid payloads before replay"):
    requireDocker()
    val source = ScanRequestRecord("inspection-invalid", Sha256Utils.sha256Hex("inspection-invalid"),
      "proxy.events", "streaming-invalid", "2026-07-27T12:00:00Z", inlineRef("{}"))
    val valid = ResolvedScanRequestRecord.from(source, "{}").fold(throw _, identity)
    val metadata = BrokerRecordMetadata("sync.scan.request", 0, 13000L, "streaming-invalid", 1,
      ArtifactSha256, None, source.sourceRecordSha256)
    for
      _ <- repository.recordScanRequestWithEvidence(valid, metadata).map(requireRight)
      _ <- List("", "null", "{} {}", "[1,broken]").traverse_ { payload =>
        val direct = ResolvedScanRequestRecord(source, payload, valid.eventPayloadSha256)
        val copied = valid.copy(payloadJson = payload)
        for
          single <- repository.recordScanRequestWithEvidence(direct, metadata)
          batch <- repository.recordScanRequestsWithEvidence(List(copied -> metadata))
          deduplicated <- repository.recordScanRequests(List(valid, copied))
        yield
          assert(single.left.toOption.exists(_.isInstanceOf[DatabaseError.Permanent]))
          assert(batch.left.toOption.exists(_.isInstanceOf[DatabaseError.Permanent]))
          assert(deduplicated.left.toOption.exists(_.isInstanceOf[DatabaseError.Permanent]))
      }
      evidenceCount <- sql"""SELECT COUNT(*) FROM ingestion_evidence WHERE group_id = 'streaming-invalid'"""
        .query[Long].unique.transact(xa)
    yield assertEquals(evidenceCount, 1L)

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
      projection = circeParser
        .parse(projectionJson)
        .fold(throw _, identity)
        .hcursor
    yield
      assertEquals(hashes, (resolved.eventPayloadSha256, true))
      assertEquals(evidenceHash, resolved.sourceRecordSha256)
      assertEquals(
        projection.get[String]("event_type"),
        Right("wifi_data_frame")
      )
      assertEquals(
        projection.get[String]("sensor_id"),
        Right("sensor-projection")
      )
      assertEquals(
        projection.get[String]("source_mac"),
        Right("aa:bb:cc:dd:ee:01")
      )
      assertEquals(
        projection.get[String]("destination_bssid"),
        Right("aa:bb:cc:dd:ee:02")
      )
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
      assertEquals(
        projection.get[String]("frame_fingerprint"),
        Right("frame-fingerprint")
      )
      assertEquals(projection.get[Boolean]("retry"), Right(false))
      assertEquals(projection.get[Boolean]("protected"), Right(true))
      assertEquals(projection.get[Double]("risk_score"), Right(0.6))
      assertEquals(
        projection.get[String]("identity_source"),
        Right("observed_identity")
      )
      assert(
        projection
          .get[String]("wireless_search_text")
          .toOption
          .exists(_.contains("sensor-projection"))
      )

  test(
    "automatic identity confirmation requires the hard guard and yields to human decisions"
  ):
    requireDocker()
    val trustedIdentity = "11111111-1111-1111-1111-111111111111"
    val macs = (1 to 10).toList.map(index => f"02:00:00:00:00:$index%02x")
    val candidates = List(
      ("identity-auto", macs(0), macs(1), 0.98d),
      ("identity-low", macs(2), macs(3), 0.979d),
      ("identity-not-match", macs(4), macs(5), 1.0d),
      ("identity-more-data", macs(6), macs(7), 1.0d),
      ("identity-conflict", macs(8), macs(9), 1.0d)
    )

    for
      _ <- macs.traverse_ { mac =>
        sql"""INSERT INTO atheros_search.devices (
                 mac, registered_device_id, first_seen, last_seen, registered
               ) VALUES (
                 $mac, CAST($trustedIdentity AS uuid), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE
               )""".update.run.void.transact(xa)
      }
      _ <- macs.traverse_ { mac =>
        sql"""INSERT INTO octopus_core.devices (mac_id, mac_hint)
               VALUES ($mac, $mac)""".update.run.void.transact(xa)
      }
      _ <- candidates.traverse_ { case (candidateId, left, right, confidence) =>
        sql"""INSERT INTO atheros_search.merge_candidates (
                 candidate_id, mac_a, mac_b, confidence, projection_run_id
               ) VALUES ($candidateId, $left, $right, $confidence, $candidateId)""".update.run.void
          .transact(xa)
      }
      _ <- List(
        "identity-not-match" -> "not_match",
        "identity-more-data" -> "needs_more_data",
        "identity-conflict" -> "conflict"
      ).traverse_ { case (candidateId, decision) =>
        sql"""INSERT INTO atheros_search.merge_decisions (candidate_id, decision, decided_by)
               VALUES ($candidateId, $decision, 'identity-test')""".update.run.void
          .transact(xa)
      }
      projected <- repository.projectApprovedIdentities(0)
      _ = requireRight(projected)
      graphed <- repository.projectInfrastructureGraph(100)
      _ = requireRight(graphed)
      approved <- IdentityGraphSql.approvedIdentityEdges.to[List].transact(xa)
      states <- sql"""SELECT candidate_id, status, confirmation_source
                       FROM atheros_search.merge_candidates
                       WHERE candidate_id LIKE 'identity-%'"""
        .query[(String, String, Option[String])]
        .to[List]
        .transact(xa)
      confirmedProjection <- sql"""SELECT
          (SELECT COUNT(*) FROM atheros_search.identity_cluster_members
           WHERE mac IN (${macs(0)}, ${macs(1)})),
          (SELECT COUNT(*) FROM atheros_search.graph_edges
           WHERE edge_id = 'same-device:identity-auto')"""
        .query[(Long, Long)]
        .unique
        .transact(xa)
      stateByCandidate = states.map { case (candidateId, status, source) =>
        candidateId -> (status -> source)
      }.toMap
      _ = assertEquals(approved, List((macs(0), macs(1), 0.98d)))
      _ = assertEquals(confirmedProjection, (2L, 1L))
      _ = assertEquals(
        stateByCandidate,
        Map(
          "identity-auto" -> ("confirmed" -> Some("automatic")),
          "identity-low" -> ("pending" -> None),
          "identity-not-match" -> ("not_match" -> None),
          "identity-more-data" -> ("needs_more_data" -> None),
          "identity-conflict" -> ("conflict" -> None)
        )
      )
      skipped <- repository.projectApprovedIdentities(0)
      _ = assertEquals(skipped, Right(0))
      skippedProjection <- sql"""SELECT COUNT(*), MIN(confidence)
                                 FROM atheros_search.identity_cluster_members
                                 WHERE mac IN (${macs(0)}, ${macs(1)})"""
        .query[(Long, Option[Double])]
        .unique
        .transact(xa)
      _ = assertEquals(skippedProjection, (2L, Some(0.98d)))
      _ <- sql"""UPDATE atheros_search.merge_candidates
                  SET confidence = 0.99
                  WHERE candidate_id = 'identity-auto'""".update.run.void
        .transact(xa)
      recomputed <- repository.projectApprovedIdentities(1)
      _ = requireRight(recomputed)
      refreshedProjection <- sql"""SELECT COUNT(*), MIN(confidence)
                                   FROM atheros_search.identity_cluster_members
                                   WHERE mac IN (${macs(0)}, ${macs(1)})"""
        .query[(Long, Option[Double])]
        .unique
        .transact(xa)
      _ = assertEquals(refreshedProjection, (2L, Some(0.99d)))
      _ <-
        sql"""INSERT INTO atheros_search.merge_decisions (candidate_id, decision, decided_by)
                  VALUES ('identity-auto', 'conflict', 'identity-test')""".update.run.void
          .transact(xa)
      reprojected <- repository.projectApprovedIdentities(0)
      _ = requireRight(reprojected)
      regraphed <- repository.projectInfrastructureGraph(100)
      _ = requireRight(regraphed)
      blocked <- IdentityGraphSql.approvedIdentityEdges.to[List].transact(xa)
      blockedProjection <- sql"""SELECT
          (SELECT COUNT(*) FROM atheros_search.identity_cluster_members
           WHERE mac IN (${macs(0)}, ${macs(1)})),
          (SELECT COUNT(*) FROM atheros_search.graph_edges
           WHERE edge_id = 'same-device:identity-auto')"""
        .query[(Long, Long)]
        .unique
        .transact(xa)
      _ = assertEquals(blocked, Nil)
      _ = assertEquals(blockedProjection, (0L, 0L))
    yield ()

  test("graph projection keeps endpoints closed, repairs dangling edges, and avoids starve-out"):
    requireDocker()
    val bssid = "02:00:00:aa:00:01"
    val uninventoried = "02:00:00:cc:00:01"
    def frames: IO[Unit] =
      (for
        _ <- sql"""INSERT INTO wireless_frames
                     (dedupe_key, source_mac, bssid, observed_at, frame_type, frame_subtype)
                     SELECT CONCAT('closure-bb-', device, '-', sample),
                            CONCAT('02:00:00:bb:00:0', device), $bssid::text,
                            TIMESTAMPTZ '2026-09-01 00:00:00+00' + sample * INTERVAL '1 second',
                            'management', 'beacon'
                     FROM generate_series(1, 4) device,
                          generate_series(1, 3) sample""".update.run
        _ <- sql"""INSERT INTO wireless_frames
                     (dedupe_key, source_mac, bssid, observed_at, frame_type, frame_subtype)
                     SELECT CONCAT('closure-cc-', sample),
                            $uninventoried::text, $bssid::text,
                            TIMESTAMPTZ '2026-09-01 00:00:10+00' + sample * INTERVAL '1 second',
                            'management', 'beacon'
                     FROM generate_series(1, 3) sample""".update.run
        _ <- sql"""INSERT INTO devices (mac_id, mac_hint, first_seen, last_seen)
                     SELECT CONCAT('02:00:00:bb:00:0', device),
                            CONCAT('02:00:00:bb:00:0', device),
                            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                     FROM generate_series(1, 4) device
                     ON CONFLICT (mac_id) DO NOTHING""".update.run
      yield ()).transact(xa)

    for
      _ <- frames
      _ <- sql"""INSERT INTO atheros_search.graph_edges
                   (edge_id, source_node_id, target_node_id, edge_kind, weight, weight_basis,
                    evidence, projection_run_id)
                   VALUES ('observed:stale:dangling', 'device:02:00:00:dd:00:01',
                           CONCAT('ap:', $bssid::text),
                           'observed_at', 1, 'frame_count', '{}'::jsonb, 'stale-run')""".update.run
        .transact(xa)
      // First run with a small batch: pending-first must still cover cold pairs
      // across ticks instead of refreshing only the hottest.
      first <- repository.projectInfrastructureGraph(2)
      _ = requireRight(first)
      second <- repository.projectInfrastructureGraph(2)
      _ = requireRight(second)
      third <- repository.projectInfrastructureGraph(2)
      _ = requireRight(third)
      closed <- sql"""SELECT COUNT(*)
                      FROM atheros_search.graph_edges edge
                      LEFT JOIN atheros_search.graph_nodes source_node
                        ON source_node.node_id = edge.source_node_id
                      LEFT JOIN atheros_search.graph_nodes target_node
                        ON target_node.node_id = edge.target_node_id
                      WHERE source_node.node_id IS NULL OR target_node.node_id IS NULL"""
        .query[Long]
        .unique
        .transact(xa)
      observed <- sql"""SELECT COUNT(*)
                        FROM atheros_search.graph_edges
                        WHERE edge_kind = 'observed_at' AND edge_id LIKE 'observed:02:00:00:bb:%'"""
        .query[Long]
        .unique
        .transact(xa)
      unobserved <- sql"""SELECT COUNT(*)
                          FROM atheros_search.graph_edges
                          WHERE edge_id = CONCAT('observed:', $uninventoried::text, ':', $bssid::text)"""
        .query[Long]
        .unique
        .transact(xa)
      stubs <- sql"""SELECT COUNT(*)
                     FROM atheros_search.graph_nodes
                     WHERE node_id = CONCAT('device:', $uninventoried::text)"""
        .query[Long]
        .unique
        .transact(xa)
      deviceNodes <- sql"""SELECT COUNT(*)
                           FROM atheros_search.graph_nodes
                           WHERE node_kind = 'device' AND normalized_mac LIKE '02:00:00:bb:%'"""
        .query[Long]
        .unique
        .transact(xa)
      coverage <- sql"""SELECT coverage_status
                        FROM atheros_search.investigation_watermarks
                        WHERE projection_name = 'graph_projection'"""
        .query[String]
        .unique
        .transact(xa)
      _ = assertEquals(closed, 0L)
      _ = assertEquals(observed, 4L)
      _ = assertEquals(unobserved, 0L)
      _ = assertEquals(stubs, 0L)
      _ = assertEquals(deviceNodes, 4L)
      _ = assert(coverage == "partial" || coverage == "complete")
    yield ()

  test(
    "historical hydration normalizes null-like projections without changing durable payloads"
  ):
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
      _ <- sql"""UPDATE sync_events SET event_kind = 'wifi_management_frame'
                  WHERE dedupe_key = $payloadHash AND stream_name = 'wireless.audit'""".update.run.transact(xa)
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
      rawBefore <-
        sql"""SELECT CAST(payload AS TEXT), payload_sha256, payload_ref, event_kind
                          FROM sync_events
                          WHERE dedupe_key = $payloadHash
                            AND stream_name = 'wireless.audit'"""
          .query[(String, String, String, Option[String])]
          .unique
          .transact(xa)
      candidates <- repository
        .findSyncEventsNeedingHydration(None, 100)
        .map(requireRight)
      candidate = candidates
        .find(_.dedupeKey == payloadHash)
        .getOrElse(fail("expected sparse wireless row in hydration page"))
      nullSchemaCandidate = candidates
        .find(_.dedupeKey == nullSchemaHash)
        .getOrElse(
          fail("expected JSON-null schema version row in hydration page")
        )
      _ = assert(!candidates.exists(_.dedupeKey == archivedHash))
      _ = assert(!candidates.exists(_.dedupeKey == tombstonedHash))
      hydrated <- repository
        .hydrateExistingSyncEvent(candidate, candidate.payloadJson.getOrElse(fail("expected stored body")))
        .map(requireRight)
      nullSchemaHydrated <- repository
        .hydrateExistingSyncEvent(
          nullSchemaCandidate,
          nullSchemaPayload
        )
        .map(requireRight)
      rawAfter <-
        sql"""SELECT CAST(payload AS TEXT), payload_sha256, payload_ref, event_kind
                         FROM sync_events
                         WHERE dedupe_key = $payloadHash
                           AND stream_name = 'wireless.audit'"""
          .query[(String, String, String, Option[String])]
          .unique
          .transact(xa)
      state <- sql"""SELECT schema_version, ssid, signal_dbm, noise_dbm,
                            frequency_mhz, channel_flags, raw_len, frame_control_flags,
                            retry, risk_score, mixed_encryption, protected
                     FROM sync_events
                     WHERE dedupe_key = $payloadHash
                       AND stream_name = 'wireless.audit'"""
        .query[
          (
            Int,
            String,
            Int,
            Option[Int],
            Int,
            Int,
            Int,
            Int,
            Boolean,
            Option[Double],
            Option[Boolean],
            Boolean
          )
        ]
        .unique
        .transact(xa)
      nullSchemaVersion <- sql"""SELECT schema_version
                                  FROM sync_events
                                  WHERE dedupe_key = $nullSchemaHash
                                    AND stream_name = 'wireless.audit'"""
        .query[Int]
        .unique
        .transact(xa)
      remaining <- repository
        .findSyncEventsNeedingHydration(None, 100)
        .map(requireRight)
    yield
      assert(hydrated)
      assert(nullSchemaHydrated)
      assertEquals(rawAfter, rawBefore)
      assertEquals(rawAfter._2, payloadHash)
      assertEquals(rawAfter._3, payloadRef)
      assertEquals(rawAfter._4, Some("wifi_management_frame"))
      assertEquals(
        state,
        (1, "null", -42, None, 5180, 256, 0, 0, true, None, None, false)
      )
      assertEquals(nullSchemaVersion, 1)
      assert(!remaining.exists(_.dedupeKey == payloadHash))
      assert(!remaining.exists(_.dedupeKey == nullSchemaHash))

  test(
    "shadow alert generation skips null-like signals and uses maintained projections"
  ):
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
                   ${inlineRef(
          nullSignalPayload
        )}, $nullSignalKey, $nullSignalPayload,
                   'completed', 'ssl-proxy', 'aa:bb:cc:dd:ee:20', NULL
                 )""".update.run.transact(xa)
      _ <- sql"""INSERT INTO sync_events (
                   dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                   payload, status, producer, source_mac, signal_dbm
                 ) VALUES (
                   $validSignalKey, 'wireless.audit', CURRENT_TIMESTAMP,
                   ${inlineRef(
          validSignalPayload
        )}, $validSignalKey, $validSignalPayload,
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

  test(
    "normalization fills a bounded frame and repairs missing children on replay"
  ):
    requireDocker()
    val keys = List("normalizer-repair-1", "normalizer-repair-2")
    val seed = keys.zipWithIndex.traverse_ { case (key, index) =>
      sql"""INSERT INTO sync_events (
               dedupe_key, stream_name, observed_at, payload_ref, payload,
               sensor_id, location_id, frame_control_flags, source_mac
             ) VALUES (
               $key, 'wireless.audit', ${java.sql.Timestamp.from(
          java.time.Instant.EPOCH.plusSeconds(index.toLong)
        )},
               'inline://test', '{}', 'normalizer-test', 'lab', 0, 'aa:bb:cc:dd:ee:28'
             )""".update.run.void
    }
    for
      _ <- seed.transact(xa)
      first <- repository.normalizeWirelessFrames(1).map(requireRight)
      _ = assertEquals(first, 7)
      count <-
        sql"""SELECT COUNT(*) FROM wireless_frames WHERE sensor_id = 'normalizer-test'"""
          .query[Long]
          .unique
          .transact(xa)
      _ = assertEquals(count, 1L)
      _ <-
        sql"""DELETE FROM wireless_frame_network WHERE dedupe_key = ${keys.head}""".update.run
          .transact(xa)
      _ <- repository.normalizeWirelessFrames(1).map(requireRight)
      network <-
        sql"""SELECT COUNT(*) FROM wireless_frame_network WHERE dedupe_key = ${keys.head}"""
          .query[Long]
          .unique
          .transact(xa)
      countAfter <-
        sql"""SELECT COUNT(*) FROM wireless_frames WHERE sensor_id = 'normalizer-test'"""
          .query[Long]
          .unique
          .transact(xa)
    yield
      assertEquals(network, 1L)
      assertEquals(countAfter, 2L)

  test(
    "normalization projects archived events after the payload leaves PostgreSQL"
  ):
    requireDocker()
    val key = "normalizer-archived-1"
    val seed =
      sql"""INSERT INTO sync_events (
               dedupe_key, stream_name, observed_at, payload_ref, payload,
               payload_sha256, payload_archived, payload_archived_at,
               sensor_id, location_id, frame_control_flags, source_mac
             ) VALUES (
               $key, 'wireless.audit', ${java.sql.Timestamp.from(
          java.time.Instant.EPOCH.plusSeconds(7L)
        )},
               'inline://test', NULL, ${"ab" * 32}, true, CURRENT_TIMESTAMP,
               'normalizer-archived', 'lab', 0, 'aa:bb:cc:dd:ee:2a'
             )""".update.run.void
    for
      _ <- seed.transact(xa)
      _ <- repository.normalizeWirelessFrames(200).map(requireRight)
      frames <-
        sql"""SELECT COUNT(*) FROM wireless_frames WHERE dedupe_key = $key"""
          .query[Long]
          .unique
          .transact(xa)
      radio <-
        sql"""SELECT COUNT(*) FROM wireless_frame_radio WHERE dedupe_key = $key"""
          .query[Long]
          .unique
          .transact(xa)
    yield
      assertEquals(frames, 1L)
      assertEquals(radio, 1L)

  test(
    "shadow alerts aggregate repeated devices and mark exactly the selected inputs"
  ):
    requireDocker()
    val mac = "aa:bb:cc:dd:ee:29"
    val now = java.time.Instant.now()
    val observations = List((1, 30L, -42), (2, 10L, -35), (3, 20L, -38))
    val seed = observations.traverse_ { case (index, age, signal) =>
      val key = Sha256Utils.sha256Hex(s"shadow-alert-batch-$index")
      sql"""INSERT INTO sync_events (
               dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
               payload, status, source_mac, signal_dbm, ssid
             ) VALUES (
               $key, 'wireless.audit', ${java.sql.Timestamp.from(
          now.minusSeconds(age)
        )},
               'inline://test', $key, '{}', 'completed', $mac, $signal, ${s"network-$index"}
             )""".update.run.void
    }

    for
      _ <- seed.transact(xa)
      first <- repository.generateShadowAlerts(100).map(requireRight)
      repeated <- repository.generateShadowAlerts(100).map(requireRight)
      stored <-
        sql"""SELECT occurrence_count, signal_dbm, ssid, first_occurred_at, last_occurred_at
                       FROM wireless_shadow_alerts WHERE source_mac = $mac"""
          .query[(Long, Int, String, java.sql.Timestamp, java.sql.Timestamp)]
          .unique
          .transact(xa)
      marked <-
        sql"""SELECT COUNT(*) FROM wireless_shadow_alert_inputs WHERE source_mac = $mac"""
          .query[Long]
          .unique
          .transact(xa)
    yield
      assertEquals(first.count(_.contains(mac)), 1)
      assert(!repeated.exists(_.contains(mac)))
      assertEquals(stored._1, 3L)
      assertEquals(stored._2, -35)
      assertEquals(stored._3, "network-2")
      assertEquals(
        stored._4.toInstant.toEpochMilli,
        now.minusSeconds(30L).toEpochMilli
      )
      assertEquals(
        stored._5.toInstant.toEpochMilli,
        now.minusSeconds(10L).toEpochMilli
      )
      assertEquals(marked, 3L)

  test(
    "concurrent shadow alert replays aggregate only the inputs each call claims"
  ):
    requireDocker()
    val mac = "aa:bb:cc:dd:ee:30"
    val now = java.time.Instant.now()
    val keys = (1 to 3).toList.map(index => Sha256Utils.sha256Hex(s"shadow-alert-concurrent-$index"))
    val seed = keys.zipWithIndex.traverse_ { case (key, index) =>
      sql"""INSERT INTO sync_events (
               dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
               payload, status, source_mac, signal_dbm
             ) VALUES (
               $key, 'wireless.audit', ${java.sql.Timestamp.from(
          now.minusSeconds(30L - index * 10L)
        )},
               'inline://test', $key, '{}', 'completed', $mac, -40
             )""".update.run.void
    }

    for
      _ <- seed.transact(xa)
      firstProjected <- Deferred[IO, Int]
      allowCommit <- Deferred[IO, Unit]
      first = xa.liftF { liftIO =>
        for
          pid <- sql"SELECT pg_backend_pid()".query[Int].unique
          alerts <- ProjectionSql.generateShadowAlerts(60, -50, 300, 2)
          _ <- liftIO(firstProjected.complete(pid) *> allowCommit.get)
        yield alerts
      }
      replays = (
        ProjectionSql.generateShadowAlerts(60, -50, 300, 2).transact(xa),
        ProjectionSql.generateShadowAlerts(60, -50, 300, 100).transact(xa)
      ).parTupled
      results <- first.background.use { firstResult =>
        for
          pid <- firstProjected.get.timeout(10.seconds)
          alerts <- replays.background.use { replayResult =>
            (for
              // Both replays must select their inputs before the first transaction commits.
              _ <- sql"""SELECT COUNT(*) FROM pg_stat_activity
                           WHERE $pid = ANY(pg_blocking_pids(pid))"""
                .query[Long]
                .unique
                .transact(xa)
                .flatTap(count => IO.sleep(10.millis).unlessA(count == 2L))
                .iterateUntil(_ == 2L)
                .timeout(10.seconds)
              _ <- allowCommit.complete(())
              initial <- firstResult.flatMap(_.embedNever)
              replayed <- replayResult.flatMap(_.embedNever)
            yield (initial, replayed))
              .guarantee(allowCommit.complete(()).void)
          }
        yield alerts
      }
      stored <-
        sql"""SELECT occurrence_count FROM wireless_shadow_alerts WHERE source_mac = $mac"""
          .query[Long]
          .unique
          .transact(xa)
      marked <- sql"""SELECT dedupe_key FROM wireless_shadow_alert_inputs
                       WHERE source_mac = $mac ORDER BY dedupe_key"""
        .query[String]
        .to[List]
        .transact(xa)
      repeated <- repository.generateShadowAlerts(100).map(requireRight)
    yield
      val (initial, (duplicateReplay, overlappingReplay)) = results
      assertEquals(initial.count(_.contains(mac)), 1)
      assertEquals(duplicateReplay, Nil)
      assertEquals(overlappingReplay.count(_.contains(mac)), 1)
      assertEquals(stored, 3L)
      assertEquals(marked, keys.sorted)
      assert(!repeated.exists(_.contains(mac)))

  test(
    "archived retention honors pending outboxes and deletes terminal UUID dependencies"
  ):
    requireDocker()
    val payload =
      """{"observed_at":"2025-01-01T00:00:00Z","event_type":"wifi_data_frame","sensor_id":"retention-test"}"""
    val record = resolvedWireless(payload)
    val resourceType = "maintenance"
    val resourceId = "retention-integration-test"

    for
      decision <- persist(record, offset = 9501L)
      _ <-
        sql"""UPDATE sync_events SET payload = NULL, payload_archived = true
                  WHERE dedupe_key = ${record.dedupeKey}""".update.run
          .transact(xa)
      _ <- sql"""INSERT INTO sync_event_payload_archives
                   (dedupe_key, stream_name, observed_at, payload_sha256, archive_uri)
                 SELECT dedupe_key, stream_name, observed_at, payload_sha256, 's3://test/retention'
                 FROM sync_events WHERE dedupe_key = ${record.dedupeKey}""".update.run
        .transact(xa)
      _ <- ResultSql
        .completeSuccessful(
          PostgresResult.success(decision.jobId, decision.batchId, 1, "retention", "2025-01-01T00:00:01Z"),
          record.streamName,
          "1735689600"
        )
        .transact(xa)
      blocked <- MaintenanceSql
        .retentionCandidates(30, 100)
        .to[List]
        .transact(xa)
      _ = assert(!blocked.exists(_._1 == record.dedupeKey))
      _ <-
        sql"""UPDATE outbox_events SET status = 'published' WHERE source_id = ${decision.batchId}""".update.run
          .transact(xa)
      _ <- sql"""INSERT INTO outbox_publish_attempts (outbox_id, attempt_no, status)
                 SELECT outbox_id, 1, 'published' FROM outbox_events WHERE source_id = ${decision.batchId}""".update.run
        .transact(xa)
      _ <-
        sql"""INSERT INTO sync_errors (job_id, batch_id, error_class, error_text)
                 VALUES (${decision.jobId}, ${decision.batchId}, 'test', 'retention fixture')""".update.run
          .transact(xa)
      lease <- repository
        .claimMaintenanceLease(
          resourceType,
          resourceId,
          "test-worker",
          java.util.UUID.randomUUID().toString,
          60
        )
        .map(result => requireRight(result).getOrElse(fail("expected maintenance lease")))
      result <- repository
        .retainArchivedEvents(30, 90, 100, resourceType, resourceId, lease)
        .map(requireRight)
      remaining <-
        sql"""SELECT COUNT(*) FROM sync_events WHERE dedupe_key = ${record.dedupeKey}"""
          .query[Long]
          .unique
          .transact(xa)
      outboxes <-
        sql"""SELECT COUNT(*) FROM outbox_events WHERE source_id = ${decision.batchId}"""
          .query[Long]
          .unique
          .transact(xa)
      errors <-
        sql"""SELECT COUNT(*) FROM sync_errors WHERE batch_id = ${decision.batchId}"""
          .query[Long]
          .unique
          .transact(xa)
      tombstones <-
        sql"""SELECT COUNT(*) FROM sync_event_tombstones WHERE dedupe_key = ${record.dedupeKey}"""
          .query[Long]
          .unique
          .transact(xa)
      archives <-
        sql"""SELECT COUNT(*) FROM sync_event_payload_archives WHERE dedupe_key = ${record.dedupeKey}"""
          .query[Long]
          .unique
          .transact(xa)
    yield
      assertEquals(result, 1L -> 1L)
      assertEquals((remaining, outboxes, errors), (0L, 0L, 0L))
      assertEquals((tombstones, archives), (1L, 1L))

  test(
    "load acknowledgement binds batch_id without a JSON collation comparison"
  ):
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
      claimed = requireRight(claimedResult).getOrElse(
        fail("expected a claimed load outbox record")
      )
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

  test(
    "broker loads and results require an exact durable coordinator dispatch and outcome"
  ):
    requireDocker()
    val rawJson =
      """{"observed_at":"2026-07-25T20:20:00Z","host":"authorization.example","body":{"ok":true}}"""
    val record = translatedAudit(rawJson, offset = 9200L)
    def metadata(
      topic: String,
      offset: Long,
      key: String
    ): BrokerRecordMetadata =
      BrokerRecordMetadata(
        topic,
        0,
        offset,
        "authorization-integration",
        1,
        ArtifactSha256,
        Some(key),
        "b" * 64
      )
    for
      _ <- parkPendingLoadOutboxes()
      decision <- persist(record, offset = 9200L)
      stored <- sql"""SELECT payload::text FROM outbox_events
                       WHERE source_id = ${decision.batchId} AND event_type = 'sync.load.requested'"""
        .query[String]
        .unique
        .transact(xa)
      load <- IO.fromEither(io.circe.parser.decode[PostgresLoad](stored))
      _ <- DispatchAuthorizationSql.authorizeLoad(load).transact(xa)
      rejected <- List(
        load.copy(batchId = "unknown-batch"),
        load.copy(jobId = "other-job"),
        load.copy(streamName = "proxy.events"),
        load.copy(payloadRef = "inline://json/W10"),
        load.copy(cursorEnd = "forged-cursor"),
        load.copy(attempt = load.attempt + 1),
        load.copy(batchNo = None)
      ).traverse(candidate => DispatchAuthorizationSql.authorizeLoad(candidate).transact(xa).attempt)
      _ = assert(rejected.forall(_.isLeft))
      handler = new PostgresLoadHandler(
        new PostgresPayloadResolver("/tmp"),
        PostgresTransformService,
        schemaTransactor,
        PostgresClock,
        sha => IngestionSql.payloadBySha256(sha).unique.transact(xa).map(Some(_)),
        candidate => DispatchAuthorizationSql.authorizeLoad(candidate).transact(xa)
      )
      forged <- handler
        .handle(load.copy(payloadRef = "inline://json/W10"))
        .attempt
      _ = assert(forged.isLeft)
      result <- handler.handle(load)
      _ = assertEquals(result.status, "success")
      key = s"${load.batchId}:${load.attempt}"
      noOutcome <- repository.recordResultWithEvidence(
        result,
        metadata("sync.oracle.result", 9201L, key)
      )
      _ = assert(noOutcome.isLeft)
      _ <- repository
        .recordLoadResultWithEvidence(
          load,
          result,
          metadata("sync.oracle.load", 9202L, key)
        )
        .map(requireRight)
      altered <- repository.recordResultWithEvidence(
        result.copy(rowCount = result.rowCount + 1),
        metadata("sync.oracle.result", 9203L, key)
      )
      missingKey <- repository.recordResultWithEvidence(
        result,
        metadata("sync.oracle.result", 9204L, key).copy(messageKey = None)
      )
      _ = assert(altered.isLeft && missingKey.isLeft)
      _ <- repository
        .recordResultWithEvidence(
          result,
          metadata("sync.oracle.result", 9205L, key)
        )
        .map(requireRight)
      completedAt <- sql"""SELECT updated_at FROM sync_events
                           WHERE dedupe_key = ${record.dedupeKey} AND stream_name = ${record.streamName}"""
        .query[java.sql.Timestamp]
        .unique
        .transact(xa)
      _ <- repository
        .recordResultWithEvidence(
          result,
          metadata("sync.oracle.result", 9205L, key)
        )
        .map(requireRight)
      _ <- repository
        .recordResultWithEvidence(result, metadata("sync.oracle.result", 9206L, key))
        .map(requireRight)
      replayedAt <- sql"""SELECT updated_at FROM sync_events
                          WHERE dedupe_key = ${record.dedupeKey} AND stream_name = ${record.streamName}"""
        .query[java.sql.Timestamp]
        .unique
        .transact(xa)
      _ = assertEquals(replayedAt, completedAt)
      status <-
        sql"""SELECT batch.status, job.status, event.status
               FROM sync_batches batch
               JOIN sync_jobs job ON job.job_id = batch.job_id
               JOIN sync_events event ON event.dedupe_key = batch.dedupe_key
                 AND event.stream_name = batch.stream_name
               WHERE batch.batch_id = ${load.batchId}"""
          .query[(String, String, String)]
          .unique
          .transact(xa)
      _ = assertEquals(status, ("completed", "completed", "completed"))
      _ <- sql"""UPDATE outbox_events SET message_key = ${s"${load.batchId}:2"}
                  WHERE source_id = ${load.batchId} AND event_type = 'sync.load.requested'""".update.run
        .transact(xa)
      stale <- DispatchAuthorizationSql
        .authorizeResult(result, Some(key))
        .transact(xa)
        .attempt
    yield assert(stale.isLeft)

  test("completion recovery repairs anonymized Wiretrap samples without changing unfinished work or search"):
    requireDocker()
    val documentId = "0e000000-0000-0000-0000-000000000001"
    def checkpoint = sql"""SELECT checkpoint_value, rows_processed, status FROM processor_checkpoints
                          WHERE processor_name = 'sync-backlog-recovery' AND shard_id = 'event-completion-v1'"""
      .query[(Option[String], Long, String)]
      .unique
      .transact(xa)
    def searchState = sql"""SELECT document.status, document.normalized_text, embedding.content_sha256,
                                  embedding.embedding::text
                           FROM atheros_search.search_documents document
                           JOIN atheros_search.embeddings embedding USING (document_id)
                           WHERE document.document_id = $documentId::uuid"""
      .query[(String, String, String, String)]
      .unique
      .transact(xa)

    for
      json <- IO.blocking(
        Files.readString(Path.of("src/test/resources/fixtures/wiretrap-event-completion-2026-10-16.json"))
      )
      samples <- IO.fromEither(circeParser.parse(json).flatMap(_.hcursor.get[List[CompletionSample]]("samples")))
      _ = assertEquals(samples.size, 32)
      _ = assertEquals(samples.count(_.batchStatus == "completed"), 16)
      _ <- samples.zipWithIndex.traverse_ { case (sample, index) =>
        seedCompletionSample(f"completion-sample-$index%03d", sample)
      }
      _ <- sql"""INSERT INTO atheros_search.search_documents (
                   document_id, source_kind, source_id, source_key, source_table,
                   normalized_text, normalized_sha256, search_vector
                 ) VALUES ($documentId::uuid, 'event', 'completion-sample-search', 'completion-sample-000',
                           'wireless_frames', 'retained evidence',
                           ${"a" * 64}, to_tsvector('simple', 'retained evidence'))""".update.run.transact(xa)
      _ <- sql"""INSERT INTO atheros_search.embeddings (
                   document_id, embedding_kind, embedding_model, content_sha256, embedding, embedded_at
                 ) VALUES ($documentId::uuid, 'event', 'completion-sample-model', ${"a" * 64},
                           array_fill(0.1::real, ARRAY[768])::public.vector, CURRENT_TIMESTAMP)""".update.run
        .transact(xa)
      before <- searchState
      _ <- sql"""DELETE FROM processor_checkpoints WHERE processor_name = 'sync-backlog-recovery'
                 AND shard_id = 'event-completion-v1'""".update.run.transact(xa)
      rolledBack <- (ResultSql.reconcileCompletedEvents(7) *>
        doobie.free.connection.raiseError[Int](IllegalStateException("rollback completion repair")))
        .transact(xa)
        .attempt
      _ = assert(rolledBack.isLeft)
      checkpointCount <- sql"""SELECT count(*) FROM processor_checkpoints
                              WHERE processor_name = 'sync-backlog-recovery'
                                AND shard_id = 'event-completion-v1'""".query[Long].unique.transact(xa)
      _ = assertEquals(checkpointCount, 0L)
      first <- repository.reconcileCompletedEvents(7).map(requireRight)
      firstCheckpoint <- checkpoint
      _ = assert(first >= 0 && first <= 7)
      _ = assertEquals(firstCheckpoint._2, 7L)
      restarted = new PostgresRepository(xa)
      _ <- (
        restarted.reconcileCompletedEvents(7).map(requireRight),
        restarted.reconcileCompletedEvents(7).map(requireRight)
      ).parTupled
      concurrentCheckpoint <- checkpoint
      _ = assertEquals(concurrentCheckpoint._2, 21L)
      _ <- (restarted.reconcileCompletedEvents(7).map(requireRight) *> checkpoint)
        .iterateUntil(_._3 == "healthy")
        .timeout(15.seconds)
      statuses <- sql"""SELECT dedupe_key, status, payload::text, payload_ref FROM sync_events
                         WHERE dedupe_key LIKE 'completion-sample-%' ORDER BY dedupe_key"""
        .query[(String, String, String, String)]
        .to[List]
        .transact(xa)
      after <- searchState
      completedCheckpoint <- checkpoint
      repeated <- restarted.reconcileCompletedEvents(7).map(requireRight)
      repeatedCheckpoint <- checkpoint
    yield
      assertEquals(
        statuses.map(_._2),
        samples.map { sample =>
          if sample.jobStatus == "completed" && sample.batchStatus == "completed" then "completed"
          else sample.eventStatus
        }
      )
      assert(statuses.forall(row => row._3 == "{}" && row._4 == "inline://json/e30"))
      assertEquals(after, before)
      assertEquals(repeated, 0)
      assertEquals(repeatedCheckpoint, completedCheckpoint)

  test(
    "completion finalization preserves quarantine and waits for every batch; retry and failure do not complete events"
  ):
    requireDocker()
    val key = "completion-guards"
    val pending = CompletionSample("wireless.audit", "batched", "pending", "pending", 0, 5, 1)
    def result(id: String) = PostgresResult.success(id, id, 1, "guard-test", "2026-10-16T00:00:00Z")
    def eventStatus(id: String) =
      sql"SELECT status FROM sync_events WHERE dedupe_key = $id AND stream_name = 'wireless.audit'"
        .query[String]
        .unique
        .transact(xa)

    for
      _ <- seedCompletionSample(key, pending)
      _ <- seedCompletionSample("completion-quarantined", pending.copy(eventStatus = "failed"))
      _ <- seedCompletionSample("completion-retry", pending)
      _ <- seedCompletionSample("completion-failure", pending)
      _ <- seedCompletionSample("completion-unfinished", pending.copy(jobStatus = "completed"))
      _ <- seedCompletionSample(
        "completion-cancelled",
        pending.copy(jobStatus = "cancelled", batchStatus = "cancelled")
      )
      _ <- seedCompletionSample("completion-orphan", pending.copy(jobStatus = "completed", batchStatus = "completed"))
      _ <- sql"DELETE FROM sync_batches WHERE batch_id = 'completion-orphan'".update.run.transact(xa)
      _ <- sql"""INSERT INTO sync_events (dedupe_key, stream_name, observed_at, payload_ref, status)
                 VALUES ($key, 'proxy.events', CURRENT_TIMESTAMP, 'inline://json/e30', 'batched')""".update.run
        .transact(xa)
      _ <- sql"""INSERT INTO sync_batches (
                   batch_id, job_id, batch_no, payload_ref, dedupe_key, stream_name, cursor_start, cursor_end
                 ) VALUES ('completion-guards-sibling', $key, 1, 'inline://json/e30',
                           'completion-guards-sibling', 'wireless.audit', '0', '1')""".update.run.transact(xa)
      _ <- ResultSql.completeSuccessful(result(key), "wireless.audit", "1").transact(xa)
      partial <- eventStatus(key)
      _ <- ResultSql
        .completeSuccessful(result(key).copy(batchId = "completion-guards-sibling"), "wireless.audit", "1")
        .transact(xa)
      complete <- eventStatus(key)
      unrelated <- sql"SELECT status FROM sync_events WHERE dedupe_key = $key AND stream_name = 'proxy.events'"
        .query[String]
        .unique
        .transact(xa)
      _ <- ResultSql.completeSuccessful(result("completion-quarantined"), "wireless.audit", "1").transact(xa)
      quarantined <- eventStatus("completion-quarantined")
      retryState <- ResultSql.batchForUpdate("completion-retry").unique.transact(xa)
      failedState <- ResultSql.batchForUpdate("completion-failure").unique.transact(xa)
      _ <- ResultSql.scheduleRetry(result("completion-retry").copy(status = "failed"), retryState).transact(xa)
      _ <- ResultSql.completeFailed(result("completion-failure").copy(status = "failed"), failedState).transact(xa)
      retry <- eventStatus("completion-retry")
      failed <- eventStatus("completion-failure")
      stale <- ResultSql.completeSuccessful(result("completion-failure"), "wireless.audit", "1").transact(xa).attempt
      _ <- sql"""DELETE FROM processor_checkpoints WHERE processor_name = 'sync-backlog-recovery'
                 AND shard_id = 'event-completion-v1'""".update.run.transact(xa)
      _ <- repository.reconcileCompletedEvents(1000).map(requireRight)
      recoveredQuarantine <- eventStatus("completion-quarantined")
      recoveredFailure <- eventStatus("completion-failure")
      unfinished <- eventStatus("completion-unfinished")
      cancelled <- eventStatus("completion-cancelled")
      orphan <- eventStatus("completion-orphan")
    yield
      assertEquals(partial, "batched")
      assertEquals(complete, "completed")
      assertEquals(unrelated, "batched")
      assertEquals(quarantined, "failed")
      assertEquals(retry, "batched")
      assertEquals(failed, "batched")
      assert(stale.isLeft)
      assertEquals(recoveredQuarantine, "failed")
      assertEquals(recoveredFailure, "batched")
      assertEquals(unfinished, "batched")
      assertEquals(cancelled, "batched")
      assertEquals(orphan, "batched")

  private def seedCompletionSample(key: String, sample: CompletionSample): IO[Unit] =
    (for
      _ <- sql"""INSERT INTO sync_events (
                   dedupe_key, stream_name, observed_at, payload_ref, payload, payload_sha256, status
                 ) VALUES ($key, ${sample.streamName}, TIMESTAMPTZ '2026-10-12 00:00:00Z',
                           'inline://json/e30', '{}'::jsonb, ${"a" * 64}, ${sample.eventStatus})""".update.run
      _ <- sql"""INSERT INTO sync_jobs (job_id, dedupe_key, stream_name, status)
                 VALUES ($key, $key, ${sample.streamName}, ${sample.jobStatus})""".update.run
      _ <- sql"""INSERT INTO sync_batches (
                   batch_id, job_id, dedupe_key, stream_name, payload_ref, status,
                   attempt_count, max_attempts, row_count, cursor_start, cursor_end
                 ) VALUES ($key, $key, $key, ${sample.streamName}, 'inline://json/e30', ${sample.batchStatus},
                           ${sample.attemptCount}, ${sample.maxAttempts}, ${sample.rowCount}, '0', '1')""".update.run
    yield ()).transact(xa)

  test(
    "invalid load batch_id is parked after publication without dispatching its batch"
  ):
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
      claimed = requireRight(claimedResult).getOrElse(
        fail("expected a claimed load outbox record")
      )
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

  test(
    "outbox enqueue keeps published messages closed and reopens failed ones"
  ):
    requireDocker()
    val record = translatedAudit(
      """{"observed_at":"2026-07-25T20:01:30Z","host":"idempotent-outbox.example"}""",
      offset = 90L
    )
    val resultBatchId = "22222222-2222-2222-2222-222222222222"
    val firstResult = PostgresResult.success(
      jobId = "33333333-3333-3333-3333-333333333333",
      batchId = resultBatchId,
      rowCount = 1,
      checksum = "first",
      finishedAt = "2026-07-25T20:01:30Z"
    )

    for
      _ <- parkPendingLoadOutboxes()
      decision <- persist(record, offset = 90L)
      _ <- sql"""UPDATE outbox_events
                  SET status = 'published', published_at = CURRENT_TIMESTAMP
                  WHERE destination_topic = 'sync.oracle.load'
                    AND source_id = ${decision.batchId}""".update.run.void
        .transact(xa)
      retriedLoad <- repository.prepareLoadDispatch(
        List(record.streamName),
        maxAttempts = 5,
        limit = 100
      )
      _ = requireRight(retriedLoad)
      loadState <-
        sql"""SELECT status, published_at IS NOT NULL, COUNT(*) OVER ()
                          FROM outbox_events
                          WHERE destination_topic = 'sync.oracle.load'
                            AND source_id = ${decision.batchId}"""
          .query[(String, Boolean, Long)]
          .unique
          .transact(xa)
      _ <- ResultSql
        .enqueue(
          firstResult,
          attempt = 1,
          outboxId = "44444444-4444-4444-4444-444444444444"
        )
        .transact(xa)
      _ <- sql"""UPDATE outbox_events
                  SET status = 'published', published_at = CURRENT_TIMESTAMP
                  WHERE destination_topic = 'sync.oracle.result'
                    AND message_key = ${s"$resultBatchId:1"}""".update.run.void
        .transact(xa)
      _ <- ResultSql
        .enqueue(
          firstResult.copy(rowCount = 2, checksum = "retry"),
          attempt = 1,
          outboxId = "55555555-5555-5555-5555-555555555555"
        )
        .transact(xa)
      resultState <- sql"""SELECT status, published_at IS NOT NULL,
                                   payload ->> 'row_count', outbox_id
                            FROM outbox_events
                            WHERE destination_topic = 'sync.oracle.result'
                              AND message_key = ${s"$resultBatchId:1"}"""
        .query[(String, Boolean, String, String)]
        .unique
        .transact(xa)
      _ <- sql"""UPDATE sync_batches SET status = 'pending'
                  WHERE batch_id = ${decision.batchId}""".update.run.void
        .transact(xa)
      _ <- sql"""UPDATE outbox_events
                  SET status = 'failed', last_error = 'publish attempts exhausted'
                  WHERE destination_topic IN ('sync.oracle.load', 'sync.oracle.result')
                    AND (source_id = ${decision.batchId}
                         OR message_key = ${s"$resultBatchId:1"})""".update.run.void
        .transact(xa)
      reopenedLoad <- repository.prepareLoadDispatch(
        List(record.streamName),
        maxAttempts = 5,
        limit = 100
      )
      _ = requireRight(reopenedLoad)
      _ <- ResultSql
        .enqueue(
          firstResult.copy(rowCount = 3, checksum = "recovered"),
          attempt = 1,
          outboxId = "66666666-6666-6666-6666-666666666666"
        )
        .transact(xa)
      reopened <- sql"""SELECT
          l.status, l.attempt_count, l.published_at IS NULL,
          (SELECT COUNT(*) FROM outbox_events
           WHERE destination_topic = 'sync.oracle.load'
             AND source_id = ${decision.batchId}),
          r.status, r.attempt_count, r.payload ->> 'row_count',
          (SELECT COUNT(*) FROM outbox_events
           WHERE destination_topic = 'sync.oracle.result'
             AND message_key = ${s"$resultBatchId:1"})
        FROM outbox_events l
        JOIN outbox_events r
          ON r.destination_topic = 'sync.oracle.result'
         AND r.message_key = ${s"$resultBatchId:1"}
        WHERE l.destination_topic = 'sync.oracle.load'
          AND l.source_id = ${decision.batchId}"""
        .query[(String, Int, Boolean, Long, String, Int, String, Long)]
        .unique
        .transact(xa)
      _ <- sql"""UPDATE outbox_events
                  SET status = 'published', published_at = CURRENT_TIMESTAMP
                  WHERE destination_topic = 'sync.oracle.result'
                    AND source_id = $resultBatchId""".update.run.void
        .transact(xa)
      _ <- ResultSql
        .enqueue(
          firstResult.copy(rowCount = 4, checksum = "second-attempt"),
          attempt = 2,
          outboxId = "77777777-7777-7777-7777-777777777777"
        )
        .transact(xa)
      secondAttempt <- sql"""SELECT outbox_id, message_key, status,
                                      payload ->> 'row_count', COUNT(*) OVER ()
                               FROM outbox_events
                               WHERE destination_topic = 'sync.oracle.result'
                                 AND source_id = $resultBatchId"""
        .query[(String, String, String, String, Long)]
        .unique
        .transact(xa)
      _ <- parkPendingLoadOutboxes()
    yield
      assertEquals(loadState, ("published", true, 1L))
      assertEquals(
        resultState,
        ("published", true, "1", "44444444-4444-4444-4444-444444444444")
      )
      assertEquals(reopened, ("pending", 0, true, 1L, "pending", 0, "3", 1L))
      assertEquals(
        secondAttempt,
        (
          "44444444-4444-4444-4444-444444444444",
          s"$resultBatchId:2",
          "pending",
          "4",
          1L
        )
      )

  test("load dispatch reuses the source outbox row for a later batch attempt"):
    requireDocker()
    val record = translatedAudit(
      """{"observed_at":"2026-07-25T20:01:31Z","host":"retry-outbox.example"}""",
      offset = 91L
    )

    for
      _ <- parkPendingLoadOutboxes()
      decision <- persist(record, offset = 91L)
      originalOutboxId <- sql"""SELECT outbox_id FROM outbox_events
                                  WHERE destination_topic = 'sync.oracle.load'
                                    AND source_id = ${decision.batchId}"""
        .query[String]
        .unique
        .transact(xa)
      _ <- (for
        _ <- sql"""UPDATE outbox_events
                     SET status = 'published', published_at = CURRENT_TIMESTAMP
                     WHERE outbox_id = $originalOutboxId""".update.run
        _ <- sql"""UPDATE sync_batches
                     SET status = 'pending', attempt_count = 1
                     WHERE batch_id = ${decision.batchId}""".update.run
      yield ()).transact(xa)
      dispatched <- repository.prepareLoadDispatch(
        List(record.streamName),
        maxAttempts = 5,
        limit = 100
      )
      _ = requireRight(dispatched)
      state <- sql"""SELECT outbox_id, message_key, status, attempt_count,
                             published_at IS NULL, COUNT(*) OVER ()
                      FROM outbox_events
                      WHERE source_type = 'sync_batch'
                        AND source_id = ${decision.batchId}
                        AND event_type = 'sync.load.requested'"""
        .query[(String, String, String, Int, Boolean, Long)]
        .unique
        .transact(xa)
      _ <- parkPendingLoadOutboxes()
    yield assertEquals(
      state,
      (originalOutboxId, s"${decision.batchId}:2", "pending", 0, true, 1L)
    )

  test("pgvector cosine operator resolves outside the public search path"):
    requireDocker()
    sql"""SELECT '[1,0]'::public.vector OPERATOR(public.<=>) '[1,0]'::public.vector"""
      .query[Double]
      .unique
      .transact(xa)
      .map(distance => assertEqualsDouble(distance, 0.0d, 0.000001d))

  test(
    "maximum accepted payload audit persists a compact payload_ref and stores full payload in payload column"
  ):
    requireDocker()
    val prefix = """{"observed_at":"2026-07-25T20:02:00Z","body":""""
    val suffix = "\"}"
    val filler = "x" * (ProxyMaxAuditBodyBytes - prefix.length - suffix.length)
    val rawJson = prefix + filler + suffix
    val record = translatedAudit(rawJson, offset = 3L)
    val expectedPayload = circeParser
      .parse(rawJson)
      .fold(
        error => fail(s"expected valid payload audit JSON, found $error"),
        identity
      )
    val expectedPayloadRef = circeParser
      .parse(record.requestJson)
      .toOption
      .flatMap(_.hcursor.get[String]("payload_ref").toOption)
      .getOrElse(fail("translated payload audit must contain payload_ref"))

    assertEquals(
      rawJson.getBytes(StandardCharsets.UTF_8).length,
      ProxyMaxAuditBodyBytes
    )
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
      assertEquals(
        eventRef,
        (expectedPayloadRef, expectedPayloadRef.length.toLong)
      )
      assertEquals(batchRef, eventRef)
      val parsedStoredPayload = circeParser
        .parse(storedPayload)
        .fold(
          error => fail(s"expected stored payload JSON, found $error"),
          identity
        )
      assertEquals(
        Printer.noSpacesSortKeys.print(parsedStoredPayload),
        Printer.noSpacesSortKeys.print(expectedPayload)
      )
      assertEquals(evidenceCount, 1L)

  test(
    "canonical manifest parsing is repeatable and retains TEXT payload capacity"
  ):
    requireDocker()
    IO.blocking {
      val first = canonicalStatements().map { case (path, statements) =>
        path -> statements.size
      }
      val second = canonicalStatements().map { case (path, statements) =>
        path -> statements.size
      }
      assertEquals(second, first)

      val searchRoot = schemaRoot.resolveSibling("atheros_search")
      val embeddingRecoveryStatements = canonicalStatements(
        canonicalManifest(searchRoot),
        searchRoot
      )
        .collectFirst { case ("01_tables/009_embedding_recovery_contract.sql", statements) =>
          statements
        }
        .getOrElse(
          fail(
            "embedding recovery contract is missing from the atheros_search manifest"
          )
        )
      val pgvectorRequirement = embeddingRecoveryStatements
        .find(_.trim.startsWith("DO $$"))
        .getOrElse(
          fail(
            "embedding recovery contract is missing its pgvector requirement block"
          )
        )
      assert(pgvectorRequirement.contains("RAISE EXCEPTION"))
      assert(pgvectorRequirement.trim.endsWith("END $$"))
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

  test(
    "scan request evidence batch rolls back atomically when one record is invalid"
  ):
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
        List(first.dedupeKey, second.dedupeKey)
          .map(value => fr0"$value")
          .intercalate(fr",") ++
        fr")").query[Long].unique.transact(xa)
    yield
      assert(result.isLeft)
      assertEquals(evidenceCount, 0L)
      assertEquals(eventCount, 0L)

  test("concurrent outbox claims lease each eligible row exactly once"):
    requireDocker()
    val topic = "test.concurrent.claim"
    val outboxIds = (1 to 12).toList.map { index =>
      java.util.UUID
        .nameUUIDFromBytes(s"$topic:$index".getBytes(StandardCharsets.UTF_8))
        .toString
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
        repository.claimOutbox(
          s"concurrent-owner-$index",
          List(topic),
          leaseSeconds = 60
        )
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
    val source =
      ScanRequestRecord.decodeWire(requestJson).fold(throw _, identity)
    new PostgresPayloadResolver("/unused").resolve(source)

  private def inlineRef(payload: String): String =
    "inline://json/" + Base64.getUrlEncoder.withoutPadding
      .encodeToString(payload.getBytes(StandardCharsets.UTF_8))

  private def translatedAudit(
    rawJson: String,
    offset: Long
  ): ResolvedScanRequestRecord =
    PayloadAuditConsumer
      .translateRecord(
        ConsumerRecord[String, String](
          "proxy.payload_audit",
          0,
          offset,
          null,
          rawJson
        )
      )
      .fold(
        error => fail(s"expected valid payload audit, found $error"),
        identity
      )

  private def persist(
    record: ResolvedScanRequestRecord,
    offset: Long
  ): IO[IngestionDecision] =
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
        .prepareLoadDispatch(
          List(record.streamName),
          maxAttempts = 5,
          limit = 100
        )
        .map(dispatch =>
          requireRight(dispatch): Unit
          decision
        )
    }

  test(
    "projection cursors preserve complete groups across fetch chunks, replay, and late arrivals"
  ):
    requireDocker()
    def seed(first: Int, last: Int): IO[Unit] =
      (for
        _ <- sql"""INSERT INTO wireless_frames
                    (dedupe_key, source_mac, bssid, observed_at, frame_type, frame_subtype)
                    SELECT CONCAT('streaming-', device, '-', hour, '-', sample),
                           CONCAT('02:00:00:99:00:0', device),
                           CONCAT('02:00:00:99:00:0', device),
                           TIMESTAMPTZ '2026-08-01 00:00:00+00'
                             + hour * INTERVAL '1 hour' + sample * INTERVAL '1 second',
                           'management', 'beacon'
                    FROM generate_series(1, 2) device,
                         generate_series(0, 1) hour,
                         generate_series($first, $last) sample""".update.run
        _ <-
          sql"""INSERT INTO wireless_frame_radio (dedupe_key, signal_dbm, tsft_delta_us, wall_clock_delta_ms)
                    SELECT dedupe_key, -60, 10, 2 FROM wireless_frames
                    WHERE dedupe_key LIKE 'streaming-%'
                    ON CONFLICT (dedupe_key) DO NOTHING""".update.run
        _ <- sql"""INSERT INTO wireless_frame_identity (dedupe_key, session_key)
                    SELECT dedupe_key, CONCAT('streaming-session-', source_mac)
                    FROM wireless_frames WHERE dedupe_key LIKE 'streaming-%'
                    ON CONFLICT (dedupe_key) DO NOTHING""".update.run
      yield ()).transact(xa)

    def project: IO[Unit] =
      (
        repository.projectBehavior(10000),
        repository.projectTiming(10000),
        repository.projectSequences(10000),
        repository.projectBaselines(10000)
      ).parTupled.map { case (behavior, timing, sequence, baseline) =>
        List(behavior, timing, sequence, baseline).foreach(requireRight)
      }

    def verify(samplesPerWindow: Long): IO[Unit] =
      (for
        behavior <-
          sql"""SELECT event_count FROM atheros_search.behaviour_snapshots
                          WHERE source_mac LIKE '02:00:00:99:00:%'"""
            .query[Long]
            .to[List]
        timing <- sql"""SELECT source_event_count, tsft_p50_us, wall_p50_ms
                        FROM atheros_search.timing_profiles
                        WHERE source_mac LIKE '02:00:00:99:00:%'"""
          .query[(Long, Double, Double)]
          .to[List]
        sequence <- sql"""SELECT frame_count FROM atheros_search.frame_sequences
                          WHERE session_key LIKE 'streaming-session-%'"""
          .query[Long]
          .to[List]
        baseline <- sql"""SELECT sample_count, p50 FROM atheros_search.baseline_profiles
                          WHERE bssid LIKE '02:00:00:99:00:%' AND metric = 'signal_dbm'"""
          .query[(Long, Double)]
          .to[List]
      yield
        assertEquals(behavior, List.fill(4)(samplesPerWindow))
        assertEquals(timing, List.fill(4)((samplesPerWindow, 10.0d, 2.0d)))
        assertEquals(sequence, List.fill(2)(samplesPerWindow * 2))
        assertEquals(baseline, List.fill(2)((samplesPerWindow * 2, -60.0d)))
      ).transact(xa)

    for
      _ <- seed(1, 300)
      _ <- project
      _ <- verify(300L)
      _ <- project
      _ <- verify(300L)
      _ <- seed(301, 301)
      _ <- project
      _ <- verify(301L)
    yield ()

  private def parkPendingLoadOutboxes(): IO[Unit] =
    sql"""UPDATE outbox_events
           SET status = 'failed',
               last_error = 'superseded by integration test isolation',
               updated_at = CURRENT_TIMESTAMP
           WHERE destination_topic = 'sync.oracle.load'
             AND status = 'pending'""".update.run.void.transact(xa)

  private def proxyLease: IO[com.sslproxy.coordinator.processor.Lease] =
    sql"DELETE FROM octopus_core.work_leases WHERE resource_type = 'processor' AND resource_id = 'proxy-search-preparer'".update.run
      .transact(xa) *>
      repository
        .claimMaintenanceLease(
          "processor",
          "proxy-search-preparer",
          "proxy-test",
          java.util.UUID.randomUUID().toString,
          60
        )
        .map(result => requireRight(result).get)

  test("older preparation generations cannot publish evidence, consume dirty work or advance checkpoints") {
    requireDocker()
    import com.sslproxy.coordinator.processor.*
    import com.sslproxy.coordinator.postgres.sql.ProxyPreparationSql
    val id = "55229101-31c3-43e2-9f3a-9e236b829bc1"
    val json = circeParser
      .parse(s"""{"event_id":"$id","type":"http_proxied","host":"generation.example","time":"2026-10-19T12:00:00Z"}""")
      .toOption
      .get
    for
      lease <- proxyLease
      revision <- sql"""INSERT INTO octopus_core.proxy_events(event_id,event_time,event_type,host,payload)
        VALUES ($id, '2026-10-19T12:00:00Z', 'http_proxied', 'generation.example', ${json.noSpaces}::jsonb)
        RETURNING observation_revision""".query[Long].unique.transact(xa)
      _ <-
        sql"INSERT INTO octopus_core.proxy_preparation_dirty(event_id,source_revision) VALUES ($id,$revision)".update.run
          .transact(xa)
      source = ProxyEvidence
        .source(json, id, revision, java.sql.Timestamp.from(java.time.Instant.parse("2026-10-19T12:00:00Z")))
        .get
      _ <- ProxyPreparationSql.persist(lease, source, "nomic-embed-text-v2-moe").transact(xa)
      _ <- sql"UPDATE atheros_search.proxy_document_state SET preparation_generation=3 WHERE source_key=$id".update.run
        .transact(xa)
      before <- sql"SELECT after_revision FROM octopus_core.proxy_preparation_progress WHERE scope='proxy-event-v2'"
        .query[Long]
        .option
        .transact(xa)
      outcome <- ProxyPreparationSql
        .tick(lease, 500, "nomic-embed-text-v2-moe", "legacy_unknown", 1000)
        .transact(xa)
        .attempt
      after <- sql"SELECT after_revision FROM octopus_core.proxy_preparation_progress WHERE scope='proxy-event-v2'"
        .query[Long]
        .option
        .transact(xa)
      dirty <- sql"SELECT count(*) FROM octopus_core.proxy_preparation_dirty WHERE event_id=$id"
        .query[Long]
        .unique
        .transact(xa)
      evidence <- sql"SELECT count(*) FROM atheros_search.proxy_event_evidence WHERE event_id=$id"
        .query[Long]
        .unique
        .transact(xa)
      _ <- sql"UPDATE atheros_search.proxy_document_state SET preparation_generation=2 WHERE source_key=$id".update.run
        .transact(xa)
    yield
      assert(outcome.isLeft)
      assertEquals(before, after)
      assertEquals(dirty, 1L)
      assertEquals(evidence, 0L)
  }

  test("proxy similarity excludes wireless on both sides and uses independent scope checkpoints") {
    requireDocker()
    import com.sslproxy.coordinator.postgres.sql.IntelligenceSql
    val left = "55229101-31c3-43e2-9f3a-9e236b829bb1"
    val right = "55229101-31c3-43e2-9f3a-9e236b829bb2"
    val wireless = "55229101-31c3-43e2-9f3a-9e236b829bb3"
    val revision = "nomic-prefix-v1:" + ("b" * 64)
    val vector = (List("1") ++ List.fill(767)("0")).mkString("[", ",", "]")
    for
      _ <- List(left -> "proxy_event", right -> "proxy_event", wireless -> "event").traverse_ { case (id, kind) =>
        sql"""INSERT INTO atheros_search.search_documents(document_id,source_id,source_key,source_table,source_kind,normalized_text,normalized_sha256,normalization_version)
          VALUES ($id::uuid,$id,$id,'proxy_events',$kind,'synthetic',encode(sha256(convert_to('synthetic','UTF8')),'hex'),'proxy-authority-v1')""".update.run
          .transact(xa)
      }
      _ <- List(left, right, wireless).traverse_ { id =>
        (sql"""INSERT INTO atheros_search.search_vectors_event(document_id,embedding_model,content_sha256,embedding,embedded_at)
          VALUES ($id::uuid,'scope-fixture',encode(sha256(convert_to('synthetic','UTF8')),'hex'),$vector::public.vector,clock_timestamp())""".update.run *>
          sql"""INSERT INTO atheros_search.embeddings(document_id,embedding_kind,embedding_model,content_sha256,embedding,embedded_at)
          VALUES ($id::uuid,'event','scope-fixture',encode(sha256(convert_to('synthetic','UTF8')),'hex'),$vector::public.vector,clock_timestamp())""".update.run)
          .transact(xa)
      }
      anchors <- IntelligenceSql.similarityAnchors(IntelligenceSql.VectorKind.Event, 100, "proxy").to[List].transact(xa)
      candidates <- IntelligenceSql
        .similarityCandidatesForAnchor(
          IntelligenceSql.VectorKind.Event,
          left,
          "scope-fixture",
          vector,
          0.1,
          100,
          "proxy"
        )
        .to[List]
        .transact(xa)
      wrongAnchor <- IntelligenceSql
        .similarityCandidatesForAnchor(
          IntelligenceSql.VectorKind.Event,
          wireless,
          "scope-fixture",
          vector,
          0.1,
          100,
          "proxy"
        )
        .to[List]
        .transact(xa)
      anchor = anchors.find(_._2 == left).get
      _ <- IntelligenceSql
        .beginSimilarityAnchor(IntelligenceSql.VectorKind.Event, anchor._1, left, "scope-fixture", anchor._5, "proxy")
        .transact(xa)
      _ <- IntelligenceSql
        .markSimilarityAnchor(IntelligenceSql.VectorKind.Event, anchor._1, anchor._5, "wireless")
        .transact(xa)
      stillOpen <-
        sql"SELECT completed_at IS NULL FROM atheros_search.similarity_scan_state WHERE scope = 'proxy' AND vector_id = ${anchor._1}"
          .query[Boolean]
          .unique
          .transact(xa)
      _ <- sql"UPDATE atheros_search.search_documents SET input_revision=$revision WHERE document_id IN ($left::uuid,$right::uuid)".update.run.transact(xa)
      _ <- sql"UPDATE atheros_search.embeddings SET input_revision=$revision WHERE document_id IN ($left::uuid,$right::uuid)".update.run.transact(xa)
      _ <- sql"UPDATE atheros_search.search_vectors_event SET input_revision=$revision WHERE document_id IN ($left::uuid,$right::uuid)".update.run.transact(xa)
      projected <- repository.projectSimilarities(100,0.1,0.9,0.1,revision).map(requireRight)
      proxyPairs <- sql"""SELECT count(*) FROM atheros_search.similarity_pairs WHERE scope='proxy'
        AND input_revision=$revision AND normalization_version='proxy-authority-v1' AND embedding_model='scope-fixture'"""
        .query[Long].unique.transact(xa)
      completed <- sql"""SELECT count(*) FROM atheros_search.similarity_scan_state WHERE scope='proxy'
        AND input_revision=$revision AND normalization_version='proxy-authority-v1' AND completed_at IS NOT NULL"""
        .query[Long].unique.transact(xa)
    yield
      assert(!anchors.exists(_._2 == wireless))
      assertEquals(candidates.map(_.rightDocumentId), List(right))
      assert(wrongAnchor.isEmpty)
      assert(stillOpen)
      assert(projected > 0)
      assertEquals(proxyPairs, 1L)
      assertEquals(completed, 2L)
  }

  test("proxy dirty windows preserve legacy source keys and reconcile late observations without text churn") {
    requireDocker()
    def insert(id: String, time: String): IO[Unit] =
      val json = circeParser
        .parse(s"""{"event_id":"$id","type":"block","time":"$time",
        "host":"WINDOW.Example:443","authority_raw":"WINDOW.Example:443","authority_source":"connect_authority",
        "evidence_version":"proxy-evidence-v1","blocked":true,"bytes_up":0,"bytes_down":0}""")
        .toOption
        .get
      val rows =
        PostgresTransformService.transform(com.sslproxy.coordinator.postgres.PostgresSinkTarget.ProxyEvents, List(json))
      schemaTransactor.insertProxyEvents("proxy-window-" + id, rows.proxyEvents, rows.blockedEvents).void
    for
      lease <- proxyLease
      key <- sql"""SELECT CONCAT('proxy-window:', md5(jsonb_build_array('WINDOW.Example:443'::text, ''::text,
        '2020-01-01T01:00:00Z'::timestamptz)::text))""".query[String].unique.transact(xa)
      _ <- insert("13229101-31c3-43e2-9f3a-9e236b829bb1", "2020-01-01T01:30:00Z")
      _ <- (0 until 3).toList.traverse_(_ =>
        repository.prepareProxySearch(lease, 500, "nomic-embed-text-v2-moe", "legacy_unknown", 1000).map(requireRight)
      )
      first <-
        sql"SELECT document_id::text, detail_json::text, host FROM atheros_search.search_documents WHERE source_key = $key AND status = 'active'"
          .query[(String, String, String)]
          .unique
          .transact(xa)
      _ <- insert("13229101-31c3-43e2-9f3a-9e236b829bb2", "2020-01-01T01:01:00Z")
      _ <- (0 until 3).toList.traverse_(_ =>
        repository.prepareProxySearch(lease, 500, "nomic-embed-text-v2-moe", "legacy_unknown", 1000).map(requireRight)
      )
      corrected <-
        sql"SELECT document_id::text, detail_json::text FROM atheros_search.search_documents WHERE source_key = $key AND status = 'active'"
          .query[(String, String)]
          .unique
          .transact(xa)
    yield
      assertEquals(first._1, corrected._1)
      assertEquals(first._3, "WINDOW.Example:443")
      assertEquals(circeParser.parse(first._2).toOption.get.hcursor.get[Int]("observed_blocked_events"), Right(1))
      assertEquals(circeParser.parse(corrected._2).toOption.get.hcursor.get[Int]("observed_blocked_events"), Right(2))
      assertEquals(
        circeParser.parse(corrected._2).toOption.get.hcursor.get[String]("attempts_status"),
        Right("unknown_coalesced")
      )
  }

  test("proxy windows isolate member coverage and limits and retry after member normalization") {
    requireDocker()
    val start = java.sql.Timestamp.from(java.time.Instant.parse("1990-01-01T01:00:00Z"))
    val end = java.sql.Timestamp.from(start.toInstant.plusSeconds(3600))
    val host = "SCOPED.Example:443"
    val device = "89ec1e80-9459-4d8e-a938-64adbb4c2061"
    val otherDevice = "89ec1e80-9459-4d8e-a938-64adbb4c2062"
    val version = com.sslproxy.coordinator.processor.ProxyAuthority.Version
    val evidence = s"""{"host_normalized":"scoped.example","normalization_version":"$version"}"""
    val payload = s"""{"type":"block","blocked":true,"host":"$host","authority_raw":"$host"}"""
    def tick(lease: com.sslproxy.coordinator.processor.Lease, highWater: Int = 1000): IO[Unit] =
      repository
        .prepareProxySearch(lease, 500, "nomic-embed-text-v2-moe", "legacy_unknown", highWater)
        .map(requireRight)
        .void
    val skipHistory = sql"""INSERT INTO octopus_core.proxy_preparation_progress(scope, after_revision)
      SELECT 'proxy-event-v2', max(observation_revision) FROM octopus_core.proxy_events WHERE true
      ON CONFLICT(scope) DO UPDATE SET after_revision = EXCLUDED.after_revision""".update.run.transact(xa)
    for
      lease <- proxyLease
      _ <- sql"""INSERT INTO octopus_core.proxy_events
        (event_id,event_time,event_type,host,registered_device_id,blocked,payload,evidence) VALUES
        ('scoped-window-ready', $start::timestamptz + interval '45 minutes', 'block', $host, $device::uuid, true, $payload::jsonb, $evidence::jsonb),
        ('scoped-window-late', $start::timestamptz + interval '46 minutes', 'block', $host, $device::uuid, true, $payload::jsonb, $evidence::jsonb)""".update.run
        .transact(xa)
      _ <- sql"""INSERT INTO octopus_core.proxy_events
        (event_id,event_time,event_type,host,registered_device_id,blocked,evidence)
        SELECT 'scoped-window-unrelated-' || n, $start::timestamptz + interval '10 minutes', 'block',
          CASE WHEN n <= 250 THEN $host ELSE 'unrelated.example' END,
          CASE WHEN n <= 250 THEN $otherDevice::uuid ELSE $device::uuid END, true,
          CASE WHEN n <= 250 THEN jsonb_build_object('host_normalized', 'different.example', 'normalization_version', $version::text)
            ELSE '{}'::jsonb END FROM generate_series(1,501) n""".update.run.transact(xa)
      _ <- skipHistory
      _ <- sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key)
        VALUES ($start,$host,$device)""".update.run.transact(xa)
      _ <- tick(lease)
      key <-
        sql"""SELECT CONCAT('proxy-window:', md5(jsonb_build_array($host::text,$device::text,$start::timestamptz)::text))"""
          .query[String].unique.transact(xa)
      original <- sql"SELECT document_id::text FROM atheros_search.search_documents WHERE source_key=$key AND status='active'"
        .query[String].unique.transact(xa)
      _ <- sql"""UPDATE atheros_search.embedding_jobs SET status='leased', owner_id='parked-worker',
        lease_token=gen_random_uuid(), lease_fence=1,
        lease_expires_at=clock_timestamp()+interval '5 minutes', attempt_count=1
        WHERE document_id=$original::uuid""".update.run.transact(xa)
      _ <- sql"UPDATE octopus_core.proxy_events SET evidence='{}'::jsonb WHERE event_id='scoped-window-late'".update.run.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key)
        VALUES ($start,$host,$device)""".update.run.transact(xa)
      _ <- tick(lease, highWater = 1)
      parked <- sql"""SELECT disposition, observed_rows_lower_bound FROM octopus_core.proxy_window_dirty
        WHERE window_start=$start AND host=$host AND device_key=$device""".query[(String, Int)].unique.transact(xa)
      parkedActive <- sql"SELECT count(*) FROM atheros_search.search_documents WHERE source_key=$key AND status='active'"
        .query[Long].unique.transact(xa)
      parkedJob <- sql"""SELECT status, owner_id IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL,
        attempt_count, lease_fence FROM atheros_search.embedding_jobs WHERE document_id=$original::uuid"""
        .query[(String, Boolean, Int, Long)].unique.transact(xa)
      watermark <- sql"""SELECT max(observation_revision) FROM (
        SELECT observation_revision FROM octopus_core.proxy_events WHERE event_time >= $start AND event_time < $end
        ORDER BY event_time, event_id LIMIT 501) bounded""".query[Long].unique.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.proxy_preparation_dirty(event_id,source_revision)
        SELECT event_id, observation_revision FROM octopus_core.proxy_events WHERE event_id='scoped-window-late'""".update.run
        .transact(xa)
      _ <- tick(lease)
      detail <- sql"""SELECT detail_json::text FROM atheros_search.search_documents
        WHERE source_key=$key AND status='active'""".query[String].unique.transact(xa)
      revivedJob <- sql"SELECT status, last_error FROM atheros_search.embedding_jobs WHERE document_id=$original::uuid"
        .query[(String, Option[String])].unique.transact(xa)
      remaining <- sql"""SELECT count(*) FROM octopus_core.proxy_window_dirty
        WHERE window_start=$start AND host=$host AND device_key=$device""".query[Long].unique.transact(xa)
      _ <- sql"""UPDATE octopus_core.proxy_events SET evidence=jsonb_build_object('host_normalized','other.example',
        'normalization_version',$version::text) WHERE event_id='scoped-window-late'""".update.run.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key)
        VALUES ($start,$host,$device)""".update.run.transact(xa)
      _ <- tick(lease)
      ambiguous <- sql"""SELECT disposition FROM octopus_core.proxy_window_dirty
        WHERE window_start=$start AND host=$host AND device_key=$device""".query[String].unique.transact(xa)
      ambiguousActive <- sql"SELECT count(*) FROM atheros_search.search_documents WHERE source_key=$key AND status='active'"
        .query[Long].unique.transact(xa)
      _ <- sql"UPDATE octopus_core.proxy_events SET evidence=$evidence::jsonb WHERE event_id='scoped-window-late'".update.run.transact(xa)
      _ <- sql"""UPDATE octopus_core.proxy_window_dirty SET disposition='pending', revision=nextval('octopus_core.proxy_window_revision_seq')
        WHERE window_start=$start AND host=$host AND device_key=$device""".update.run.transact(xa)
      _ <- tick(lease)
      _ <- sql"""INSERT INTO octopus_core.proxy_events
        (event_id,event_time,event_type,host,registered_device_id,blocked,evidence)
        SELECT 'scoped-window-overflow-' || n, $start::timestamptz + interval '50 minutes', 'block', $host, $device::uuid,
          true, $evidence::jsonb FROM generate_series(1,499) n""".update.run.transact(xa)
      _ <- skipHistory
      _ <- sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key)
        VALUES ($start,$host,$device)""".update.run.transact(xa)
      _ <- tick(lease)
      overflow <- sql"""SELECT disposition, observed_rows_lower_bound FROM octopus_core.proxy_window_dirty
        WHERE window_start=$start AND host=$host AND device_key=$device""".query[(String, Int)].unique.transact(xa)
      overflowActive <- sql"SELECT count(*) FROM atheros_search.search_documents WHERE source_key=$key AND status='active'"
        .query[Long].unique.transact(xa)
    yield
      assertEquals(parked, ("source_coverage_pending", 2))
      assertEquals(parkedActive, 0L)
      assertEquals(parkedJob, ("cancelled", true, 1, 1L))
      assertEquals(revivedJob, ("pending", None))
      assertEquals(ambiguous, "ambiguous_normalization")
      assertEquals(ambiguousActive, 0L)
      val cursor = circeParser.parse(detail).toOption.get.hcursor
      assertEquals(cursor.get[Int]("observed_blocked_events"), Right(2))
      assertEquals(cursor.get[String]("host_normalized"), Right("scoped.example"))
      assertEquals(cursor.get[Long]("source_watermark"), Right(watermark))
      assertEquals(remaining, 0L)
      assertEquals(overflow, ("input_limit", 501))
      assertEquals(overflowActive, 0L)
  }

  test("proxy windows exclude unsafe legacy hosts from every Search publication field") {
    requireDocker()
    val version = com.sslproxy.coordinator.processor.ProxyAuthority.Version
    val cases = List(
      ("user:password@safe.example:443", false, None),
      ("safe.example/path?token=secret", false, None),
      ("hmac:synthetic?token=secret", true, None),
      ("safe.example\tcredential", false, None),
      ("private.example", true, None),
      ("SAFE.Example:443", false, Some("SAFE.Example:443"))
    )
    for
      lease <- proxyLease
      _ <- cases.zipWithIndex.traverse_ { case ((rawHost, sanitized, expected), index) =>
        val start = java.sql.Timestamp.from(java.time.Instant.parse("1980-01-01T00:00:00Z").plusSeconds(index.toLong * 3600))
        val evidence = io.circe.Json.obj("host_normalized" -> io.circe.Json.fromString("safe.example"),
          "normalization_version" -> io.circe.Json.fromString(version),
          "sanitization_status" -> io.circe.Json.fromString(if sanitized then "sanitized" else "not_sanitized")).noSpaces
        for
          _ <- sql"""INSERT INTO octopus_core.proxy_events(event_id,event_time,event_type,host,blocked,evidence)
            VALUES (${s"unsafe-window-$index"},$start,'block',$rawHost,true,$evidence::jsonb)""".update.run.transact(xa)
          _ <- sql"""INSERT INTO octopus_core.proxy_preparation_progress(scope,after_revision)
            SELECT 'proxy-event-v2',max(observation_revision) FROM octopus_core.proxy_events WHERE true
            ON CONFLICT(scope) DO UPDATE SET after_revision=EXCLUDED.after_revision""".update.run.transact(xa)
          _ <- sql"INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key) VALUES ($start,$rawHost,'')".update.run.transact(xa)
          _ <- repository.prepareProxySearch(lease,500,"nomic-embed-text-v2-moe","legacy_unknown",1000).map(requireRight)
          published <- sql"""SELECT host,detail_json::text,title,normalized_text,tags::text FROM atheros_search.search_documents
            WHERE source_kind='proxy_blocked_host_window' AND window_start=$start AND status='active'"""
            .query[(Option[String],String,Option[String],String,String)].unique.transact(xa)
        yield
          assertEquals(published._1, expected)
          assertEquals(circeParser.parse(published._2).toOption.get.hcursor.get[Option[String]]("host"), Right(expected))
          if expected.isEmpty then
            List(published._2,published._3.getOrElse(""),published._4,published._5).foreach(value => assert(!value.contains(rawHost)))
      }
    yield ()
  }

  test("proxy job reconciliation repairs missing coverage and retains terminal failure history") {
    requireDocker()
    import com.sslproxy.coordinator.processor.*
    import com.sslproxy.coordinator.postgres.sql.ProxyPreparationSql
    val source = ProxyEvidence
      .source(
        circeParser.parse("""{"type":"http_proxied","host":"reconcile.example"}""").toOption.get,
        "proxy-reconcile-fixture",
        1L,
        java.sql.Timestamp.from(java.time.Instant.now())
      )
      .get
    val document = SearchDocumentPreparation.prepare(source).toOption.get
    for
      lease <- proxyLease
      _ <- ProxyPreparationSql.persist(lease, source, "nomic-embed-text-v2-moe").transact(xa)
      _ <- sql"""UPDATE atheros_search.embedding_jobs SET status = 'completed', attempt_count = 1,
        completed_at = clock_timestamp(), last_error = 'retained historical diagnostic'
        WHERE document_id = ${document.documentId}::uuid""".update.run.transact(xa)
      _ <- (0 until 3).toList.traverse_(_ =>
        repository
          .prepareProxySearch(lease, 500, "nomic-embed-text-v2-moe", "legacy_unknown", 1000)
          .map(requireRight)
      )
      repaired <-
        sql"SELECT status, attempt_count, last_error FROM atheros_search.embedding_jobs WHERE document_id = ${document.documentId}::uuid"
          .query[(String, Int, Option[String])]
          .unique
          .transact(xa)
      receipts <-
        sql"""SELECT count(*) FROM atheros_search.embedding_repair_receipts receipt JOIN atheros_search.embedding_jobs job USING(job_id)
        WHERE job.document_id = ${document.documentId}::uuid""".query[Long].unique.transact(xa)
      _ <-
        sql"UPDATE atheros_search.embedding_jobs SET status = 'failed' WHERE document_id = ${document.documentId}::uuid".update.run
          .transact(xa)
      _ <- repository
        .prepareProxySearch(lease, 500, "nomic-embed-text-v2-moe", "legacy_unknown", 1000)
        .map(requireRight)
      _ <- repository
        .prepareProxySearch(lease, 500, "nomic-embed-text-v2-moe", "legacy_unknown", 1000)
        .map(requireRight)
      terminal <- sql"SELECT status FROM atheros_search.embedding_jobs WHERE document_id = ${document.documentId}::uuid"
        .query[String]
        .unique
        .transact(xa)
    yield
      assertEquals(repaired, ("pending", 1, Some("retained historical diagnostic")))
      assertEquals(receipts, 1L)
      assertEquals(terminal, "failed")
  }

  test("proxy text rollbacks resume superseded unfinished jobs and preserve terminal history") {
    requireDocker()
    import com.sslproxy.coordinator.processor.*
    import com.sslproxy.coordinator.postgres.sql.ProxyPreparationSql
    val model = "nomic-embed-text-v2-moe"
    val source = ProxyEvidence.source(
      circeParser.parse("""{"type":"http_proxied","host":"text-rollback.example"}""").toOption.get,
      "proxy-text-rollback", 1L, java.sql.Timestamp.from(java.time.Instant.now())
    ).get
    val fixtures = List("pending", "leased", "completed", "failed", "cancelled").map { status =>
      status -> source.copy(sourceKey = s"${source.sourceKey}-$status")
    }
    for
      lease <- proxyLease
      _ <- fixtures.traverse_ { case (status, original) =>
        val document = SearchDocumentPreparation.prepare(original).toOption.get
        for
          _ <- ProxyPreparationSql.persist(lease, original, model).transact(xa)
          _ <- sql"""UPDATE atheros_search.embedding_jobs SET status=$status, attempt_count=1,
            owner_id=CASE WHEN $status='leased' THEN 'rollback-worker' ELSE NULL END,
            lease_token=CASE WHEN $status='leased' THEN gen_random_uuid() ELSE NULL END,
            lease_expires_at=CASE WHEN $status='leased' THEN clock_timestamp()+interval '5 minutes' ELSE NULL END,
            last_error='retained diagnostic'
            WHERE document_id=${document.documentId}::uuid""".update.run.transact(xa)
          _ <- ProxyPreparationSql.persist(lease,
            original.copy(evidenceRevision = Some(2L), searchText = "replacement proxy text"), model).transact(xa)
          _ <- ProxyPreparationSql.persist(lease, original.copy(evidenceRevision = Some(3L)), model).transact(xa)
          restored <- sql"""SELECT doc.status, job.status, job.attempt_count,
            job.owner_id IS NULL AND job.lease_token IS NULL AND job.lease_expires_at IS NULL, job.last_error
            FROM atheros_search.embedding_jobs job JOIN atheros_search.search_documents doc USING(document_id)
            WHERE doc.document_id=${document.documentId}::uuid"""
            .query[(String, String, Int, Boolean, Option[String])].unique.transact(xa)
          jobs <- sql"""SELECT count(*) FROM atheros_search.embedding_jobs job
            JOIN atheros_search.search_documents doc USING(document_id) WHERE doc.source_key=${original.sourceKey}"""
            .query[Long].unique.transact(xa)
        yield
          val resumed = status == "pending" || status == "leased"
          assertEquals(restored, ("active", if resumed then "pending" else status, 1, true,
            if resumed then None else Some("retained diagnostic")))
          assertEquals(jobs, 2L)
      }
    yield ()
  }

  test("proxy preparation preserves replay identity, private evidence boundaries and newer corrections") {
    requireDocker()
    import com.sslproxy.coordinator.processor.*
    import com.sslproxy.coordinator.postgres.sql.ProxyPreparationSql
    val id = "812d9705-2c5a-421e-a977-fb35250101ec"
    val observed = java.sql.Timestamp.from(java.time.Instant.parse("2026-10-19T12:30:00Z"))
    val payload = circeParser
      .parse(s"""{"event_id":"$id","time":"2026-10-19T12:30:00Z",
      "type":"http_proxied","host":"ChatGPT.com","authority_raw":"ChatGPT.com:8443","authority_source":"http_authority",
      "evidence_version":"proxy-evidence-v1","classification":"essential_api","bytes_up":0,"bytes_down":0,
      "peer_ip":"192.0.2.99","wg_pubkey":"private-key","device_id":"private-device"}""")
      .toOption
      .get
    val rows = PostgresTransformService.transform(
      com.sslproxy.coordinator.postgres.PostgresSinkTarget.ProxyEvents,
      List(payload)
    )
    for
      lease <- proxyLease
      _ <- schemaTransactor.insertProxyEvents("proxy-evidence-fixture", rows.proxyEvents, rows.blockedEvents)
      revision <- sql"SELECT observation_revision FROM octopus_core.proxy_events WHERE event_id = $id"
        .query[Long]
        .unique
        .transact(xa)
      _ <- schemaTransactor.insertProxyEvents("proxy-evidence-fixture", rows.proxyEvents, rows.blockedEvents)
      replayRevision <- sql"SELECT observation_revision FROM octopus_core.proxy_events WHERE event_id = $id"
        .query[Long]
        .unique
        .transact(xa)
      _ <- repository
        .prepareProxySearch(lease, 100, "nomic-embed-text-v2-moe", "legacy_unknown", 1000)
        .map(requireRight)
      first <- sql"""SELECT document_id::text, normalized_text, detail_json::text FROM atheros_search.search_documents
        WHERE source_key = $id AND status = 'active'""".query[(String, String, String)].unique.transact(xa)
      published <- sql"SELECT evidence::text FROM atheros_search.proxy_event_evidence WHERE event_id = $id"
        .query[String]
        .unique
        .transact(xa)
      source = ProxyEvidence
        .source(payload, id, revision + 1000, observed)
        .get
        .copy(detailJson = "{\"correction\":true}")
      _ <- ProxyPreparationSql.persist(lease, source, "nomic-embed-text-v2-moe").transact(xa)
      _ <- ProxyPreparationSql
        .persist(
          lease,
          source.copy(evidenceRevision = Some(revision), searchText = "stale old text"),
          "nomic-embed-text-v2-moe"
        )
        .transact(xa)
      corrected <-
        sql"""SELECT document_id::text, detail_json::text, evidence_revision FROM atheros_search.search_documents
        WHERE source_key = $id AND status = 'active'""".query[(String, String, Long)].unique.transact(xa)
      jobs <- sql"SELECT count(*) FROM atheros_search.embedding_jobs WHERE document_id = ${first._1}::uuid"
        .query[Long]
        .unique
        .transact(xa)
      _ <-
        sql"UPDATE octopus_core.work_leases SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE resource_id = 'proxy-search-preparer'".update.run
          .transact(xa)
      denied <- ProxyPreparationSql
        .persist(
          lease,
          source.copy(evidenceRevision = Some(revision + 2000), searchText = "expired owner text"),
          "nomic-embed-text-v2-moe"
        )
        .transact(xa)
        .attempt
      current <-
        sql"SELECT document_id::text FROM atheros_search.search_documents WHERE source_key = $id AND status = 'active'"
          .query[String]
          .unique
          .transact(xa)
    yield
      assertEquals(revision, replayRevision)
      assertEquals(first._1, corrected._1)
      assertEquals(corrected._3, revision + 1000)
      assert(corrected._2.contains("correction"))
      assertEquals(jobs, 1L)
      List("192.0.2.99", "private-key", "private-device").foreach(value =>
        assert(!first._2.contains(value) && !published.contains(value))
      )
      assert(published.contains("chatgpt.com:8443"))
      assert(denied.isLeft)
      assertEquals(current, first._1)
  }

  test("proxy source state, jobs and checkpoints roll back when ownership expires inside a transaction") {
    requireDocker()
    import com.sslproxy.coordinator.processor.*
    import com.sslproxy.coordinator.postgres.sql.ProxyPreparationSql
    val source = ProxyEvidence
      .source(
        circeParser.parse("""{"type":"http_proxied","host":"rollback.example"}""").toOption.get,
        "proxy-rollback-fixture",
        1L,
        java.sql.Timestamp.from(java.time.Instant.now())
      )
      .get
    for
      lease <- proxyLease
      _ <-
        sql"UPDATE octopus_core.work_leases SET lease_expires_at = clock_timestamp() + interval '100 milliseconds' WHERE resource_id = 'proxy-search-preparer'".update.run
          .transact(xa)
      outcome <- (for
        _ <- ProxyPreparationSql.persist(lease, source, "nomic-embed-text-v2-moe")
        _ <- sql"SELECT pg_sleep(0.15)".query[Unit].unique
        _ <- ProxyPreparationSql.guard(lease)
      yield ()).transact(xa).attempt
      count <- sql"SELECT count(*) FROM atheros_search.proxy_document_state WHERE source_key = 'proxy-rollback-fixture'"
        .query[Long]
        .unique
        .transact(xa)
    yield
      assert(outcome.isLeft)
      assertEquals(count, 0L)
  }

  test("proxy preparation runs with no wireless privileges and honors scoped queue headroom") {
    requireDocker()
    import com.sslproxy.coordinator.postgres.sql.ProxyPreparationSql
    for
      lease <- proxyLease
      _ <- sql"CREATE ROLE proxy_preparation_contract".update.run.transact(xa)
      _ <- sql"GRANT USAGE ON SCHEMA octopus_core, atheros_search, public TO proxy_preparation_contract".update.run
        .transact(xa)
      _ <-
        sql"""GRANT SELECT, INSERT, UPDATE, DELETE ON octopus_core.proxy_events, octopus_core.proxy_preparation_dirty,
        octopus_core.proxy_preparation_progress, octopus_core.proxy_window_dirty, octopus_core.work_leases,
        atheros_search.proxy_event_evidence, atheros_search.proxy_document_state, atheros_search.search_documents,
        atheros_search.search_document_tokens, atheros_search.search_document_tags, atheros_search.embedding_jobs,
        atheros_search.embedding_repair_receipts, atheros_search.proxy_preparation_policy TO proxy_preparation_contract""".update.run
          .transact(xa)
      _ <-
        sql"GRANT SELECT ON atheros_search.embeddings, atheros_search.search_vectors_event TO proxy_preparation_contract".update.run
          .transact(xa)
      _ <-
        sql"GRANT USAGE, SELECT ON SEQUENCE octopus_core.proxy_window_revision_seq TO proxy_preparation_contract".update.run
          .transact(xa)
      result <- (sql"SET LOCAL ROLE proxy_preparation_contract".update.run *>
        ProxyPreparationSql.tick(lease, 10, "nomic-embed-text-v2-moe", "legacy_unknown", 1000)).transact(xa)
      limited <- (sql"SET LOCAL ROLE proxy_preparation_contract".update.run *>
        ProxyPreparationSql.tick(lease, 10, "nomic-embed-text-v2-moe", "legacy_unknown", 1)).transact(xa)
    yield
      assert(result <= 10)
      assertEquals(limited, 0)
  }

  test("full proxy queues validate beyond valid pending windows and revisit them without publishing") {
    requireDocker()
    import com.sslproxy.coordinator.processor.*
    val start = java.sql.Timestamp.from(java.time.Instant.parse("1991-01-01T01:00:00Z"))
    val later = java.sql.Timestamp.from(start.toInstant.plusSeconds(3600))
    val version = ProxyAuthority.Version
    val evidence = s"""{"host_normalized":"validation.example","normalization_version":"$version"}"""
    def tick(lease: Lease, limit: Int, highWater: Int): IO[Int] =
      repository.prepareProxySearch(lease, limit, "nomic-embed-text-v2-moe", "legacy_unknown", highWater).map(requireRight)
    for
      // Isolate the bounded validation order from earlier suite fixtures.
      _ <- sql"""TRUNCATE atheros_search.search_documents, atheros_search.embedding_jobs,
        atheros_search.proxy_document_state, atheros_search.embeddings, atheros_search.search_vectors_event,
        octopus_core.proxy_events, octopus_core.proxy_preparation_dirty, octopus_core.proxy_window_dirty,
        octopus_core.proxy_preparation_progress CASCADE""".update.run.transact(xa)
      lease <- proxyLease
      _ <- sql"""INSERT INTO octopus_core.proxy_events(event_id,event_time,event_type,host,blocked,evidence)
        VALUES ('validation-ready',$start,'block','validation.example',true,$evidence::jsonb),
          ('validation-later',$later,'block','validation.example',true,$evidence::jsonb)""".update.run.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.proxy_preparation_progress(scope,after_revision)
        SELECT 'proxy-event-v2',max(observation_revision) FROM octopus_core.proxy_events""".update.run.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key)
        VALUES ($start,'validation.example',''),($later,'validation.example','')""".update.run.transact(xa)
      _ <- tick(lease, 10, 1000)
      _ <- tick(lease, 10, 1000)
      initialJobs <- sql"SELECT count(*) FROM atheros_search.embedding_jobs".query[Long].unique.transact(xa)
      _ <- sql"UPDATE octopus_core.proxy_events SET evidence='{}'::jsonb WHERE event_id='validation-later'".update.run.transact(xa)
      _ <- sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key)
        VALUES ($start,'validation.example',''),($later,'validation.example','')""".update.run.transact(xa)
      first <- tick(lease, 1, 1)
      second <- tick(lease, 1, 1)
      dispositions <- sql"SELECT disposition FROM octopus_core.proxy_window_dirty ORDER BY window_start"
        .query[String].to[List].transact(xa)
      active <- sql"SELECT window_start FROM atheros_search.search_documents WHERE status='active'"
        .query[java.sql.Timestamp].to[List].transact(xa)
      jobsAfterValidation <- sql"SELECT count(*) FROM atheros_search.embedding_jobs".query[Long].unique.transact(xa)
      _ <- sql"UPDATE octopus_core.proxy_events SET evidence='{}'::jsonb WHERE event_id='validation-ready'".update.run.transact(xa)
      _ <- (0 until 2).toList.traverse_(_ => tick(lease, 1, 1))
      remaining <- sql"SELECT count(*) FROM atheros_search.search_documents WHERE status='active'"
        .query[Long].unique.transact(xa)
      finalJobs <- sql"SELECT count(*) FROM atheros_search.embedding_jobs".query[Long].unique.transact(xa)
    yield
      assertEquals(initialJobs, 2L)
      assertEquals(first, 1)
      assertEquals(second, 1)
      assertEquals(dispositions, List("pending", "source_coverage_pending"))
      assertEquals(active, List(start))
      assertEquals(jobsAfterValidation, initialJobs)
      assertEquals(remaining, 0L)
      assertEquals(finalJobs, initialJobs)
  }

  test("proxy input revision migration is bounded, drains a full old queue and preserves history") {
    requireDocker()
    import com.sslproxy.coordinator.processor.*
    import com.sslproxy.coordinator.postgres.sql.{ProxyPreparationSql, SearchPreparationSql}
    val model = "nomic-embed-text-v2-moe"
    val unsafeHost = "migration.example/path?token=secret"
    val revision = "nomic-prefix-v1:" + ("c" * 64)
    val source = ProxyEvidence.source(circeParser.parse("""{"type":"http_proxied","host":"migration.example"}""").toOption.get,
      "migration-pending", 1L, java.sql.Timestamp.from(java.time.Instant.now())).get
    val sources = List(source, source.copy(sourceKey="migration-completed", kind=SearchDocumentKind.ProxyBlockedHostWindow),
      source.copy(sourceKey="migration-failed"))
    val wireless = SearchDocumentPreparation.prepare(source.copy(sourceKey="migration-wireless",kind=SearchDocumentKind.Event)).toOption.get
    val vector = (List("1") ++ List.fill(767)("0")).mkString("[",",","]")
    def tick(lease: Lease, input: String, limit: Int = 1): IO[Int] =
      repository.prepareProxySearch(lease,limit,model,input,1).map(requireRight)
    for
      // This suite owns an ephemeral PostgreSQL container. Start with no source
      // notifications, proving migration does not depend on replaying history.
      _ <- sql"""TRUNCATE atheros_search.search_documents, atheros_search.embedding_jobs,
        atheros_search.proxy_document_state, atheros_search.embeddings, atheros_search.search_vectors_event,
        octopus_core.proxy_events, octopus_core.proxy_preparation_dirty, octopus_core.proxy_window_dirty,
        octopus_core.proxy_preparation_progress CASCADE""".update.run.transact(xa)
      lease <- proxyLease
      _ <- sources.traverse_(s => ProxyPreparationSql.persist(lease,s,model).transact(xa))
      _ <- SearchPreparationSql.persist(wireless).transact(xa)
      _ <- sql"""UPDATE atheros_search.search_documents SET host=$unsafeHost,title=$unsafeHost,
        detail_json=jsonb_set(detail_json,'{host}',to_jsonb($unsafeHost::text)),
        tags=jsonb_build_array(jsonb_build_object('type','host','value',$unsafeHost::text))
        WHERE source_key='migration-pending'""".update.run.transact(xa)
      _ <- sql"""INSERT INTO atheros_search.search_document_tags(document_id,tag_type,tag_value)
        SELECT document_id,'host',$unsafeHost FROM atheros_search.search_documents WHERE source_key='migration-pending'""".update.run.transact(xa)
      legacyJobId <- sql"""UPDATE atheros_search.embedding_jobs job SET job_id=gen_random_uuid(), status='leased', owner_id='old-worker',
        lease_token=gen_random_uuid(), lease_fence=1, attempt_count=1,
        lease_expires_at=clock_timestamp()+interval '5 minutes'
        FROM atheros_search.search_documents doc WHERE doc.document_id=job.document_id AND doc.source_key='migration-pending'
        RETURNING job.job_id::text""".query[String].unique.transact(xa)
      _ <- sql"""UPDATE atheros_search.embedding_jobs job SET status='completed', completed_at=clock_timestamp(), attempt_count=1
        FROM atheros_search.search_documents doc WHERE doc.document_id=job.document_id AND doc.source_key='migration-completed'""".update.run.transact(xa)
      _ <- sql"""UPDATE atheros_search.embedding_jobs job SET status='failed', attempt_count=max_attempts, last_error='retained terminal failure'
        FROM atheros_search.search_documents doc WHERE doc.document_id=job.document_id AND doc.source_key='migration-failed'""".update.run.transact(xa)
      _ <- sql"""INSERT INTO atheros_search.embeddings(document_id,embedding_kind,embedding_model,content_sha256,embedding,embedded_at)
        SELECT document_id,'event',$model,normalized_sha256,$vector::public.vector,clock_timestamp()
        FROM atheros_search.search_documents WHERE source_key='migration-completed'""".update.run.transact(xa)
      _ <- sql"UPDATE atheros_search.proxy_document_state SET preparation_generation=3 WHERE source_key='migration-pending'".update.run.transact(xa)
      denied <- tick(lease,revision).attempt
      _ <- sql"UPDATE atheros_search.proxy_document_state SET preparation_generation=2 WHERE source_key='migration-pending'".update.run.transact(xa)
      first <- tick(lease,revision)
      unmigrated <- sql"SELECT count(*) FROM atheros_search.search_documents WHERE input_revision=$revision".query[Long].unique.transact(xa)
      _ <- (0 until 3).toList.traverse_ { _ =>
        for
          _ <- sql"""UPDATE atheros_search.embedding_jobs SET status='completed', completed_at=clock_timestamp()
            WHERE input_revision=$revision AND status='pending'""".update.run.transact(xa)
          count <- tick(lease,revision)
          pending <- sql"SELECT count(*) FROM atheros_search.embedding_jobs WHERE input_revision=$revision AND status='pending'".query[Long].unique.transact(xa)
        yield
          assertEquals(count,1)
          assert(pending <= 1L)
      }
      migrated <- sql"SELECT count(*) FROM atheros_search.search_documents WHERE input_revision=$revision".query[Long].unique.transact(xa)
      wirelessRevision <- sql"SELECT input_revision FROM atheros_search.search_documents WHERE source_kind='event'".query[String].unique.transact(xa)
      oldHistory <- sql"""SELECT doc.source_key,job.status,job.attempt_count,job.last_error FROM atheros_search.embedding_jobs job
        JOIN atheros_search.search_documents doc USING(document_id) WHERE job.input_revision='legacy_unknown' ORDER BY doc.source_key"""
        .query[(String,String,Int,Option[String])].to[List].transact(xa)
      vectors <- sql"SELECT count(*) FROM atheros_search.embeddings WHERE input_revision='legacy_unknown'".query[Long].unique.transact(xa)
      sanitized <- sql"""SELECT host,detail_json::text,title,tags::text FROM atheros_search.search_documents
        WHERE source_key='migration-pending'""".query[(Option[String],String,Option[String],String)].unique.transact(xa)
      leakedTags <- sql"SELECT count(*) FROM atheros_search.search_document_tags WHERE tag_value=$unsafeHost".query[Long].unique.transact(xa)
      _ <- sql"""UPDATE atheros_search.embedding_jobs SET status='completed',completed_at=clock_timestamp()
        WHERE input_revision=$revision AND status='pending'""".update.run.transact(xa)
      _ <- (0 until 3).toList.traverse_(_ => tick(lease,"legacy_unknown"))
      restored <- sql"""SELECT job.status,job.attempt_count,job.lease_token IS NULL,job.last_error
        FROM atheros_search.embedding_jobs job JOIN atheros_search.search_documents doc USING(document_id)
        WHERE doc.source_key='migration-pending' AND job.input_revision='legacy_unknown'"""
        .query[(String,Int,Boolean,Option[String])].unique.transact(xa)
      restoredJobId <- sql"""SELECT job.job_id::text FROM atheros_search.embedding_jobs job
        JOIN atheros_search.search_documents doc USING(document_id)
        WHERE doc.source_key='migration-pending' AND job.input_revision='legacy_unknown'"""
        .query[String].unique.transact(xa)
    yield
      assert(denied.isLeft)
      assertEquals(first,1)
      assertEquals(unmigrated,0L)
      assertEquals(migrated,3L)
      assertEquals(wirelessRevision,"legacy_unknown")
      assertEquals(oldHistory.find(_._1=="migration-completed").map(_._2),Some("completed"))
      assertEquals(oldHistory.find(_._1=="migration-failed").map(row => (row._2,row._4)),Some(("failed",Some("retained terminal failure"))))
      assertEquals(oldHistory.find(_._1=="migration-pending").map(_._2),Some("cancelled"))
      assertEquals(vectors,1L)
      assertEquals(sanitized._1,None)
      assert(!List(sanitized._2,sanitized._3.getOrElse(""),sanitized._4).exists(_.contains("secret")))
      assertEquals(leakedTags,0L)
      assertEquals(restored,("pending",1,true,None))
      assertEquals(restoredJobId,legacyJobId)
  }

  private def requireRight[A](result: Either[DatabaseError, A]): A =
    result.fold(
      error => fail(s"${error.operation}: ${error.message}"),
      identity
    )

  private def requireDocker(): Unit =
    if dockerRequired && !dockerAvailable then
      throw IllegalStateException(
        "OCTOPUS_REQUIRE_DOCKER=true but Docker is unavailable"
      )
    else
      assume(
        dockerAvailable,
        "Docker is required for the PostgreSQL integration suite"
      )

  private def applyCanonicalManifest(): Unit =
    val manifest = canonicalManifest()
    val searchRoot = schemaRoot.resolveSibling("atheros_search")
    val parsedStatements = canonicalStatements(manifest) ++
      canonicalStatements(canonicalManifest(searchRoot), searchRoot)

    val connection =
      DriverManager.getConnection(jdbcUrl, "postgres", "postgres")
    try
      val statement = connection.createStatement()
      try
        val extensionsFile =
          schemaRoot.resolveSibling("00_extensions/001_runtime_extensions.sql")
        if Files.exists(extensionsFile) then
          val extensionsSql = new String(
            Files.readAllBytes(extensionsFile),
            StandardCharsets.UTF_8
          )
          extensionsSql
            .split(";")
            .map(_.trim)
            .filter(_.nonEmpty)
            .foreach(statement.execute)

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
      case _ =>
        throw IllegalStateException(s"unsupported canonical manifest key $key")

  private final case class CanonicalManifest(
    schemaVersion: String,
    manifestSha256: String,
    applyOrder: List[String]
  )

  private def canonicalManifest(root: Path = schemaRoot): CanonicalManifest =
    val input = Files.newInputStream(root.resolve("manifest.yaml"))
    val document =
      try Option(new Yaml().load[java.util.Map[String, Object]](input))
      finally input.close()
    val values = document.getOrElse(
      throw IllegalStateException("canonical manifest is empty")
    )

    def requiredScalar(key: String): String =
      Option(values.get(key))
        .map(_.toString.trim)
        .filter(_.nonEmpty)
        .getOrElse(
          throw IllegalStateException(
            s"missing or empty $key in canonical manifest"
          )
        )

    val applyOrder = Option(values.get("apply_order"))
      .map(_.asInstanceOf[java.util.List[Object]].asScala.toList)
      .getOrElse(Nil)
      .map(_.toString.trim)
      .filter(_.nonEmpty)
    if applyOrder.isEmpty then
      throw IllegalStateException(
        "canonical manifest apply_order must not be missing or empty"
      )

    CanonicalManifest(
      requiredScalar("schema_version"),
      requiredScalar("manifest_sha256"),
      applyOrder
    )

  private def canonicalStatements(): List[(String, List[String])] =
    canonicalStatements(canonicalManifest())

  private def canonicalStatements(
    manifest: CanonicalManifest,
    root: Path = schemaRoot
  ): List[(String, List[String])] =
    manifest.applyOrder.map { relative =>
      val source = Files.readString(root.resolve(relative))
      val statements = splitSqlStatements(source, relative)
      if statements.isEmpty then
        throw IllegalStateException(
          s"canonical schema file $relative contains no complete SQL statements"
        )
      relative -> statements
    }

  private def splitSqlStatements(
    source: String,
    relative: String
  ): List[String] =
    val statements = List.newBuilder[String]
    val current = new StringBuilder
    var index = 0
    var quote: Char = 0.toChar
    var lineComment = false
    var blockComment = false
    var dollarTag: Option[String] = None

    while index < source.length do
      val char = source.charAt(index)
      val next =
        if index + 1 < source.length then source.charAt(index + 1) else 0.toChar

      if lineComment then
        if char == '\n' || char == '\r' then
          lineComment = false
          current.append(' ')
      else if blockComment then
        if char == '*' && next == '/' then
          blockComment = false
          index += 1
      else if dollarTag.exists(source.startsWith(_, index)) then
        val tag = dollarTag.get
        current.append(tag)
        dollarTag = None
        index += tag.length - 1
      else if dollarTag.nonEmpty then current.append(char)
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
      else if char == '$' then
        parseDollarTag(source, index) match
          case Some(tag) =>
            current.append(tag)
            dollarTag = Some(tag)
            index += tag.length - 1
          case None => current.append(char)
      else if char == ';' then
        val statement = current.result().trim
        if statement.nonEmpty then statements += statement
        current.clear()
      else current.append(char)

      index += 1

    if quote != 0.toChar then
      throw IllegalStateException(
        s"unterminated quoted literal in canonical schema file $relative"
      )
    if dollarTag.nonEmpty then
      throw IllegalStateException(
        s"unterminated dollar-quoted block in canonical schema file $relative"
      )
    if blockComment then
      throw IllegalStateException(
        s"unterminated block comment in canonical schema file $relative"
      )
    if current.result().trim.nonEmpty then
      throw IllegalStateException(
        s"incomplete SQL statement without semicolon in canonical schema file $relative"
      )

    statements.result()

  private def parseDollarTag(source: String, start: Int): Option[String] =
    var index = start + 1
    while index < source.length do
      val char = source.charAt(index)
      if char == '$' then return Some(source.substring(start, index + 1))
      if !char.isLetterOrDigit && char != '_' then return None
      index += 1
    None

  private def schemaRoot: Path =
    Iterator
      .iterate(Path.of("").toAbsolutePath)(_.getParent)
      .takeWhile(_ != null)
      .map(_.resolve("sql/postgres/octopus_core"))
      .find(path => Files.isDirectory(path))
      .getOrElse(
        throw IllegalStateException(
          "cannot locate canonical octopus_core schema"
        )
      )
