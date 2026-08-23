package com.sslproxy.coordinator.postgres

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.util.Sha256Utils
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.Transactor
import doobie.implicits.*
import munit.CatsEffectSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.sql.{DriverManager, Timestamp}
import java.util.Base64
import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

/** Investigative repro for the production error:
  *   com.postgresql.cj.jdbc.exceptions.PostgresDataTruncation:
  *   Data truncation: Truncated incorrect INTEGER value: 'null'
  *   (operation = postgres.hydrate_existing_sync_event)
  *
  * Proves:
  *  1. PostgreSQL `->>` on a JSON null field yields the string 'null', and
  *     CAST('null' AS SIGNED) raises error 1292 (the exact prod error).
  *  2. The pre-15f6043 projection SQL (raw CAST of `->>` results) fails
  *     with that exact error for a JSON-null integer field.
  *  3. The current guarded projection survives a fuzz battery of
  *     null-like / malformed payload shapes.
  */
class HydrationNullTruncationReproSuite extends CatsEffectSuite:
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
    config.setConnectionInitSql("SET TIME ZONE 'UTC'; SET search_path TO octopus_core, atheros_search")
    config.addDataSourceProperty("stringtype", "unspecified")
    config.setMaximumPoolSize(4)
    new HikariDataSource(config)

  private lazy val doobieExecutor = Executors.newFixedThreadPool(2)

  private lazy val xa: Transactor[IO] =
    Transactor.fromDataSource[IO](
      dataSource,
      ExecutionContext.fromExecutorService(doobieExecutor)
    )

  private lazy val repository = new PostgresRepository(xa)
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

  test("PostgreSQL semantics: ->> on JSON null yields string 'null'; CAST truncation errors only in UPDATE context"):
    requireDocker()
    for
      unquoted <-
        sql"""SELECT JSON_UNQUOTE(JSON_EXTRACT('{"signal_dbm": null}', '$$.signal_dbm'))"""
          .query[Option[String]].unique.transact(xa)
      missingPath <-
        sql"""SELECT JSON_UNQUOTE(JSON_EXTRACT('{"signal_dbm": null}', '$$.missing'))"""
          .query[Option[String]].unique.transact(xa)
      jsonTypeOf <-
        sql"""SELECT JSON_TYPE(JSON_EXTRACT('{"signal_dbm": null}', '$$.signal_dbm'))"""
          .query[Option[String]].unique.transact(xa)
      // SELECT context: 1292 is a warning, cast yields 0 (PostgreSQL/PostgreSQL semantics)
      selectCast <- sql"SELECT CAST('null' AS SIGNED)".query[Int].unique.transact(xa)
    yield
      assertEquals(unquoted, Some("null"), "->> of JSON null must be the string 'null'")
      assertEquals(missingPath, None, "->> of a missing path must be SQL NULL")
      assertEquals(jsonTypeOf, Some("NULL"), "JSON_TYPE of JSON null must be 'NULL'")
      assertEquals(selectCast, 0, "plain SELECT CAST truncates to 0 with a warning")

  test("pre-15f6043 projection SQL fails with the exact production error for JSON-null ints"):
    requireDocker()
    val payload =
      """{"event_type":"wifi_data_frame","sensor_id":"old-sql-probe","signal_dbm":null,"retry":null}"""
    val hash = Sha256Utils.sha256Hex(payload)
    val ref = inlineRef(payload)
    val insert = sql"""INSERT INTO sync_events (
                         dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                         payload, status, producer, payload_archived
                       ) VALUES (
                         $hash, 'wireless.audit', TIMESTAMP('2026-07-27 12:01:00'),
                         $ref, $hash, $payload, 'completed', 'ssl-proxy', 0
                       )""".update.run
    // verbatim shape of the pre-15f6043 hydrateWirelessProjection assignments
    val oldProjection = sql"""UPDATE sync_events
            SET signal_dbm = CAST(NULLIF(COALESCE(
                  NULLIF(payload->>'$$.signal_dbm', ''),
                  NULLIF(payload->>'$$.rf.signal_dbm', '')
                ), '') AS SIGNED),
                retry = CASE LOWER(COALESCE(
                  NULLIF(payload->>'$$.retry', ''),
                  NULLIF(payload->>'$$.mac.retry', '')
                )) WHEN 'true' THEN 1 WHEN 'false' THEN 0 WHEN '1' THEN 1 WHEN '0' THEN 0 ELSE NULL END
            WHERE dedupe_key = $hash
              AND stream_name = 'wireless.audit'""".update.run
    for
      _ <- insert.transact(xa)
      result <- oldProjection.transact(xa).attempt
    yield result match
      case Left(error) =>
        assert(
          error.getMessage.contains("Truncated incorrect INTEGER value: 'null'"),
          s"expected the exact production error, got: ${error.getMessage}"
        )
      case Right(updated) =>
        fail(s"expected old projection SQL to fail, but it updated $updated rows")

  test("current hydrateExistingSyncEvent survives a fuzz battery of null-like payloads"):
    requireDocker()
    val observed = "2026-07-27 12:01:00"
    val allNullInts =
      """{"sensor_id":"fuzz-nulls","schema_version":null,"signal_dbm":null,"noise_dbm":null,
        |"frequency_mhz":null,"channel_flags":null,"data_rate_kbps":null,"antenna_id":null,
        |"tsft":null,"fragment_number":null,"channel_number":null,"qos_tid":null,"qos_eosp":null,
        |"qos_ack_policy":null,"qos_amsdu":null,"ethertype":null,"ip_ttl":null,"ip_protocol":null,
        |"src_port":null,"dst_port":null,"transport_length":null,"transport_checksum":null,
        |"tsft_delta_us":null,"wall_clock_delta_ms":null,"large_frame":null,"mixed_encryption":null,
        |"dedupe_or_replay_suspect":null,"raw_len":null,"frame_control_flags":null,"more_data":null,
        |"retry":null,"power_save":null,"protected":null,"security_flags":null,"risk_score":null,
        |"handshake_captured":null}""".stripMargin.replaceAll("\\s+", "")
    val allNullStrings = allNullInts.replace(":null", ":\"null\"").replace("fuzz-nulls", "fuzz-strings")
    val validAdjacentMacHint = "AA:BB:CC:DD:EE:01~AA:BB:CC:DD:EE:02"
    val validAdjacentMacPayload = io.circe.Json.obj(
      "sensor_id" -> io.circe.Json.fromString("fuzz-valid-adjacent-mac"),
      "adjacent_mac_hint" -> io.circe.Json.fromString(validAdjacentMacHint)
    ).noSpaces
    val validAdjacentMacHash = Sha256Utils.sha256Hex(validAdjacentMacPayload)
    val oversizedText = io.circe.Json.obj(
      "sensor_id" -> io.circe.Json.fromString("fuzz-oversized-text"),
      "adjacent_mac_hint" -> io.circe.Json.fromString("a" * 513)
    ).noSpaces
    val payloads = List(
      "all-json-null" -> allNullInts,
      "all-quoted-null" -> allNullStrings,
      "valid-adjacent-mac-hint" -> validAdjacentMacPayload,
      "oversized-text" -> oversizedText,
      "nested-containers-null" ->
        """{"sensor_id":"fuzz-nested","rf":null,"mac":null,"qos":null,"network":null,
          |"transport":null,"application":null,"correlation":null,"anomalies":null,
          |"llc_snap":null,"channel_flags":{"raw":null}}""".stripMargin.replaceAll("\\s+", ""),
      "numeric-boundaries" ->
        """{"sensor_id":"fuzz-bounds","signal_dbm":2147483647,"noise_dbm":-2147483648,
          |"tsft":9223372036854775807,"tsft_delta_us":-9223372036854775808,
          |"channel_flags":"2147483648","frequency_mhz":"999999999999999999999999",
          |"schema_version":"0002","antenna_id":"+7","data_rate_kbps":"-0",
          |"wall_clock_delta_ms":"9223372036854775808"}""".stripMargin.replaceAll("\\s+", ""),
      "scientific-and-whitespace" ->
        """{"sensor_id":"fuzz-sci","signal_dbm":" -42 ","noise_dbm":"4 2","tsft":"1e19",
          |"risk_score":"1e309","schema_version":"1e5","fragment_number":" 42",
          |"channel_number":"42 "}""".stripMargin.linesIterator.mkString,
      "boolean-int-cross-types" ->
        """{"sensor_id":"fuzz-bool","retry":2,"protected":true,"more_data":"yes","qos_eosp":1,
          |"signal_dbm":true,"large_frame":"0","handshake_captured":"TRUE","power_save":false,
          |"qos_amsdu":"false"}""".stripMargin.replaceAll("\\s+", ""),
      "text-fields-null" ->
        """{"sensor_id":null,"ssid":null,"source_mac":null,"tags":null,"username":null,
          |"event_type":null,"wps_device_name":null}""".stripMargin.replaceAll("\\s+", ""),
      "wrong-container-types" ->
        """{"sensor_id":"fuzz-containers","signal_dbm":{"value":5},"retry":[1],"ssid":["x"],
          |"tags":"not-array","schema_version":{"v":2},"risk_score":{"v":1}}"""
          .stripMargin.replaceAll("\\s+", ""),
      "unicode-and-newlines" ->
        """{"sensor_id":"fuzz-unicode","signal_dbm":"−42","noise_dbm":"٤٢",
          |"tsft":"42\n","channel_flags":"\t256"}""".stripMargin.replaceAll("\\s+", ""),
      "empty-strings" ->
        """{"sensor_id":"fuzz-empty","signal_dbm":"","ssid":"","schema_version":" ","retry":""}"""
          .stripMargin.linesIterator.mkString
    )

    for
      results <- payloads.traverse { case (label, payload) =>
        val hash = Sha256Utils.sha256Hex(payload)
        val ref = inlineRef(payload)
        val insert = sql"""INSERT INTO sync_events (
                             dedupe_key, stream_name, observed_at, payload_ref, payload_sha256,
                             payload, status, producer, payload_archived
                           ) VALUES (
                             $hash, 'wireless.audit', TIMESTAMP('2026-07-27 12:01:00'),
                             $ref, $hash, $payload, 'completed', 'ssl-proxy', 0
                           )""".update.run
        val candidate = SyncEventHydrationCandidate(
          hash,
          "wireless.audit",
          Timestamp.valueOf(observed),
          ref,
          Some(payload)
        )
        for
          _ <- insert.transact(xa)
          result <- repository.hydrateExistingSyncEvent(candidate, payload)
        yield (label, result)
      }
      storedAdjacentMacHint <- sql"""SELECT adjacent_mac_hint
                                      FROM sync_events
                                      WHERE dedupe_key = $validAdjacentMacHash
                                        AND stream_name = 'wireless.audit'"""
        .query[Option[String]].unique.transact(xa)
    yield
      val failures = results.collect { case (label, Left(error)) => s"$label: ${error.message}" }
      assertEquals(failures, Nil, "fuzz battery must hydrate without db errors")
      val notHydrated = results.collect { case (label, Right(false)) => label }
      assertEquals(notHydrated, Nil, "every fuzz row must report hydrated=true")
      assertEquals(storedAdjacentMacHint, Some(validAdjacentMacHint.toLowerCase))

  private def inlineRef(payload: String): String =
    "inline://json/" + Base64.getUrlEncoder.withoutPadding
      .encodeToString(payload.getBytes(StandardCharsets.UTF_8))

  private def requireDocker(): Unit =
    assume(dockerAvailable, "Docker is required for the PostgreSQL integration suite")

  private def applyCanonicalManifest(): Unit =
    val manifest = schemaRoot.resolve("manifest.yaml")
    val applyOrder = Files.readAllLines(manifest).asScala
      .dropWhile(_ != "apply_order:")
      .drop(1)
      .takeWhile(_.startsWith("  - "))
      .map(_.drop(4).trim)

    val connection = DriverManager.getConnection(jdbcUrl, "postgres", "postgres")
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
      .map(_.resolve("sql/postgres/octopus_core"))
      .find(path => Files.isDirectory(path))
      .getOrElse(throw IllegalStateException("cannot locate canonical octopus_core schema"))
