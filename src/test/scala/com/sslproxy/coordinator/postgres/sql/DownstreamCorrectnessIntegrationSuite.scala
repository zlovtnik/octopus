package com.sslproxy.coordinator.postgres.sql

import cats.effect.IO
import com.sslproxy.coordinator.processor.Lease
import doobie.Transactor
import doobie.implicits.*
import munit.CatsEffectSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.{Wait, WaitAllStrategy}
import org.testcontainers.utility.DockerImageName

import java.sql.DriverManager
import scala.util.Using

class DownstreamCorrectnessIntegrationSuite extends CatsEffectSuite:
  private class Database extends PostgreSQLContainer[Database](DockerImageName.parse("postgres:17"))
  private val database = new Database()
    .withDatabaseName("octopus_downstream_test")
    .withUsername("postgres")
    .withPassword("test-password")
    .waitingFor(
      new WaitAllStrategy()
        .withStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2))
        .withStrategy(Wait.forSuccessfulCommand("pg_isready -h 127.0.0.1 -U postgres -d octopus_downstream_test"))
    )
  private lazy val available = DockerClientFactory.instance().isDockerAvailable
  private def xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver", database.getJdbcUrl, database.getUsername, database.getPassword, None
  )

  override def beforeAll(): Unit =
    if available then
      database.start()
      Using.resource(DriverManager.getConnection(database.getJdbcUrl, database.getUsername, database.getPassword)) {
        connection =>
          Using.resource(connection.createStatement()) { statement =>
            statement.execute("""
              CREATE SCHEMA atheros_search;
              CREATE TABLE work_leases(resource_type text, resource_id text, owner_id text,
                lease_token text, fence bigint, lease_expires_at timestamptz);
              CREATE TABLE atheros_search.search_documents(document_id text, status text, bssid text);
              CREATE TABLE atheros_search.search_vectors_event(vector_id bigint, document_id text);
              CREATE TABLE atheros_search.search_vectors_device(vector_id bigint, document_id text);
              CREATE TABLE atheros_search.search_vectors_behaviour(vector_id bigint, document_id text);
              CREATE TABLE atheros_search.search_vectors_sequence(vector_id bigint, document_id text);
              CREATE TABLE atheros_search.embeddings(document_id text);
              CREATE TABLE atheros_search.similarity_scan_state(embedding_kind text, vector_id bigint);
              CREATE TABLE atheros_search.similarity_pairs(left_document_id text, right_document_id text,
                cosine_distance double precision, computed_at timestamptz);
              CREATE TABLE atheros_search.embedding_jobs(document_id text, status text);
              CREATE TABLE atheros_search.search_document_tokens(document_id text);
              CREATE TABLE atheros_search.search_document_tags(document_id text);
              CREATE TABLE proxy_events(host text, blocked boolean, event_time timestamptz,
                bytes_up bigint, bytes_down bigint);
              CREATE TABLE atheros_search.threat_signals(source_key text, updated_at timestamptz);
              CREATE TABLE wireless_frames(bssid text, observed_at timestamptz, ssid text, bssid_oui text);
              CREATE TABLE wireless_alerts(primary_mac text, secondary_mac text, alert_type text,
                detected_at timestamptz);
              CREATE TABLE atheros_search.ap_risk_scores(bssid text, measured_at timestamptz);
            """): Unit
          }
      }

  override def afterAll(): Unit = if available then database.stop()

  private def requireDatabase(): Unit =
    if !available && sys.env.get("OCTOPUS_REQUIRE_DOCKER").contains("true") then
      fail("Docker is required for PostgreSQL integration tests")
    assume(available, "Docker is unavailable")

  test("retention removes unified vectors and similarity references without deleting another kind's scan state") {
    requireDatabase()
    val lease = Lease("search-retention", "test-owner", "test-token", 1L, java.time.Instant.now().plusSeconds(3600))
    val work = for
      _ <- sql"""INSERT INTO work_leases VALUES
        ('retention', 'search', 'test-owner', 'test-token', 1, CURRENT_TIMESTAMP + INTERVAL '1 hour')""".update.run
      _ <- sql"""INSERT INTO atheros_search.search_documents VALUES ('expired', 'superseded', NULL)""".update.run
      _ <- sql"""INSERT INTO atheros_search.search_vectors_event VALUES (1, 'expired')""".update.run
      _ <- sql"""INSERT INTO atheros_search.search_vectors_device VALUES (1, 'retained')""".update.run
      _ <- sql"""INSERT INTO atheros_search.similarity_scan_state VALUES ('event', 1), ('device', 1)""".update.run
      _ <- sql"""INSERT INTO atheros_search.embeddings VALUES ('expired'), ('retained')""".update.run
      _ <- sql"""INSERT INTO atheros_search.similarity_pairs VALUES
        ('expired', 'retained', 0.2, CURRENT_TIMESTAMP), ('retained', 'expired', 0.2, CURRENT_TIMESTAMP)""".update.run
      deleted <- MaintenanceSql.deleteRetainedSearchDocument("expired", "retention", "search", lease)
      vectors <- sql"""SELECT document_id FROM atheros_search.embeddings ORDER BY document_id""".query[String].to[List]
      pairs <- sql"""SELECT count(*) FROM atheros_search.similarity_pairs""".query[Long].unique
      scans <- sql"""SELECT embedding_kind FROM atheros_search.similarity_scan_state""".query[String].to[List]
    yield
      assert(deleted)
      assertEquals(vectors, List("retained"))
      assertEquals(pairs, 0L)
      assertEquals(scans, List("device"))
    work.transact(xa)
  }

  test("DNS candidates rotate past the latest projected host when the batch is smaller than the backlog") {
    requireDatabase()
    val work = for
      _ <- sql"""INSERT INTO proxy_events VALUES
        ('new.example', true, CURRENT_TIMESTAMP - INTERVAL '1 minute', 1, 1),
        ('new.example', true, CURRENT_TIMESTAMP - INTERVAL '2 minutes', 1, 1),
        ('old.example', true, CURRENT_TIMESTAMP - INTERVAL '1 day', 1, 1),
        ('old.example', true, CURRENT_TIMESTAMP - INTERVAL '2 days', 1, 1)""".update.run
      first <- ThreatRiskSql.dnsCandidates(1).to[List]
      _ <- sql"""INSERT INTO atheros_search.threat_signals VALUES ('dns:new.example', CURRENT_TIMESTAMP)""".update.run
      next <- ThreatRiskSql.dnsCandidates(1).to[List]
    yield
      assertEquals(first.map(_.host), List("new.example"))
      assertEquals(next.map(_.host), List("old.example"))
    work.transact(xa)
  }

  test("AP risk candidates rotate past the latest projected BSSID") {
    requireDatabase()
    val work = for
      _ <- sql"""INSERT INTO wireless_frames VALUES
        ('aa:bb:cc:dd:ee:01', CURRENT_TIMESTAMP - INTERVAL '1 minute', NULL, NULL),
        ('aa:bb:cc:dd:ee:02', CURRENT_TIMESTAMP - INTERVAL '1 day', NULL, NULL)""".update.run
      first <- ThreatRiskSql.apRiskCandidates(1).to[List]
      _ <- sql"""INSERT INTO atheros_search.ap_risk_scores VALUES ('aa:bb:cc:dd:ee:01', CURRENT_TIMESTAMP)""".update.run
      next <- ThreatRiskSql.apRiskCandidates(1).to[List]
    yield
      assertEquals(first.map(_._1), List("aa:bb:cc:dd:ee:01"))
      assertEquals(next.map(_._1), List("aa:bb:cc:dd:ee:02"))
    work.transact(xa)
  }
