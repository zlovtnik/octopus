package com.sslproxy.coordinator.postgres

import cats.effect.IO
import com.sslproxy.coordinator.config.AppConfig
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.OffsetDateTime
import munit.CatsEffectSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import scala.concurrent.duration.*
import scala.util.Using

class PostgresSinkTimeoutIntegrationSuite extends CatsEffectSuite:
  private class Database extends GenericContainer[Database](DockerImageName.parse("postgres:17"))
  private lazy val database = new Database()
    .withExposedPorts(5432)
    .withEnv("POSTGRES_PASSWORD", "test-password")
    .withEnv("POSTGRES_DB", "octopus_timeout_test")
  private lazy val available = DockerClientFactory.instance().isDockerAvailable
  private var pool: HikariDataSource = null
  private def sink = PostgresTransactor.fromDataSource(pool, AppConfig.load.postgres.copy(
    statementTimeoutSecs = 1, networkTimeoutSecs = 4
  ))

  private def execute(conn: Connection, sql: String): Unit =
    Using.resource(conn.createStatement()) { statement => statement.execute(sql): Unit }

  private def value(conn: Connection, sql: String): String =
    Using.resource(conn.createStatement()) { statement =>
      Using.resource(statement.executeQuery(sql)) { result =>
        assert(result.next())
        result.getString(1)
      }
    }

  override def beforeAll(): Unit =
    super.beforeAll()
    if available then
      database.start()
      val config = new HikariConfig()
      config.setJdbcUrl(s"jdbc:postgresql://${database.getHost}:${database.getMappedPort(5432)}/octopus_timeout_test")
      config.setUsername("postgres")
      config.setPassword("test-password")
      config.setMaximumPoolSize(2)
      config.setConnectionTimeout(5000)
      config.setConnectionInitSql("SET search_path TO octopus_core")
      pool = new HikariDataSource(config)
      Using.resource(pool.getConnection) { conn =>
        execute(conn, "CREATE SCHEMA octopus_core")
        execute(conn, "CREATE TABLE timeout_probe (id integer PRIMARY KEY, value integer)")
        execute(conn, "INSERT INTO timeout_probe VALUES (1, 0)")
        // Read the canonical inventory table definition; never change its replay contract.
        val ddl = Files.readString(Path.of("../../sql/postgres/octopus_core/01_tables/005_wireless_sink.sql"))
        val start = ddl.indexOf("CREATE TABLE IF NOT EXISTS octopus_core.wireless_client_inventory (")
        assert(start >= 0)
        execute(conn, ddl.substring(start, ddl.indexOf(";", start) + 1))
      }

  override def afterAll(): Unit =
    try
      if pool != null then pool.close()
    finally
      if available then database.stop()
      super.afterAll()

  test("statement cancellation precedes network deadline, rolls back writes, and resets local timeout"):
    assume(available, "Docker is required for ephemeral PostgreSQL verification")
    val before = Using.resource(pool.getConnection)(conn => (value(conn, "SHOW statement_timeout"), conn.getNetworkTimeout))
    for
      started <- IO.monotonic
      result <- sink.withTransaction { conn =>
        execute(conn, "INSERT INTO timeout_probe VALUES (2, 0)")
        execute(conn, "SELECT pg_sleep(10)")
      }.attempt
      finished <- IO.monotonic
      _ <- IO {
        assertEquals(result.left.toOption.map(PostgresErrorClass.classify), Some(PostgresErrorClass.Retryable))
        assert(result.left.toOption.exists(error => com.sslproxy.coordinator.util.ErrorSanitizer.message(error).contains("57014")))
        assert((finished - started) < 4.seconds)
        Using.resource(pool.getConnection) { conn =>
          assertEquals(value(conn, "SELECT count(*) FROM timeout_probe WHERE id=2"), "0")
          assertEquals(value(conn, "SHOW statement_timeout"), before._1)
          assertEquals(conn.getNetworkTimeout, before._2)
        }
      }
      reused <- sink.withTransaction(conn => value(conn, "SELECT 1"))
    yield assertEquals(reused, "1")

  test("lock wait cancels the whole transaction and connection is reusable"):
    assume(available, "Docker is required for ephemeral PostgreSQL verification")
    val blocker = pool.getConnection
    blocker.setAutoCommit(false)
    execute(blocker, "UPDATE timeout_probe SET value=1 WHERE id=1")
    sink.withTransaction { conn =>
      execute(conn, "INSERT INTO timeout_probe VALUES (3, 0)")
      execute(conn, "UPDATE timeout_probe SET value=2 WHERE id=1")
    }.attempt.flatMap { result =>
      IO {
        assert(result.left.toOption.exists(error => com.sslproxy.coordinator.util.ErrorSanitizer.message(error).contains("57014")))
      }
    }.guarantee(IO.blocking { blocker.rollback(); blocker.close() }) *>
      sink.withTransaction { conn =>
        assertEquals(value(conn, "SELECT count(*) FROM timeout_probe WHERE id=3"), "0")
        assertEquals(value(conn, "SELECT value FROM timeout_probe WHERE id=1"), "0")
      }

  test("inventory replay preserves exactly one snapshot with both present and nullable BSSID"):
    assume(available, "Docker is required for ephemeral PostgreSQL verification")
    val now = OffsetDateTime.parse("2026-09-09T12:00:00Z")
    val rows = List(None, Some("02:00:00:00:00:03")).zipWithIndex.map { (bssid, index) =>
      WirelessClientInventoryInsert("synthetic-sensor", "synthetic-location", now,
        s"02:00:00:00:00:0${index + 1}", bssid, Some("synthetic-ssid"), None, None, None,
        now, now.minusHours(1), Some(-50L), false)
    }
    sink.insertWirelessClientInventory("synthetic-batch", rows) *>
      sink.insertWirelessClientInventory("synthetic-batch", rows) *>
      sink.withTransaction { conn =>
        assertEquals(value(conn, "SELECT count(*) FROM wireless_client_inventory"), "2")
        assertEquals(value(conn, "SELECT count(*) FROM wireless_client_inventory WHERE bssid IS NULL"), "1")
      }
