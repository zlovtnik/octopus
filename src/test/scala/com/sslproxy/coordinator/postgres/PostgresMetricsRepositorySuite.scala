package com.sslproxy.coordinator.postgres

import cats.effect.{IO, Resource}
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.AppConfig
import com.sslproxy.coordinator.metrics.{DayPeak, ThroughputPoint, WeekPeak}
import munit.CatsEffectSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}
import java.sql.Connection
import java.time.Instant
import scala.concurrent.duration.*
import scala.util.Using

class PostgresMetricsRepositorySuite extends CatsEffectSuite:
  private class Database extends GenericContainer[Database](DockerImageName.parse("postgres:17"))
  private lazy val available = DockerClientFactory.instance().isDockerAvailable
  private val at = Instant.parse("2026-10-08T12:30:00Z")

  private def requireDocker(): Unit =
    if sys.env.get("OCTOPUS_REQUIRE_DOCKER").contains("true") && !available then
      throw IllegalStateException("OCTOPUS_REQUIRE_DOCKER=true but Docker is unavailable")
    else assume(available, "Docker is required for ephemeral PostgreSQL verification")

  private def execute(connection: Connection, sql: String): Unit =
    Using.resource(connection.createStatement())(s => s.execute(sql): Unit)

  private def database: Resource[IO, PostgresTransactor] =
    for
      container <- Resource.make(IO.blocking {
        val db = new Database().withExposedPorts(5432)
          .withEnv("POSTGRES_PASSWORD", "test-password")
          .withEnv("POSTGRES_DB", "octopus_metrics_test")
        db.start()
        db
      })(db => IO.blocking(db.stop()))
      tx <- PostgresTransactor.resource(AppConfig.load.postgres.copy(
        host = container.getHost,
        port = container.getMappedPort(5432).intValue,
        database = "octopus_metrics_test",
        user = "postgres",
        password = "test-password",
        sslMode = "disable",
        poolSize = 2,
        connectionTimeoutMs = 1000,
        statementTimeoutSecs = 1,
        networkTimeoutSecs = 4
      ))
      _ <- Resource.eval(tx.withTransaction { conn =>
        execute(conn, "CREATE SCHEMA octopus_core")
        val ddl = Files.readString(Path.of("../../sql/postgres/octopus_core/01_tables/006_legacy_sync_ledger.sql"))
        val start = ddl.indexOf("CREATE TABLE IF NOT EXISTS octopus_core.ingestion_evidence (")
        assert(start >= 0)
        execute(conn, ddl.substring(start, ddl.indexOf(";", start) + 1))
      })
    yield tx

  private def seed(tx: PostgresTransactor): IO[Unit] = tx.withTransaction { conn =>
    val times = List(
      "2026-10-01T12:00:00Z", "2026-10-07T11:59:59Z", "2026-10-07T12:00:00Z",
      "2026-10-08T11:00:00Z", "2026-10-08T12:00:00Z", "2026-10-08T14:00:00Z"
    )
    Using.resource(conn.prepareStatement(
      """INSERT INTO octopus_core.ingestion_evidence
        |(topic, partition_id, record_offset, group_id, group_version, artifact_sha256, payload_sha256, first_seen_at)
        |VALUES ('synthetic', 0, ?, 'metrics-test', 'v1', repeat('a', 64), repeat('b', 64), ?)
        |ON CONFLICT ON CONSTRAINT ingestion_evidence_pkey DO UPDATE SET updated_at = CURRENT_TIMESTAMP""".stripMargin
    )) { statement =>
      times.zipWithIndex.foreach { (time, offset) =>
        statement.setLong(1, offset.toLong)
        statement.setObject(2, Instant.parse(time).atOffset(java.time.ZoneOffset.UTC))
        val _ = statement.executeUpdate()
      }
    }
  }

  test("empty ledger is null-safe; UTC aggregates count evidence once with exact window boundaries"):
    requireDocker()
    database.use { tx =>
      val repo = new PostgresMetricsRepository(tx)
      for
        emptyDay <- repo.peakDay
        emptyWeek <- repo.peakWeek
        emptyTotal <- repo.lifetimeTotals(at)
        emptyHistory <- repo.hourlyBuckets(at.minusSeconds(86400), at)
        _ <- seed(tx) *> seed(tx)
        day <- repo.peakDay
        week <- repo.peakWeek
        total <- repo.lifetimeTotals(at)
        dayHistory <- repo.hourlyBuckets(Instant.parse("2026-10-07T12:00:00Z"), Instant.parse("2026-10-08T12:00:00Z"))
        weekHistory <- repo.hourlyBuckets(Instant.parse("2026-10-01T12:00:00Z"), Instant.parse("2026-10-08T12:00:00Z"))
      yield
        assertEquals(emptyDay, None)
        assertEquals(emptyWeek, None)
        assertEquals(emptyTotal.recordsTotal, 0L)
        assertEquals(emptyTotal.daysCounted, 0L)
        assertEquals(emptyHistory, Nil)
        assertEquals(day, Some(DayPeak(3L, "2026-10-08")))
        assertEquals(week, Some(WeekPeak(5L, "2026-10-05", "2026-10-11")))
        assertEquals(total.recordsTotal, 6L)
        assertEquals(total.daysCounted, 3L)
        assertEquals(dayHistory, List(
          ThroughputPoint("2026-10-07T12:00:00Z", 1L), ThroughputPoint("2026-10-08T11:00:00Z", 1L)
        ))
        assertEquals(weekHistory.map(_.records).sum, 4L)
    }

  test("query timeout releases pool connection and permit, then a later refresh succeeds"):
    requireDocker()
    database.use { tx =>
      for
        permit <- Semaphore[IO](1)
        repo = new PostgresMetricsRepository(tx, Some(permit))
        result <- Resource.make(IO.blocking {
          val connection = tx.dataSource.getConnection
          try
            connection.setAutoCommit(false)
            execute(connection, "LOCK TABLE octopus_core.ingestion_evidence IN ACCESS EXCLUSIVE MODE")
            connection
          catch
            case scala.util.control.NonFatal(error) => connection.close(); throw error
        })(connection => IO.blocking { try connection.rollback() finally connection.close() }).use { _ =>
          for
            start <- IO.monotonic
            result <- repo.lifetimeTotals(at).attempt
            end <- IO.monotonic
            permits <- permit.available
            _ <- IO.blocking {
              assertEquals(tx.dataSource.getHikariPoolMXBean.getActiveConnections, 1)
            }
          yield
            assert(result.left.toOption.exists(error =>
              PostgresErrorClass.exceptions(error).exists {
                case sql: java.sql.SQLException => sql.getSQLState == "57014"
                case _ => false
              }
            ))
            assert((end - start) < 4.seconds)
            assertEquals(permits, 1L)
        }
        total <- repo.lifetimeTotals(at)
        _ <- IO.blocking(assertEquals(tx.dataSource.getHikariPoolMXBean.getActiveConnections, 0))
      yield
        assertEquals(result, ())
        assertEquals(total.recordsTotal, 0L)
    }

  test("canceling an in-flight JDBC read cleans up after its database deadline"):
    requireDocker()
    database.use { tx =>
      for
        permits <- Semaphore[IO](1)
        repo = new PostgresMetricsRepository(tx, Some(permits))
        _ <- Resource.make(IO.blocking {
          val connection = tx.dataSource.getConnection
          connection.setAutoCommit(false)
          execute(connection, "LOCK TABLE octopus_core.ingestion_evidence IN ACCESS EXCLUSIVE MODE")
          connection
        })(connection => IO.blocking { try connection.rollback() finally connection.close() }).use { _ =>
          Resource.make(repo.peakDay.start)(_.cancel).use { fiber =>
            def awaitBorrowed: IO[Unit] =
              IO.blocking(tx.dataSource.getHikariPoolMXBean.getActiveConnections).flatMap { active =>
                if active == 2 then IO.unit else IO.cede *> awaitBorrowed
              }
            awaitBorrowed.timeout(2.seconds) *> fiber.cancel.timeout(4.seconds)
          }
        }
        remaining <- permits.available
        total <- repo.lifetimeTotals(at)
        _ <- IO.blocking(assertEquals(tx.dataSource.getHikariPoolMXBean.getActiveConnections, 0))
      yield
        assertEquals(remaining, 1L)
        assertEquals(total.recordsTotal, 0L)
    }
