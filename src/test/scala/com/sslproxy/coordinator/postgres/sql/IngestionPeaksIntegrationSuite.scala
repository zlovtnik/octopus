package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.wait.strategy.WaitAllStrategy
import org.testcontainers.utility.DockerImageName
import java.sql.DriverManager
import scala.util.Using

class IngestionPeaksIntegrationSuite extends FunSuite:
  private class Database extends PostgreSQLContainer[Database](DockerImageName.parse("postgres:17"))

  // PostgreSQL starts twice during image init; a listening TCP port can still
  // reject connections ("database system is starting up"). Wait for the second
  // ready log and a TCP pg_isready before DriverManager connects.
  private val database = new Database()
    .withDatabaseName("octopus_peaks_test")
    .withUsername("postgres")
    .withPassword("test-password")
    .waitingFor(
      new WaitAllStrategy()
        .withStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2))
        .withStrategy(Wait.forSuccessfulCommand(
          "pg_isready -h 127.0.0.1 -p 5432 -U postgres -d octopus_peaks_test"
        ))
    )

  private lazy val available = DockerClientFactory.instance().isDockerAvailable
  override def beforeAll(): Unit = if available then database.start()
  override def afterAll(): Unit = if available then database.stop()

  test("real PostgreSQL computes UTC days and Monday-Sunday weeks across boundaries"):
    if !available && sys.env.get("OCTOPUS_REQUIRE_DOCKER").contains("true") then fail("Docker is required for PostgreSQL integration tests")
    assume(available, "Docker is unavailable")
    Using.resource(DriverManager.getConnection(
      database.getJdbcUrl,
      database.getUsername,
      database.getPassword
    )) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        statement.execute("SET TIME ZONE 'America/Los_Angeles'"): Unit
        statement.execute("CREATE TEMP TABLE ingestion_evidence (first_seen_at timestamptz NOT NULL)"): Unit
        statement.execute("""INSERT INTO ingestion_evidence VALUES
          ('2026-01-04T23:59:59Z'), ('2026-01-05T00:00:00Z'),
          ('2026-01-05T10:00:00Z'), ('2026-01-11T23:59:59Z'),
          ('2026-01-12T00:00:00Z')"""): Unit
        Using.resource(statement.executeQuery(IngestionSql.PeakRecordsDayQuery.sql)) { rows =>
          assert(rows.next())
          assertEquals(rows.getLong(1), 2L)
          assertEquals(rows.getString(2), "2026-01-05")
        }
        Using.resource(statement.executeQuery(IngestionSql.PeakRecordsWeekQuery.sql)) { rows =>
          assert(rows.next())
          assertEquals(rows.getLong(1), 3L)
          assertEquals(rows.getString(2), "2026-01-05")
          assertEquals(rows.getString(3), "2026-01-11")
        }
        statement.execute("TRUNCATE ingestion_evidence"): Unit
        Using.resource(statement.executeQuery(IngestionSql.PeakRecordsDayQuery.sql))(rows => assert(!rows.next()))
        Using.resource(statement.executeQuery(IngestionSql.PeakRecordsWeekQuery.sql))(rows => assert(!rows.next()))
      }
    }
