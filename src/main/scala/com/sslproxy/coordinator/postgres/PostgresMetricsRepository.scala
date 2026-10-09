package com.sslproxy.coordinator.postgres

import cats.effect.{IO, Resource}
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.PostgresConfig
import com.sslproxy.coordinator.metrics.*
import com.sslproxy.coordinator.postgres.sql.IngestionSql

import java.sql.{PreparedStatement, ResultSet, SQLException, Timestamp}
import java.time.Instant
import scala.util.Using

/** Shares the application's Resource-managed Hikari pool and worker permits.
  * withTransaction encloses acquisition, JDBC calls, decoding and cleanup in
  * IO.blocking, with transaction-local statement and connection network limits.
  * Metrics are retried on the next refresh, never in an ingestion retry loop.
  */
final class PostgresMetricsRepository(
  transactor: PostgresTransactor,
  permits: Option[Semaphore[IO]] = None
) extends MetricsRepository[IO]:
  private def query[A](sql: String)(bind: PreparedStatement => Unit)(read: ResultSet => A): IO[A] =
    val run = transactor.withTransaction { connection =>
      Using.resource(connection.prepareStatement(sql)) { statement =>
        bind(statement)
        Using.resource(statement.executeQuery())(read)
      }
    }
    permits.fold(run)(_.permit.use(_ => run))

  // COUNT is non-null in PostgreSQL. A malformed row must fail the measurement,
  // rather than getLong silently converting SQL NULL to a fabricated zero.
  private def count(rows: ResultSet, column: Int): Long =
    val value = rows.getLong(column)
    if rows.wasNull() || value < 0 then throw SQLException("Invalid metrics count")
    value

  private def text(rows: ResultSet, column: Int): String =
    Option(rows.getString(column)).getOrElse(throw SQLException("Missing metrics timestamp"))

  def peakDay: IO[Option[DayPeak]] =
    query(IngestionSql.PeakRecordsDayQuery.sql)(_ => ()) { rows =>
      Option.when(rows.next())(DayPeak(count(rows, 1), text(rows, 2)))
    }

  def peakWeek: IO[Option[WeekPeak]] =
    query(IngestionSql.PeakRecordsWeekQuery.sql)(_ => ()) { rows =>
      Option.when(rows.next())(WeekPeak(count(rows, 1), text(rows, 2), text(rows, 3)))
    }

  def lifetimeTotals(computedAt: Instant): IO[LifetimeTotals] =
    query(IngestionSql.IngestionLifetimeTotalsQuery.sql)(_ => ()) { rows =>
      if !rows.next() then throw SQLException("Missing lifetime aggregate")
      LifetimeTotals(count(rows, 1), count(rows, 2), StatsSnapshot.toIso(computedAt))
    }

  def hourlyBuckets(from: Instant, until: Instant): IO[List[ThroughputPoint]] =
    IO.raiseWhen(!from.isBefore(until))(IllegalArgumentException("Empty metrics time window")) *>
      query(IngestionSql.ingestionHourlyBuckets(Timestamp.from(from), Timestamp.from(until)).sql) { statement =>
        // OffsetDateTime binds timestamptz explicitly, independent of the JVM/session zone.
        statement.setObject(1, from.atOffset(java.time.ZoneOffset.UTC))
        statement.setObject(2, until.atOffset(java.time.ZoneOffset.UTC))
      } { rows =>
        val points = List.newBuilder[ThroughputPoint]
        while rows.next() do
          points += ThroughputPoint(text(rows, 1), count(rows, 2))
        points.result()
      }

object PostgresMetricsRepository:
  /** Standalone managed setup; Main reuses its existing pool instead. */
  def resource(config: PostgresConfig): Resource[IO, MetricsRepository[IO]] =
    PostgresTransactor.resource(config).map(tx => new PostgresMetricsRepository(tx))
