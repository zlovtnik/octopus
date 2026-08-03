package com.sslproxy.coordinator.tidb

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.*
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import doobie.Transactor
import munit.CatsEffectSuite

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext

class LiveHydrationProbeSuite extends CatsEffectSuite:
  test("reproduce production hydration candidates in an isolated scratch database"):
    if !sys.env.get("OCTOPUS_LIVE_HYDRATION_PROBE").contains("true") then
      IO(assume(false, "set OCTOPUS_LIVE_HYDRATION_PROBE=true to enable the live probe"))
    else resources.use { case (dataSource, executor, sourceDatabase, scratchDatabase) =>
      val xa = Transactor.fromDataSource[IO](
        dataSource,
        ExecutionContext.fromExecutorService(executor)
      )
      val repository = new TidbRepository(xa)

      for
        seeds <- IO.blocking(seedScratchTables(dataSource, sourceDatabase, scratchDatabase))
        results <- seeds.traverse { case (candidate, payload) =>
          repository.hydrateExistingSyncEvent(candidate, payload)
        }
        failures = results.collect { case Left(error) => error.message }
      yield assertEquals(failures, Nil)
    }

  private def resources =
    Resource.make(IO.blocking {
      val database = requiredEnv("OCTOPUS_LIVE_TIDB_DATABASE")
      require(database.matches("[A-Za-z0-9_]+"), "OCTOPUS_LIVE_TIDB_DATABASE must be a safe identifier")
      val scratchDatabase = requiredEnv("OCTOPUS_LIVE_TIDB_SCRATCH_DATABASE")
      require(
        scratchDatabase.matches("[A-Za-z0-9_]+"),
        "OCTOPUS_LIVE_TIDB_SCRATCH_DATABASE must be a safe identifier"
      )
      require(
        scratchDatabase != database,
        "OCTOPUS_LIVE_TIDB_SCRATCH_DATABASE must differ from OCTOPUS_LIVE_TIDB_DATABASE"
      )
      val config = new HikariConfig()
      config.setJdbcUrl(requiredEnv("OCTOPUS_LIVE_TIDB_JDBC_URL"))
      config.setUsername(requiredEnv("OCTOPUS_LIVE_TIDB_USER"))
      config.setPassword(requiredEnv("OCTOPUS_LIVE_TIDB_PASSWORD"))
      config.setDriverClassName("com.mysql.cj.jdbc.Driver")
      config.setMaximumPoolSize(1)
      config.setMinimumIdle(1)
      config.setCatalog(scratchDatabase)
      (new HikariDataSource(config), Executors.newSingleThreadExecutor(), database, scratchDatabase)
    }) { case (dataSource, executor, _, scratchDatabase) =>
      IO.blocking {
        try dropScratchTables(dataSource, scratchDatabase)
        finally
          dataSource.close()
          executor.shutdown()
      }
    }

  private def seedScratchTables(
      dataSource: HikariDataSource,
      sourceDatabase: String,
      scratchDatabase: String
  ): List[(SyncEventHydrationCandidate, String)] =
    val connection = dataSource.getConnection
    try
      connection.setCatalog(scratchDatabase)
      val sourceTable = s"`$sourceDatabase`.`sync_events`"
      val scratchTable = s"`$scratchDatabase`.`sync_events`"
      val sourceTombstones = s"`$sourceDatabase`.`sync_event_tombstones`"
      val scratchTombstones = s"`$scratchDatabase`.`sync_event_tombstones`"
      val select = connection.prepareStatement(
        """SELECT dedupe_key, stream_name, observed_at, payload_ref, payload_sha256
          |FROM %s e
          |WHERE payload_archived = 0
          |  AND stream_name = 'wireless.audit'
          |  AND (payload IS NULL OR event_type IS NULL OR schema_version IS NULL
          |       OR sensor_id IS NULL OR wireless_search_text IS NULL)
          |ORDER BY observed_at, dedupe_key
          |LIMIT 20""".stripMargin.format(sourceTable)
      )
      val result = select.executeQuery()
      val candidates = List.newBuilder[SyncEventHydrationCandidate]
      while result.next() do
        candidates += SyncEventHydrationCandidate(
          result.getString(1),
          result.getString(2),
          result.getTimestamp(3),
          result.getString(4),
          None,
          Option(result.getString(5))
        )
      result.close()
      select.close()
      val candidateList = candidates.result()
      if candidateList.isEmpty then fail("expected production hydration candidates")

      val setup = connection.createStatement()
      setup.execute(s"CREATE TABLE $scratchTable LIKE $sourceTable")
      setup.execute(s"CREATE TABLE $scratchTombstones LIKE $sourceTombstones")
      val copy = connection.prepareStatement(
        s"""INSERT INTO $scratchTable
          |SELECT * FROM %s
          |WHERE payload_archived = 0
          |  AND stream_name = 'wireless.audit'
          |  AND (payload IS NULL OR event_type IS NULL OR schema_version IS NULL
          |       OR sensor_id IS NULL OR wireless_search_text IS NULL)
          |ORDER BY observed_at, dedupe_key
          |LIMIT 20""".stripMargin.format(sourceTable)
      )
      copy.executeUpdate()
      copy.close()
      setup.close()

      val resolver = new TidbPayloadResolver("/unused")
      candidateList.map(candidate => candidate -> resolver.resolvePayload(candidate.payloadRef))
    finally connection.close()

  private def dropScratchTables(
      dataSource: HikariDataSource,
      scratchDatabase: String
  ): Unit =
    val connection = dataSource.getConnection
    try
      val statement = connection.createStatement()
      try
        statement.execute(s"DROP TABLE IF EXISTS `$scratchDatabase`.`sync_event_tombstones`"): Unit
        statement.execute(s"DROP TABLE IF EXISTS `$scratchDatabase`.`sync_events`"): Unit
      finally statement.close()
    finally connection.close()

  private def requiredEnv(name: String): String =
    sys.env.get(name).filter(_.nonEmpty).getOrElse(
      throw IllegalArgumentException(s"$name is required when OCTOPUS_LIVE_HYDRATION_PROBE=true")
    )
