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
  test("reproduce one production hydration candidate in a temporary table"):
    resources.use { case (dataSource, executor) =>
      val xa = Transactor.fromDataSource[IO](
        dataSource,
        ExecutionContext.fromExecutorService(executor)
      )
      val repository = new TidbRepository(xa)

      for
        seeds <- IO.blocking(seedTemporaryTable(dataSource))
        results <- seeds.traverse { case (candidate, payload) =>
          repository.hydrateExistingSyncEvent(candidate, payload)
        }
        failures = results.collect { case Left(error) => error.message }
      yield assertEquals(failures, Nil)
    }

  private def resources =
    Resource.make(IO.blocking {
      val config = new HikariConfig()
      config.setJdbcUrl(
        "jdbc:mysql://192.168.1.221:4000/octopus_core?useSSL=false&allowPublicKeyRetrieval=true"
      )
      config.setUsername("root")
      config.setPassword("")
      config.setDriverClassName("com.mysql.cj.jdbc.Driver")
      config.setMaximumPoolSize(1)
      config.setMinimumIdle(1)
      new HikariDataSource(config) -> Executors.newSingleThreadExecutor()
    }) { case (dataSource, executor) =>
      IO.blocking {
        dataSource.close()
        executor.shutdown()
      }
    }

  private def seedTemporaryTable(
      dataSource: HikariDataSource
  ): List[(SyncEventHydrationCandidate, String)] =
    val connection = dataSource.getConnection
    try
      val select = connection.prepareStatement(
        """SELECT dedupe_key, stream_name, observed_at, payload_ref
          |FROM octopus_core.sync_events e
          |WHERE payload_archived = 0
          |  AND stream_name = 'wireless.audit'
          |  AND (payload IS NULL OR event_type IS NULL OR schema_version IS NULL
          |       OR sensor_id IS NULL OR wireless_search_text IS NULL)
          |ORDER BY observed_at, dedupe_key
          |LIMIT 20""".stripMargin
      )
      val result = select.executeQuery()
      val candidates = List.newBuilder[SyncEventHydrationCandidate]
      while result.next() do
        candidates += SyncEventHydrationCandidate(
          result.getString(1),
          result.getString(2),
          result.getTimestamp(3),
          result.getString(4),
          None
        )
      result.close()
      select.close()
      val candidateList = candidates.result()
      if candidateList.isEmpty then fail("expected production hydration candidates")

      val setup = connection.createStatement()
      setup.execute("CREATE TEMPORARY TABLE sync_events_seed LIKE octopus_core.sync_events")
      val copy = connection.prepareStatement(
        """INSERT INTO sync_events_seed
          |SELECT * FROM octopus_core.sync_events
          |WHERE payload_archived = 0
          |  AND stream_name = 'wireless.audit'
          |  AND (payload IS NULL OR event_type IS NULL OR schema_version IS NULL
          |       OR sensor_id IS NULL OR wireless_search_text IS NULL)
          |ORDER BY observed_at, dedupe_key
          |LIMIT 20""".stripMargin
      )
      copy.executeUpdate()
      copy.close()
      setup.execute("CREATE TEMPORARY TABLE sync_events LIKE octopus_core.sync_events")
      setup.execute("INSERT INTO sync_events SELECT * FROM sync_events_seed")
      setup.close()

      val resolver = new TidbPayloadResolver("/unused")
      candidateList.map(candidate => candidate -> resolver.resolvePayload(candidate.payloadRef))
    finally connection.close()
