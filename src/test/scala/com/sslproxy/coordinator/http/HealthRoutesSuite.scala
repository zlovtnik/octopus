package com.sslproxy.coordinator.http

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.sslproxy.coordinator.config.{PostgresConfig, ProcessorConfig}
import com.sslproxy.coordinator.observability.CoordinatorMetrics
import com.sslproxy.coordinator.postgres.PostgresTransactor
import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorReadiness, ProcessorSupervisor}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.circe.parser.parse
import munit.CatsEffectSuite
import org.http4s.implicits.*
import org.http4s.{Method, Request, Status, Uri}

import scala.concurrent.duration.*

class HealthRoutesSuite extends CatsEffectSuite:
  private val unavailableDatabaseConfig = PostgresConfig(
    host = "127.0.0.1",
    port = 1,
    database = "octopus",
    user = "test",
    password = "test",
    poolSize = 1,
    healthcheckReserve = 0,
    connectionTimeoutMs = 250,
    statementTimeoutSecs = 1,
    enabled = true,
    warnOnly = false,
    sslMode = "disable"
  )

  private def routes(
    processorReadiness: Option[ProcessorReadiness] = None,
    metrics: CoordinatorMetrics = CoordinatorMetrics()
  ): Resource[IO, HealthRoutes] =
    Resource
      .make(
        IO.blocking {
          val hikariConfig = HikariConfig()
          hikariConfig.setJdbcUrl(PostgresTransactor.jdbcUrl(unavailableDatabaseConfig))
          hikariConfig.setUsername(unavailableDatabaseConfig.user)
          hikariConfig.setPassword(unavailableDatabaseConfig.password)
          hikariConfig.setMaximumPoolSize(unavailableDatabaseConfig.poolSize)
          hikariConfig.setConnectionTimeout(unavailableDatabaseConfig.connectionTimeoutMs)
          hikariConfig.setInitializationFailTimeout(-1)
          val dataSource = HikariDataSource(hikariConfig)
          val transactor = PostgresTransactor.fromDataSource(dataSource, unavailableDatabaseConfig)
          (HealthRoutes(transactor, metrics, processorReadiness, 1.second), dataSource)
        }
      ) { case (_, dataSource) => IO.blocking(dataSource.close()) }
      .map(_._1)

  private def get(routes: HealthRoutes, path: Uri): IO[(Status, String)] =
    routes.routes.orNotFound.run(Request[IO](Method.GET, path)).flatMap { response =>
      response.bodyText.compile.string.map(response.status -> _)
    }

  test("completed database health checks remain healthy"):
    HealthRoutes.withTimeout(IO.pure(true), 5.millis).map { healthy =>
      assertEquals(healthy, true)
    }

  test("stalled database health checks time out as unhealthy"):
    HealthRoutes.withTimeout(IO.never, 5.millis).map { healthy =>
      assertEquals(healthy, false)
    }

  test("raised database health checks are reported as unhealthy"):
    HealthRoutes.withTimeout(IO.raiseError(RuntimeException("database unavailable")), 5.millis).map { healthy =>
      assertEquals(healthy, false)
    }

  test("live returns UP without a database check"):
    routes().use { healthRoutes =>
      get(healthRoutes, uri"/live").map { case (status, body) =>
        assertEquals(status, Status.Ok)
        assertEquals(parse(body).toOption.flatMap(_.hcursor.get[String]("status").toOption), Some("UP"))
      }
    }

  test("ready reports an unavailable database as DOWN"):
    routes().use { healthRoutes =>
      get(healthRoutes, uri"/ready").map { case (status, body) =>
        val cursor = parse(body).toOption.map(_.hcursor)
        assertEquals(status, Status.ServiceUnavailable)
        assertEquals(cursor.flatMap(_.get[String]("status").toOption), Some("DOWN"))
        assertEquals(
          cursor.flatMap(_.downField("components").downField("postgres").get[String]("status").toOption),
          Some("DOWN")
        )
        assertEquals(
          cursor.flatMap(_.downField("components").downField("processors").get[String]("status").toOption),
          Some("UP")
        )
      }
    }

  test("ready reports processors that have not started as DOWN"):
    ProcessorSupervisor
      .create(ProcessorConfig(List(ProcessorId.SyncScanIngestion.value), 1L, 10L))
      .flatMap { supervisor =>
        routes(Some(supervisor.readiness)).use { healthRoutes =>
          get(healthRoutes, uri"/ready").map { case (status, body) =>
            val cursor = parse(body).toOption.map(_.hcursor)
            assertEquals(status, Status.ServiceUnavailable)
            assertEquals(
              cursor.flatMap(_.downField("components").downField("processors").get[String]("status").toOption),
              Some("DOWN")
            )
          }
        }
      }

  test("metrics exposes the Prometheus scrape on both metric endpoints"):
    val metrics = CoordinatorMetrics()
    metrics.recordIngestProcessed(3)
    routes(metrics = metrics).use { healthRoutes =>
      List(uri"/metrics", uri"/actuator/prometheus").traverse { path =>
        get(healthRoutes, path).map { case (status, body) =>
          assertEquals(status, Status.Ok)
          assert(body.contains("coordinator_ingest_processed_total_count 3.0"), body)
        }
      }
    }
