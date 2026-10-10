package com.sslproxy.coordinator.wiring

import cats.effect.{IO, Resource}
import cats.effect.std.Semaphore
import com.sslproxy.coordinator.config.{PostgresConfig, WirelessProjectionConfig}
import com.sslproxy.coordinator.postgres.*
import com.sslproxy.coordinator.postgres.sql.{DispatchAuthorizationSql, IngestionSql}
import doobie.Transactor
import doobie.implicits.*

import java.util.concurrent.{Executors, ThreadFactory}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

private[coordinator] final case class DatabaseRuntime(
    transactor: PostgresTransactor,
    doobieTx: Transactor[IO],
    dbSemaphore: Semaphore[IO],
    repository: PostgresRepository,
    ingestionStore: PostgresIngestionStore,
    outboxStore: PostgresOutboxStore,
    projectionStore: PostgresProjectionStore,
    maintenanceStore: PostgresMaintenanceStore,
    resultStore: PostgresResultStore,
    processorStateStore: PostgresProcessorStateStore,
    loadHandler: PostgresLoadHandler,
    payloadResolver: PostgresPayloadResolver,
    payloadLookup: String => IO[Option[String]],
    preflight: PostgresSchemaPreflight
)

private[coordinator] object DatabaseModule:
  def acquire(
      postgres: PostgresConfig,
      projection: WirelessProjectionConfig,
      outboxDir: String,
      metrics: Option[com.sslproxy.coordinator.observability.CoordinatorMetrics] = None
  ): Resource[IO, DatabaseRuntime] =
    for
      blockingEc <- blockingExecutionContext(postgres.poolSize)
      transactor <- PostgresTransactor.resource(postgres)
      // A non-owning Doobie view of the sole Hikari pool.
      doobieTx = Transactor.fromDataSource[IO](transactor.dataSource, blockingEc)
      preflight = new PostgresSchemaPreflight(transactor, postgres)
      _ <- Resource.eval(preflight.validate())
      dbSemaphore <- Resource.eval(Semaphore[IO](
        dbWorkerPermits(postgres.poolSize, postgres.healthcheckReserve)
      ))
    yield
      val repository = new PostgresRepository(doobieTx, Some(dbSemaphore), projection, metrics)
      val payloadResolver = new PostgresPayloadResolver(outboxDir)
      val payloadLookup: String => IO[Option[String]] = sha =>
        IngestionSql.payloadBySha256(sha).unique.transact(doobieTx).attempt.map(_.toOption)
      val loadHandler = new PostgresLoadHandler(
        payloadResolver,
        PostgresTransformService,
        transactor,
        PostgresClock,
        payloadLookup,
        load => DispatchAuthorizationSql.authorizeLoad(load).transact(doobieTx),
        insertChunkBytes = postgres.loadChunkMaxBytes
      )
      DatabaseRuntime(
        transactor,
        doobieTx,
        dbSemaphore,
        repository,
        new PostgresIngestionStore(repository),
        new PostgresOutboxStore(repository),
        new PostgresProjectionStore(repository),
        new PostgresMaintenanceStore(repository),
        new PostgresResultStore(repository),
        new PostgresProcessorStateStore(doobieTx, Some(dbSemaphore)),
        loadHandler,
        payloadResolver,
        payloadLookup,
        preflight
      )

  private[coordinator] def blockingExecutionContext(
      poolSize: Int
  ): Resource[IO, ExecutionContextExecutorService] =
    Resource.make(IO {
      ExecutionContext.fromExecutorService(
        Executors.newFixedThreadPool(
          poolSize,
          new ThreadFactory:
            def newThread(r: Runnable): Thread =
              val thread = new Thread(r, "doobie-postgres-pool")
              thread.setDaemon(true)
              thread
        )
      )
    })(ec => IO.blocking(ec.shutdown()))

  private[coordinator] def dbWorkerPermits(
      poolSize: Int,
      healthcheckReserve: Int
  ): Long =
    // Schema introspection and health checks use the transactor directly.
    // Keep the configured capacity available when admitted worker traffic is busy.
    (poolSize - healthcheckReserve).max(1).toLong
