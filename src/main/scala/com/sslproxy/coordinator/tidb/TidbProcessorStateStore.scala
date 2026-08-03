package com.sslproxy.coordinator.tidb

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.observability.StructuredLogger
import com.sslproxy.coordinator.persistence.{DbResultT, ProcessorStateStore}
import com.sslproxy.coordinator.processor.{ProcessorId, ProcessorLifecycle, ProcessorRunStatus, ProcessorStatus}
import com.sslproxy.coordinator.tidb.sql.ProcessorStateSql
import doobie.*
import doobie.implicits.*

import java.time.Instant

final class TidbProcessorStateStore(xa: Transactor[IO]) extends ProcessorStateStore[IO]:
  import TidbProcessorStateStore.log

  def load: DbResultT[IO, Map[ProcessorId, ProcessorStatus]] =
    run("tidb.processor_state.load") {
      ProcessorStateSql.LoadStatesQuery.to[List].map { rows =>
        rows.flatMap { case (name, databaseStatus, failures, error) =>
          val parsed = for
            id <- ProcessorId.fromString(name).toOption
            lifecycle <- lifecycle(databaseStatus)
          yield id -> ProcessorStatus(lifecycle, failures, error)

          if parsed.isEmpty then
            log.warn(
              "processor_state_load",
              "status" -> "invalid_row",
              "processor_name" -> name,
              "lifecycle" -> databaseStatus
            )
          parsed
        }.toMap
      }
    }

  private def lifecycle(databaseStatus: String): Option[ProcessorLifecycle] =
    databaseStatus match
      case "disabled" => Some(ProcessorLifecycle.Disabled)
      case "running"  => Some(ProcessorLifecycle.Starting)
      case "idle"     => Some(ProcessorLifecycle.Ready)
      case "degraded" => Some(ProcessorLifecycle.BackingOff)
      case "failed"   => Some(ProcessorLifecycle.FailedTerminal)
      case _          => None

  def persist(id: ProcessorId, status: ProcessorStatus, observedAt: Instant): DbResultT[IO, Unit] =
    run("tidb.processor_state.persist") {
      ProcessorStateSql.persistState(id, status, observedAt).run.void
    }

  def startRun(id: ProcessorId, runId: String, startedAt: Instant): DbResultT[IO, Unit] =
    run("tidb.processor_run.start") {
      ProcessorStateSql.startRun(id, runId, startedAt).run.void
    }

  def finishRun(
      runId: String,
      status: ProcessorRunStatus,
      errorClass: Option[String],
      errorText: Option[String],
      finishedAt: Instant
  ): DbResultT[IO, Unit] =
    run("tidb.processor_run.finish") {
      ProcessorStateSql.finishRun(runId, status, errorClass, errorText, finishedAt).run.flatMap { updated =>
        if updated == 1 then ().pure[ConnectionIO]
        else FC.raiseError(IllegalStateException(s"processor run $runId is not running"))
      }
    }

  private def run[A](operation: String)(action: ConnectionIO[A]): DbResultT[IO, A] =
    EitherT(
      TidbRepository.retryTransient(operation)(action.transact(xa))
        .map(Right(_))
        .handleError { cause =>
          log.error("db_error", cause, "operation" -> operation)
          TidbErrorClass.classify(cause) match
            case TidbErrorClass.Retryable => Left(DatabaseError.Retryable(operation, cause, cause.getMessage))
            case TidbErrorClass.Permanent => Left(DatabaseError.Permanent(operation, cause, cause.getMessage))
        }
    )

object TidbProcessorStateStore:
  private val log = StructuredLogger(getClass)
