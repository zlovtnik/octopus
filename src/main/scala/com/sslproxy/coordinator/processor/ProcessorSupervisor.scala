package com.sslproxy.coordinator.processor

import cats.data.EitherT
import cats.effect.{Clock, IO, Ref}
import cats.syntax.all.*
import cats.syntax.traverse.*
import com.sslproxy.coordinator.config.ProcessorConfig
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.persistence.ProcessorStateStore
import com.sslproxy.coordinator.tidb.TidbErrorClass
import fs2.Stream
import com.sslproxy.coordinator.observability.StructuredLogger
import com.sslproxy.coordinator.observability.CoordinatorMetrics

import scala.concurrent.duration.*

enum ProcessorLifecycle(val value: String):
  case Disabled extends ProcessorLifecycle("disabled")
  case Starting extends ProcessorLifecycle("starting")
  case Ready extends ProcessorLifecycle("ready")
  case BackingOff extends ProcessorLifecycle("backing_off")
  case FailedTerminal extends ProcessorLifecycle("failed_terminal")

enum ProcessorRunStatus(val value: String):
  case Running extends ProcessorRunStatus("running")
  case Retrying extends ProcessorRunStatus("retrying")
  case FailedTerminal extends ProcessorRunStatus("failed_terminal")
  case Cancelled extends ProcessorRunStatus("cancelled")

final case class ProcessorStatus(
    lifecycle: ProcessorLifecycle,
    restartCount: Int,
    lastError: Option[String]
)

final case class ProcessorWorkload(id: ProcessorId, stream: Stream[IO, Unit], startup: IO[Unit] = IO.unit)

final class TerminalProcessorError(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)

final class ProcessorReadiness private[processor] (
    statusesRef: Ref[IO, Map[ProcessorId, ProcessorStatus]]
):
  def statuses: IO[Map[ProcessorId, ProcessorStatus]] = statusesRef.get

  def ready: IO[Boolean] =
    statusesRef.get.map(_.values.forall { status =>
      status.lifecycle == ProcessorLifecycle.Ready ||
      status.lifecycle == ProcessorLifecycle.Disabled
    })

final class ProcessorSupervisor private (
    config: ProcessorConfig,
    enabled: Set[ProcessorId],
    statusesRef: Ref[IO, Map[ProcessorId, ProcessorStatus]],
    stateStore: ProcessorStateStore[IO],
    metrics: Option[CoordinatorMetrics]
):
  import ProcessorSupervisor.log

  val readiness: ProcessorReadiness = ProcessorReadiness(statusesRef)

  def run(workloads: List[ProcessorWorkload]): Stream[IO, Unit] =
    val duplicateIds = workloads.groupMapReduce(_.id)(_ => 1)(_ + _).collect {
      case (id, count) if count > 1 => id.value
    }.toList.sorted
    val byId = workloads.iterator.map(workload => workload.id -> workload).toMap
    val missing = enabled.diff(byId.keySet).toList.sortBy(_.value)

    if duplicateIds.nonEmpty then
      Stream.raiseError[IO](IllegalArgumentException(
        s"duplicate processor workloads: ${duplicateIds.mkString(",")}" 
      ))
    else if missing.nonEmpty then
      Stream.raiseError[IO](IllegalArgumentException(
        s"enabled processors have no workload: ${missing.map(_.value).mkString(",")}" 
      ))
    else
      val selected = workloads.filter(workload => enabled.contains(workload.id))
      Stream.emits(selected).map(supervise).parJoinUnbounded

  private def supervise(workload: ProcessorWorkload): Stream[IO, Unit] =
    Stream.eval(runForever(workload, 0))

  private def runForever(workload: ProcessorWorkload, restartCount: Int): IO[Unit] =
    cats.Monad[IO].tailRecM[Int, Unit](restartCount) { currentRestartCount =>
      runCycle(workload, currentRestartCount).attempt.flatMap {
        case Right(error) =>
          backoff(workload, currentRestartCount, error, supervisionFailure = false)
            .as(Left(currentRestartCount + 1): Either[Int, Unit])
        case Left(error) =>
          backoff(workload, currentRestartCount, error, supervisionFailure = true)
            .as(Left(currentRestartCount + 1): Either[Int, Unit])
      }
    }

  private def runCycle(workload: ProcessorWorkload, restartCount: Int): IO[Throwable] =
    val runId = java.util.UUID.randomUUID().toString
    val execute =
      (setStatus(workload.id, ProcessorLifecycle.Starting, restartCount, None) *>
        workload.startup *>
        setStatus(workload.id, ProcessorLifecycle.Ready, restartCount, None) *>
        workload.stream.compile.drain)
        .onCancel(bestEffortFinishRun(workload.id, runId, ProcessorRunStatus.Cancelled, None))

    val cycle = Clock[IO].realTimeInstant.flatMap { startedAt =>
      persist(stateStore.startRun(workload.id, runId, startedAt))
    } *> execute.attempt.flatMap {
        case Right(_) =>
          val error = IllegalStateException("processor stream completed unexpectedly")
          bestEffortFinishRun(workload.id, runId, ProcessorRunStatus.Retrying, Some(error)) *>
            IO.pure(error)
        case Left(error: TerminalProcessorError) =>
          failTerminal(workload, runId, restartCount, error)
        case Left(error: ProcessorPersistenceException) =>
          bestEffortFinishRun(workload.id, runId, ProcessorRunStatus.Retrying, Some(error)) *>
            IO.pure(error)
        case Left(error) if TidbErrorClass.classify(error) == TidbErrorClass.Permanent =>
          failTerminal(workload, runId, restartCount, error)
        case Left(error) =>
          bestEffortFinishRun(workload.id, runId, ProcessorRunStatus.Retrying, Some(error)) *>
            IO.pure(error)
      }

    cycle

  private def failTerminal(
      workload: ProcessorWorkload,
      runId: String,
      restartCount: Int,
      error: Throwable
  ): IO[Throwable] =
          val message = safeMessage(error)
          bestEffortFinishRun(workload.id, runId, ProcessorRunStatus.FailedTerminal, Some(error)) *>
            setStatus(workload.id, ProcessorLifecycle.FailedTerminal, restartCount, Some(message)) *>
            IO(log.error("processor",
              "status" -> "failed_terminal", "processor" -> workload.id.value,
              "error" -> message)) *> IO.never

  private def backoff(
      workload: ProcessorWorkload,
      restartCount: Int,
      error: Throwable,
      supervisionFailure: Boolean
  ): IO[Unit] =
    val nextRestart = restartCount + 1
    val delay = ProcessorSupervisor.retryDelay(
      config.restartBaseDelayMs,
      config.restartMaxDelayMs,
      nextRestart,
      ProcessorSupervisor.jitterFraction(workload.id, nextRestart)
    )
    val message = safeMessage(error)
    val event = if supervisionFailure then "supervision_retry" else "backing_off"

    IO(metrics.foreach(_.recordProcessorRetry(workload.id.value))) *>
      setStatus(workload.id, ProcessorLifecycle.BackingOff, nextRestart, Some(message))
        .handleErrorWith(statusError => logBookkeepingFailure(workload.id, "set_status", statusError)) *>
      IO(log.warn("processor",
        "status" -> event, "processor" -> workload.id.value,
        "restart_count" -> nextRestart.toString, "delay_ms" -> delay.toMillis.toString,
        "error" -> message)) *>
      IO.sleep(delay)

  private def setStatus(
      id: ProcessorId,
      lifecycle: ProcessorLifecycle,
      restartCount: Int,
      lastError: Option[String]
  ): IO[Unit] =
    val status = ProcessorStatus(lifecycle, restartCount, lastError)
    Clock[IO].realTimeInstant.flatMap { observedAt =>
      persist(stateStore.persist(id, status, observedAt)) *>
        statusesRef.update(_.updated(id, status)) *>
        IO(metrics.foreach(_.recordProcessorState(id.value, lifecycle.value, restartCount)))
    }

  private def finishRun(
      runId: String,
      status: ProcessorRunStatus,
      error: Option[Throwable]
  ): IO[Unit] =
    Clock[IO].realTimeInstant.flatMap { finishedAt =>
      persist(stateStore.finishRun(
        runId,
        status,
        error.map(_.getClass.getSimpleName),
        error.map(safeMessage),
        finishedAt
      ))
    }

  private def bestEffortFinishRun(
      id: ProcessorId,
      runId: String,
      status: ProcessorRunStatus,
      error: Option[Throwable]
  ): IO[Unit] =
    finishRun(runId, status, error).handleErrorWith { finishError =>
      logBookkeepingFailure(id, "finish_run", finishError)
    }

  private def logBookkeepingFailure(
      id: ProcessorId,
      operation: String,
      error: Throwable
  ): IO[Unit] =
    IO(log.error(
      "processor_bookkeeping",
      error,
      "status" -> "failed",
      "processor" -> id.value,
      "operation" -> operation
    ))

  private def persist[A](result: EitherT[IO, DatabaseError, A]): IO[A] =
    result.value.flatMap {
      case Right(value) => IO.pure(value)
      case Left(error) => IO.raiseError(ProcessorPersistenceException(error))
    }

  private def safeMessage(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)

object ProcessorSupervisor:
  private val log = StructuredLogger(getClass)

  def create(config: ProcessorConfig): IO[ProcessorSupervisor] =
    create(config, VolatileProcessorStateStore, None)

  def create(
      config: ProcessorConfig,
      stateStore: ProcessorStateStore[IO]
  ): IO[ProcessorSupervisor] =
    create(config, stateStore, None)

  def create(
      config: ProcessorConfig,
      stateStore: ProcessorStateStore[IO],
      metrics: Option[CoordinatorMetrics]
  ): IO[ProcessorSupervisor] =
    for
      enabled <- IO.fromEither(
        config.enabled.traverse(ProcessorId.fromString).map(_.toSet).left.map(IllegalArgumentException(_))
      )
      _ <- IO.fromEither(validateDependencies(enabled))
      persisted <- liftPersistence(stateStore.load)
      initial = (ProcessorId.octopusOwned.toSet ++ enabled).iterator.map { id =>
        val lifecycle = if enabled.contains(id) then ProcessorLifecycle.Starting else ProcessorLifecycle.Disabled
        val previous = persisted.get(id)
        id -> ProcessorStatus(
          lifecycle,
          previous.fold(0)(_.restartCount),
          previous.flatMap(_.lastError)
        )
      }.toMap
      statuses <- Ref.of[IO, Map[ProcessorId, ProcessorStatus]](initial)
      supervisor = ProcessorSupervisor(config, enabled, statuses, stateStore, metrics)
      now <- Clock[IO].realTimeInstant
      _ <- initial.toList.traverse_ { case (id, status) =>
        supervisor.persist(stateStore.persist(id, status, now)) *>
          IO(metrics.foreach(_.recordProcessorState(id.value, status.lifecycle.value, status.restartCount)))
      }
    yield supervisor

  private def liftPersistence[A](result: EitherT[IO, DatabaseError, A]): IO[A] =
    result.value.flatMap {
      case Right(value) => IO.pure(value)
      case Left(error) => IO.raiseError(ProcessorPersistenceException(error))
    }

  private def validateDependencies(enabled: Set[ProcessorId]): Either[IllegalArgumentException, Unit] =
    val missing = enabled.toList.flatMap { id =>
      ProcessorCatalog.byId(id).dependencies.filter { dependency =>
        dependency.owner == ProcessorOwner.Octopus && !enabled.contains(dependency)
      }.map(dependency => s"${id.value}->${dependency.value}")
    }.sorted
    Either.cond(
      missing.isEmpty,
      (),
      IllegalArgumentException(s"enabled processors have disabled dependencies: ${missing.mkString(",")}")
    )

  private object VolatileProcessorStateStore extends ProcessorStateStore[IO]:
    def load = EitherT.rightT[IO, DatabaseError](Map.empty[ProcessorId, ProcessorStatus])
    def persist(id: ProcessorId, status: ProcessorStatus, observedAt: java.time.Instant) =
      EitherT.rightT[IO, DatabaseError](())
    def startRun(id: ProcessorId, runId: String, startedAt: java.time.Instant) =
      EitherT.rightT[IO, DatabaseError](())
    def finishRun(
        runId: String,
        status: ProcessorRunStatus,
        errorClass: Option[String],
        errorText: Option[String],
        finishedAt: java.time.Instant
    ) = EitherT.rightT[IO, DatabaseError](())

  private[processor] def retryDelay(
      baseDelayMs: Long,
      maxDelayMs: Long,
      attempt: Int,
      jitterFraction: Double
  ): FiniteDuration =
    val exponent = (attempt.max(1) - 1).min(30)
    val uncapped = BigInt(baseDelayMs.max(1L)) * BigInt(2).pow(exponent)
    val capped = uncapped.min(BigInt(maxDelayMs.max(baseDelayMs))).toLong
    val boundedJitter = jitterFraction.max(-0.2d).min(0.2d)
    Math.max(1L, Math.round(capped.toDouble * (1.0d + boundedJitter))).millis

  private def jitterFraction(id: ProcessorId, attempt: Int): Double =
    val bucket = Math.floorMod(id.value.hashCode * 31 + attempt, 401)
    (bucket.toDouble - 200.0d) / 1000.0d

private final case class ProcessorPersistenceException(error: DatabaseError)
    extends RuntimeException(s"${error.operation}: ${error.message}", error.cause)
