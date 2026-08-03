package com.sslproxy.coordinator.processor

import cats.effect.{IO, Ref}
import cats.syntax.traverse.*
import com.sslproxy.coordinator.config.ProcessorConfig
import com.sslproxy.coordinator.tidb.TidbErrorClass
import fs2.Stream
import com.sslproxy.coordinator.observability.StructuredLogger

import scala.concurrent.duration.*

enum ProcessorLifecycle(val value: String):
  case Disabled extends ProcessorLifecycle("disabled")
  case Starting extends ProcessorLifecycle("starting")
  case Ready extends ProcessorLifecycle("ready")
  case BackingOff extends ProcessorLifecycle("backing_off")
  case FailedTerminal extends ProcessorLifecycle("failed_terminal")

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
    statusesRef: Ref[IO, Map[ProcessorId, ProcessorStatus]]
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
    (setStatus(workload.id, ProcessorLifecycle.Starting, restartCount, None) *>
      workload.startup *>
      setStatus(workload.id, ProcessorLifecycle.Ready, restartCount, None) *>
      workload.stream.compile.drain).attempt.flatMap {
        case Right(_) =>
          retry(workload, restartCount, IllegalStateException("processor stream completed unexpectedly"))
        case Left(error: TerminalProcessorError) =>
          val message = safeMessage(error)
          setStatus(workload.id, ProcessorLifecycle.FailedTerminal, restartCount, Some(message)) *>
            IO(log.error("processor",
              "status" -> "failed_terminal", "processor" -> workload.id.value,
              "error" -> message)) *> IO.never
        case Left(error) if TidbErrorClass.classify(error) == TidbErrorClass.Permanent =>
          val message = safeMessage(error)
          setStatus(workload.id, ProcessorLifecycle.FailedTerminal, restartCount, Some(message)) *>
            IO(log.error("processor",
              "status" -> "failed_terminal", "processor" -> workload.id.value,
              "error" -> message)) *> IO.never
        case Left(error) => retry(workload, restartCount, error)
      }

  private def retry(
      workload: ProcessorWorkload,
      restartCount: Int,
      error: Throwable
  ): IO[Unit] =
    val nextRestart = restartCount + 1
    val delay = ProcessorSupervisor.retryDelay(
      config.restartBaseDelayMs,
      config.restartMaxDelayMs,
      nextRestart,
      ProcessorSupervisor.jitterFraction(workload.id, nextRestart)
    )
    val message = safeMessage(error)

    setStatus(workload.id, ProcessorLifecycle.BackingOff, nextRestart, Some(message)) *>
      IO(log.warn("processor",
        "status" -> "backing_off", "processor" -> workload.id.value,
        "restart_count" -> nextRestart.toString, "delay_ms" -> delay.toMillis.toString,
        "error" -> message)) *>
      IO.sleep(delay) *>
      runForever(workload, nextRestart)

  private def setStatus(
      id: ProcessorId,
      lifecycle: ProcessorLifecycle,
      restartCount: Int,
      lastError: Option[String]
  ): IO[Unit] =
    statusesRef.update(_.updated(id, ProcessorStatus(lifecycle, restartCount, lastError)))

  private def safeMessage(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)

object ProcessorSupervisor:
  private val log = StructuredLogger(getClass)

  def create(config: ProcessorConfig): IO[ProcessorSupervisor] =
    for
      enabled <- IO.fromEither(
        config.enabled.traverse(ProcessorId.fromString).map(_.toSet).left.map(IllegalArgumentException(_))
      )
      initial = ProcessorId.octopusOwned.iterator.map { id =>
        val lifecycle = if enabled.contains(id) then ProcessorLifecycle.Starting else ProcessorLifecycle.Disabled
        id -> ProcessorStatus(lifecycle, 0, None)
      }.toMap
      statuses <- Ref.of[IO, Map[ProcessorId, ProcessorStatus]](initial)
    yield ProcessorSupervisor(config, enabled, statuses)

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
