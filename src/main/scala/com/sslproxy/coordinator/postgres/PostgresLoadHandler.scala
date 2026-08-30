package com.sslproxy.coordinator.postgres

import cats.effect.IO
import io.circe.Json
import com.sslproxy.coordinator.observability.StructuredLogger

class PostgresLoadHandler(
  payloadResolver: PostgresPayloadResolver,
  transformService: PostgresTransformService.type,
  sink: PostgresSink,
  clock: PostgresClock.type,
  payloadLookup: String => IO[Option[String]]
):
  import PostgresLoadHandler.log

  def handle(load: PostgresLoad): IO[PostgresResult] =
    val finishedAt = clock.nowRfc3339
    (for
      resolved <- repairPayloadRefIfNeeded(load)
      _ <- validateLoad(resolved)
      target <- resolveTarget(resolved)
      payload <- resolvePayload(resolved)
      rows <- parseRows(target, payload)
      _ <- IO(
        log.info(
          "postgres_load",
          "status" -> "parsed",
          "batch_id" -> load.batchId,
          "stream_name" -> load.streamName,
          "input_rows" -> rows.length.toString
        )
      )
      result <- transformAndInsert(resolved, target, rows)
      _ <- result match
        case Right(rowCount) =>
          IO(
            log.info(
              "postgres_load",
              "status" -> "inserted",
              "batch_id" -> load.batchId,
              "stream_name" -> load.streamName,
              "result_status" -> "success",
              "row_count" -> rowCount.toString
            )
          )
        case Left(_) => IO.unit
      checksum = PostgresChecksum.checksum(target, payload)
      finalResult = result match
        case Left(err) => err
        case Right(rowCount) =>
          if rowCount > Int.MaxValue then
            PostgresResult.failure(
              resolved.jobId,
              resolved.batchId,
              PostgresErrorClass.Permanent,
              "inserted row count exceeds i32 limit",
              finishedAt
            )
          else PostgresResult.success(resolved.jobId, resolved.batchId, rowCount.toInt, checksum, finishedAt)
    yield finalResult).handleError { err =>
      val errorClass = classifyError(err)
      log.error(
        "postgres_load",
        err,
        "status" -> "failed",
        "batch_id" -> load.batchId,
        "stream_name" -> load.streamName,
        "error_class" -> errorClass.wireValue
      )
      PostgresResult.failure(load.jobId, load.batchId, errorClass, err.getMessage, finishedAt)
    }

  private def resolveTarget(load: PostgresLoad): IO[PostgresSinkTarget] =
    PostgresSinkTarget.fromStreamName(load.streamName) match
      case Some(target) => IO.pure(target)
      case None => IO.raiseError(IllegalArgumentException(s"unsupported stream_name ${load.streamName}"))

  private def resolvePayload(load: PostgresLoad): IO[String] =
    val ref = load.payloadRef
    if ref.startsWith("sha256://") then
      val sha = ref.substring("sha256://".length())
      payloadLookup(sha).flatMap {
        case Some(payload) => IO.pure(payload)
        case None =>
          IO.raiseError(new IllegalArgumentException(s"payload_ref sha256 lookup returned no result for $sha"))
      }
    else IO.blocking(payloadResolver.resolvePayload(ref))

  private def parseRows(target: PostgresSinkTarget, payload: String): IO[List[Json]] =
    IO.blocking(payloadResolver.payloadRows(target, payload))

  private def transformAndInsert(
    load: PostgresLoad,
    target: PostgresSinkTarget,
    rows: List[Json]
  ): IO[Either[PostgresResult, Long]] =
    val transformed = transformService.transform(target, rows)
    val rowCount = transformed.inputRowCount(target)
    if rowCount == 0 then IO.pure(Right(0L))
    else
      val insertIO: IO[Long] = target match
        case PostgresSinkTarget.ProxyEvents =>
          sink.insertProxyEvents(load.batchId, transformed.proxyEvents, transformed.blockedEvents)
        case PostgresSinkTarget.ProxyPayloadAudit =>
          sink.insertProxyPayloadAudit(load.batchId, transformed.proxyPayloadAudit)
        case PostgresSinkTarget.WirelessAuditFrames =>
          sink.insertWirelessAuditFrames(load.batchId, transformed.wirelessAuditFrames)
        case PostgresSinkTarget.WirelessBandwidth =>
          sink.insertWirelessBandwidth(load.batchId, transformed.wirelessBandwidth)
        case PostgresSinkTarget.WirelessRogueAp =>
          sink.insertWirelessRogueAp(load.batchId, transformed.wirelessRogueAp)
        case PostgresSinkTarget.WirelessDeauthFlood =>
          sink.insertWirelessDeauthFlood(load.batchId, transformed.wirelessDeauthFlood)
        case PostgresSinkTarget.WirelessSignalAnomaly =>
          sink.insertWirelessSignalAnomaly(load.batchId, transformed.wirelessSignalAnomaly)
        case PostgresSinkTarget.WirelessPmfAttack =>
          sink.insertWirelessPmfAttack(load.batchId, transformed.wirelessPmfAttack)
        case PostgresSinkTarget.WirelessClientInventory =>
          sink.insertWirelessClientInventory(load.batchId, transformed.wirelessClientInventory)
        case PostgresSinkTarget.WirelessProbeRequests =>
          sink.insertWirelessProbeRequests(load.batchId, transformed.wirelessProbeRequests)
        case PostgresSinkTarget.WirelessAttackSequence =>
          sink.insertWirelessAttackSequence(load.batchId, transformed.wirelessAttackSequence)
        case PostgresSinkTarget.WirelessSequenceAlert =>
          sink.insertWirelessSequenceAlert(load.batchId, transformed.wirelessSequenceAlert)
        case PostgresSinkTarget.WirelessHandshakeAlert =>
          sink.insertWirelessHandshakeAlert(load.batchId, transformed.wirelessHandshakeAlert)

      insertIO.attempt.map {
        case Right(count) => Right(count)
        case Left(err) =>
          log.error(
            "postgres_load",
            err,
            "status" -> "insert_failed",
            "batch_id" -> load.batchId,
            "stream_name" -> load.streamName,
            "error_class" -> classifyError(err).wireValue
          )
          Left(buildFailureResult(load, err))
      }

  private def buildFailureResult(load: PostgresLoad, err: Throwable): PostgresResult =
    PostgresResult.failure(load.jobId, load.batchId, classifyError(err), err.getMessage, clock.nowRfc3339)

  private def repairPayloadRefIfNeeded(load: PostgresLoad): IO[PostgresLoad] =
    IO(validateLoadMetadata(load)) *>
      (if load.payloadRef.nonEmpty then IO.pure(load)
       else
         IO.raiseError(
           IllegalArgumentException(
             "payload_ref must not be empty (repair from database not available in standalone PostgreSQL sink)"
           )
         )
      )

  private def validateLoad(load: PostgresLoad): IO[Unit] =
    IO(validateLoadMetadata(load)) *>
      (if load.payloadRef.isBlank then IO.raiseError(IllegalArgumentException("payload_ref must not be empty"))
       else IO.unit)

  private def validateLoadMetadata(load: PostgresLoad): Unit =
    if load.jobId.isBlank then throw IllegalArgumentException("job_id must not be empty")
    if load.batchId.isBlank then throw IllegalArgumentException("batch_id must not be empty")
    if load.streamName.isBlank then throw IllegalArgumentException("stream_name must not be empty")

  private def classifyError(err: Throwable): PostgresErrorClass =
    err match
      case _: PostgresPayloadReadException => PostgresErrorClass.Retryable
      case _: IllegalArgumentException => PostgresErrorClass.Permanent
      case _: io.circe.ParsingFailure => PostgresErrorClass.Permanent
      case _ => PostgresErrorClass.classify(err)

object PostgresLoadHandler:
  private val log = StructuredLogger(getClass)
