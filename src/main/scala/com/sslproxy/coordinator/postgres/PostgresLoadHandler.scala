package com.sslproxy.coordinator.postgres

import cats.effect.IO
import com.sslproxy.coordinator.observability.StructuredLogger
import fs2.{Chunk, Stream}
import io.circe.Json

import java.nio.charset.StandardCharsets
import scala.collection.BufferedIterator

class PostgresLoadHandler(
  payloadResolver: PostgresPayloadResolver,
  transformService: PostgresTransformService.type,
  sink: PostgresSink,
  clock: PostgresClock.type,
  payloadLookup: String => IO[Option[String]],
  insertChunkRows: Int = 500,
  insertChunkBytes: Int = 4 * 1024 * 1024
):
  import PostgresLoadHandler.log
  require(insertChunkRows > 0 && insertChunkBytes > 0, "insert chunk bounds must be positive")

  def handle(load: PostgresLoad): IO[PostgresResult] =
    val finishedAt = clock.nowRfc3339
    (for
      resolved <- repairPayloadRefIfNeeded(load)
      _ <- validateLoad(resolved)
      target <- resolveTarget(resolved)
      payload <- resolvePayload(resolved)
      _ <- IO(
        log.info(
          "postgres_load",
          "status" -> "streaming",
          "batch_id" -> load.batchId,
          "stream_name" -> load.streamName
        )
      )
      result <- transformAndInsert(resolved, target, payload)
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
      PostgresResult.failure(
        load.jobId,
        load.batchId,
        errorClass,
        com.sslproxy.coordinator.util.ErrorSanitizer.message(err),
        finishedAt
      )
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

  private def transformAndInsert(
    load: PostgresLoad,
    target: PostgresSinkTarget,
    payload: String
  ): IO[Either[PostgresResult, Long]] =
    val insertIO = sink.withLoadTransaction { transaction =>
      Stream
        .bracket(IO.blocking(StreamingPayloadRows.open(target, payload, insertChunkBytes)))(rows =>
          IO.blocking(rows.close())
        )
        .flatMap(rows => rowChunks(rows.buffered))
        .evalMapAccumulate(0L) { case (rowOffset, chunk) =>
          val rows = chunk.toList
          insertChunk(transaction, load, target, rows, rowOffset)
            .map(inserted => (rowOffset + rows.size, inserted))
        }
        .map(_._2)
        .compile
        .fold(0L)(_ + _)
    }

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

  private def insertChunk(
    transaction: PostgresLoadTransaction,
    load: PostgresLoad,
    target: PostgresSinkTarget,
    rows: List[Json],
    rowOffset: Long
  ): IO[Long] =
    val transformed = transformService.transform(target, rows, rowOffset)
    transaction.insertChunk(load.batchId, target, transformed, rowOffset)

  private def rowChunks(rows: BufferedIterator[Json]): Stream[IO, Chunk[Json]] =
    Stream.unfoldEval(rows) { iterator =>
      IO.blocking {
        if !iterator.hasNext then None
        else
          val builder = Vector.newBuilder[Json]
          var count = 0
          var bytes = 0L
          var full = false
          while iterator.hasNext && count < insertChunkRows && !full do
            val row = iterator.head
            val rowBytes = row.noSpaces.getBytes(StandardCharsets.UTF_8).length
            if rowBytes > insertChunkBytes then
              throw IllegalArgumentException(
                s"single PostgreSQL row is $rowBytes bytes; limit is $insertChunkBytes"
              )
            if count > 0 && bytes + rowBytes > insertChunkBytes then full = true
            else
              builder += iterator.next()
              count += 1
              bytes += rowBytes
          Some((Chunk.from(builder.result()), iterator))
      }
    }

  private def buildFailureResult(load: PostgresLoad, err: Throwable): PostgresResult =
    PostgresResult.failure(
      load.jobId,
      load.batchId,
      classifyError(err),
      com.sslproxy.coordinator.util.ErrorSanitizer.message(err),
      clock.nowRfc3339
    )

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
