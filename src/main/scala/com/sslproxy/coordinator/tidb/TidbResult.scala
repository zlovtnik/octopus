package com.sslproxy.coordinator.tidb

import io.circe.{Decoder, DecodingFailure, Encoder, HCursor, Json, JsonObject}

final case class TidbResult(
    jobId: String,
    batchId: String,
    status: String,
    rowCount: Int,
    checksum: String,
    retryable: Boolean,
    errorClass: String,
    errorText: String,
    finishedAt: String
)

object TidbResult:
  given Decoder[TidbResult] = (cursor: HCursor) =>
    for
      jobId <- cursor.downField("job_id").as[String]
      batchId <- cursor.downField("batch_id").as[String]
      status <- cursor.downField("status").as[String]
      rowCount <- cursor.downField("row_count").as[Int]
      checksum <- cursor.downField("checksum").as[Option[String]].map(_.getOrElse(""))
      errorClass <- cursor.downField("error_class").as[Option[String]].map(_.getOrElse(""))
      retryableValue <- cursor.downField("retryable").as[Option[Boolean]]
      retryableDerived = errorClass == TidbErrorClass.Retryable.wireValue
      retryable <- retryableValue match
        case Some(value) if value != retryableDerived =>
          Left(DecodingFailure(
            s"retryable=$value conflicts with error_class=$errorClass",
            cursor.history
          ))
        case Some(value) => Right(value)
        case None        => Right(retryableDerived)
      errorText <- cursor.downField("error_text").as[Option[String]].map(_.getOrElse(""))
      finishedAt <- cursor.downField("finished_at").as[String]
    yield TidbResult(
      jobId,
      batchId,
      status,
      rowCount,
      checksum,
      retryable,
      errorClass,
      errorText,
      finishedAt
    )

  given Encoder[TidbResult] = (r: TidbResult) =>
    Json.fromJsonObject(JsonObject(
      "job_id"      -> Json.fromString(r.jobId),
      "batch_id"    -> Json.fromString(r.batchId),
      "status"      -> Json.fromString(r.status),
      "row_count"   -> Json.fromInt(r.rowCount),
      "checksum"    -> Json.fromString(r.checksum),
      "retryable"   -> Json.fromBoolean(r.retryable),
      "error_class" -> Json.fromString(r.errorClass),
      "error_text"  -> Json.fromString(r.errorText),
      "finished_at" -> Json.fromString(r.finishedAt)
    ))

  def success(jobId: String, batchId: String, rowCount: Int, checksum: String, finishedAt: String): TidbResult =
    TidbResult(
      jobId = jobId,
      batchId = batchId,
      status = "success",
      rowCount = rowCount,
      checksum = checksum,
      retryable = false,
      errorClass = "",
      errorText = "",
      finishedAt = finishedAt
    )

  def failure(
      jobId: String,
      batchId: String,
      errorClass: TidbErrorClass,
      errorText: String,
      finishedAt: String
  ): TidbResult =
    TidbResult(
      jobId = if jobId == null then "" else jobId,
      batchId = if batchId == null then "" else batchId,
      status = "failed",
      rowCount = 0,
      checksum = "",
      retryable = errorClass == TidbErrorClass.Retryable,
      errorClass = errorClass.wireValue,
      errorText = if errorText == null then "" else errorText,
      finishedAt = finishedAt
    )
