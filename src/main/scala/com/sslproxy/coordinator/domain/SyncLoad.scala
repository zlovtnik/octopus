package com.sslproxy.coordinator.domain

import io.circe.{Decoder, Encoder, HCursor, Json}

final case class SyncLoad(
  jobId: String,
  batchId: String,
  batchNo: Option[Int],
  streamName: String,
  payloadRef: String,
  cursorStart: String,
  cursorEnd: String,
  attempt: Int
)

object SyncLoad:
  given Decoder[SyncLoad] = (c: HCursor) =>
    for
      jobId <- c.downField("job_id").as[String]
      batchId <- c.downField("batch_id").as[String]
      batchNo <- c.downField("batch_no").as[Option[Int]]
      streamName <- c.downField("stream_name").as[String]
      payloadRef <- c.downField("payload_ref").as[String]
      cursorStart <- c.downField("cursor_start").as[String]
      cursorEnd <- c.downField("cursor_end").as[String]
      attempt <- c.downField("attempt").as[Option[Int]].map(_.getOrElse(0))
    yield SyncLoad(jobId, batchId, batchNo, streamName, payloadRef, cursorStart, cursorEnd, attempt)

  given Encoder[SyncLoad] = (load: SyncLoad) =>
    Json.obj(
      "job_id" -> Json.fromString(load.jobId),
      "batch_id" -> Json.fromString(load.batchId),
      "batch_no" -> load.batchNo.fold(Json.Null)(n => Json.fromInt(n)),
      "stream_name" -> Json.fromString(load.streamName),
      "payload_ref" -> Json.fromString(load.payloadRef),
      "cursor_start" -> Json.fromString(load.cursorStart),
      "cursor_end" -> Json.fromString(load.cursorEnd),
      "attempt" -> Json.fromInt(load.attempt)
    )
