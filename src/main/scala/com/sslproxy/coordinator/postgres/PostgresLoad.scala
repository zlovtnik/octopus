package com.sslproxy.coordinator.postgres

import io.circe.{Decoder, HCursor}

final case class PostgresLoad(
  jobId: String,
  batchId: String,
  batchNo: Option[Int],
  streamName: String,
  payloadRef: String,
  cursorStart: String,
  cursorEnd: String,
  attempt: Int
)

object PostgresLoad:
  given Decoder[PostgresLoad] = (c: HCursor) =>
    for
      jobId <- c.downField("job_id").as[String]
      batchId <- c.downField("batch_id").as[String]
      batchNo <- c.downField("batch_no").as[Option[Int]]
      streamName <- c.downField("stream_name").as[String]
      payloadRef <- c.downField("payload_ref").as[String]
      cursorStart <- c.downField("cursor_start").as[String]
      cursorEnd <- c.downField("cursor_end").as[String]
      attempt <- c.downField("attempt").as[Option[Int]].map(_.getOrElse(0))
    yield PostgresLoad(jobId, batchId, batchNo, streamName, payloadRef, cursorStart, cursorEnd, attempt)
