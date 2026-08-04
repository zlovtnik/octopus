package com.sslproxy.coordinator.domain

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.{Decoder, HCursor}
import io.circe.parser.{decode, parse}

import java.nio.charset.StandardCharsets
import java.time.Instant

/** Locked source record for a `sync.scan.request`-shaped ingest request.
  *
  * `sourceRecordSha256` hashes the broker record whose coordinates are written
  * to ingestion evidence. It is deliberately distinct from the referenced
  * event payload hash.
  */
final case class ScanRequestRecord(
    requestJson: String,
    sourceRecordSha256: String,
    streamName: String,
    dedupeKey: String,
    observedAt: String,
    payloadRef: String
)

object ScanRequestRecord:
  private final case class Wire(
      streamName: String,
      dedupeKey: String,
      payloadRef: String,
      observedAt: String
  )

  private given Decoder[Wire] = (cursor: HCursor) =>
    for
      streamName <- cursor.downField("stream_name").as[String]
      dedupeKey <- cursor.downField("dedupe_key").as[String]
      payloadRef <- cursor.downField("payload_ref").as[String]
      observedAt <- cursor.downField("observed_at").as[String]
    yield Wire(streamName, dedupeKey, payloadRef, observedAt)

  def decodeWire(rawJson: String): Either[Throwable, ScanRequestRecord] =
    decode[Wire](rawJson).flatMap { wire =>
      if wire.streamName.isBlank then Left(IllegalArgumentException("scan request stream_name must not be empty"))
      else if wire.dedupeKey.isBlank then Left(IllegalArgumentException("scan request dedupe_key must not be empty"))
      else if wire.payloadRef.isBlank then Left(IllegalArgumentException("scan request payload_ref must not be empty"))
      else if !isRfc3339( wire.observedAt) then Left(IllegalArgumentException("scan request observed_at must be RFC3339"))
      else
        val sourceRecordSha256 = Sha256Utils.sha256Hex(rawJson.getBytes(StandardCharsets.UTF_8))
        Right(
          ScanRequestRecord(
            requestJson = rawJson,
            sourceRecordSha256 = sourceRecordSha256,
            streamName = wire.streamName,
            dedupeKey = wire.dedupeKey,
            observedAt = wire.observedAt,
            payloadRef = wire.payloadRef
          )
        )
    }

  private def isRfc3339(value: String): Boolean =
    try
      Instant.parse(value)
      true
    catch case _: java.time.format.DateTimeParseException => false

/** A scan request whose referenced event payload has been resolved and
  * validated as non-null JSON.
  */
final case class ResolvedScanRequestRecord(
    source: ScanRequestRecord,
    payloadJson: String,
    eventPayloadSha256: String
):
  export source.{
    requestJson,
    sourceRecordSha256,
    streamName,
    dedupeKey,
    observedAt,
    payloadRef
  }

object ResolvedScanRequestRecord:
  def from(
      source: ScanRequestRecord,
      payloadJson: String
  ): Either[Throwable, ResolvedScanRequestRecord] =
    parse(payloadJson).left.map(identity[Throwable]).flatMap {
      case io.circe.Json.Null =>
        Left(IllegalArgumentException("resolved scan event payload must not be JSON null"))
      case _ =>
        Right(
          ResolvedScanRequestRecord(
            source = source,
            payloadJson = payloadJson,
            eventPayloadSha256 =
              Sha256Utils.sha256Hex(payloadJson.getBytes(StandardCharsets.UTF_8))
          )
        )
    }
