package com.sslproxy.coordinator.postgres

import com.sslproxy.coordinator.domain.{ResolvedScanRequestRecord, ScanRequestRecord}
import io.circe.Json
import io.circe.JsonObject

import java.nio.charset.StandardCharsets
import java.util.Base64

/** Resolves payload_ref strings to JSON and extracts row arrays per sink target. */
class PostgresPayloadResolver(syncOutboxDir: String):

  def resolve(record: ScanRequestRecord): ResolvedScanRequestRecord =
    ResolvedScanRequestRecord.from(record, resolvePayload(record.payloadRef))
      .fold(throw _, identity)

  def resolvePayload(payloadRef: String): String =
    val ref = if payloadRef == null then "" else payloadRef
    if ref.startsWith("inline://json/") then
      val encoded = ref.substring("inline://json/".length())
      val bytes   = Base64.getUrlDecoder.decode(paddedBase64(encoded))
      val payload = new String(bytes, StandardCharsets.UTF_8)
      validateJson(payload)
      payload
    else if ref.startsWith("outbox://") then
      resolveOutbox(ref.substring("outbox://".length()))
    else
      throw new IllegalArgumentException(s"unsupported payload_ref scheme: $ref")

  def payloadRows(target: PostgresSinkTarget, payload: String): List[Json] =
    val value = parseJson(payload)
    target match
      case PostgresSinkTarget.WirelessProbeRequests =>
        val probes = value.hcursor.downField("probes").focus
          .getOrElse(throw new IllegalArgumentException("wireless probe payload must contain a probes array"))
        probes.asArray.getOrElse(
          throw new IllegalArgumentException("wireless probe payload: 'probes' must be an array")
        ).toList
      case PostgresSinkTarget.WirelessClientInventory =>
        val clients = value.hcursor.downField("clients").focus
          .getOrElse(throw new IllegalArgumentException("wireless client inventory payload must contain a clients array"))
        val arr = clients.asArray.getOrElse(
          throw new IllegalArgumentException("wireless client inventory: 'clients' must be an array")
        )
        mergeClientInventory(value, arr)
      case _ =>
        value.asArray.getOrElse(Vector(value)).toList

  /** Merges parent-level sensor_id/location_id/snapshot_at into each client object. */
  private def mergeClientInventory(payload: Json, clients: Vector[Json]): List[Json] =
    val sensorId = payload.hcursor.downField("sensor_id").focus
    val locationId = payload.hcursor.downField("location_id").focus
    val snapshotAt = payload.hcursor.downField("snapshot_at").focus
      .orElse(payload.hcursor.downField("observed_at").focus)
    clients.toList.map { client =>
      val obj = client.asObject.getOrElse(
        throw new IllegalArgumentException("wireless client inventory: each client must be a JSON object")
      )
      val parentFields = List(
        sensorId.map("sensor_id" -> _),
        locationId.map("location_id" -> _),
        snapshotAt.map("snapshot_at" -> _)
      ).flatten
      Json.fromJsonObject(parentFields.foldLeft(obj) { case (o, (k, v)) => o.add(k, v) })
    }

  private def resolveOutbox(relativePath: String): String =
    if relativePath.isBlank then throw new IllegalArgumentException("invalid blank outbox path")
    val relative = java.nio.file.Path.of(relativePath)
    if relative.isAbsolute || relative.normalize().startsWith("..") then
      throw new IllegalArgumentException("invalid outbox path escapes base")
    try
      val outboxBase = java.nio.file.Path.of(syncOutboxDir).toRealPath()
      val unresolved = outboxBase.resolve(relative.normalize()).normalize()
      if !unresolved.startsWith(outboxBase) then
        throw new IllegalArgumentException("invalid outbox path escapes base")
      val resolved = unresolved.toRealPath()
      if !resolved.startsWith(outboxBase) then
        throw new IllegalArgumentException("invalid outbox path escapes base")
      val payload = java.nio.file.Files.readString(resolved)
      validateJson(payload)
      payload // returned after successful validation
    catch
      case e: java.io.InterruptedIOException =>
        Thread.currentThread().interrupt()
        throw new PostgresPayloadReadException("outbox payload read interrupted", e)
      case e: java.io.IOException =>
        throw new PostgresPayloadReadException(s"outbox payload read failed: ${e.getMessage}", e)

  private def validateJson(payload: String): Unit = parseJson(payload): Unit

  private def parseJson(payload: String): Json =
    io.circe.parser.parse(payload) match
      case Right(Json.Null) =>
        throw new IllegalArgumentException("payload_ref resolved an empty JSON payload")
      case Right(json)      => json
      case Left(error)      =>
        throw new IllegalArgumentException(s"payload_ref resolved non-JSON payload: $error")

  private def paddedBase64(encoded: String): String =
    val remainder = encoded.length % 4
    if remainder == 0 then encoded else encoded + "=".repeat(4 - remainder)
