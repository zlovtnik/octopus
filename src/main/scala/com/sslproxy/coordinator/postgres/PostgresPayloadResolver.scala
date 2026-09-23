package com.sslproxy.coordinator.postgres

import com.sslproxy.coordinator.domain.{ResolvedScanRequestRecord, ScanRequestRecord}

import java.nio.charset.StandardCharsets
import java.util.Base64

/** Resolves payload_ref strings to JSON and extracts row arrays per sink target. */
class PostgresPayloadResolver(syncOutboxDir: String):

  def resolve(record: ScanRequestRecord): ResolvedScanRequestRecord =
    ResolvedScanRequestRecord
      .from(record, resolvePayload(record.payloadRef))
      .fold(throw _, identity)

  def resolvePayload(payloadRef: String): String =
    val ref = if payloadRef == null then "" else payloadRef
    if ref.startsWith("inline://json/") then
      val encoded = ref.substring("inline://json/".length())
      val bytes = Base64.getUrlDecoder.decode(paddedBase64(encoded))
      val payload = new String(bytes, StandardCharsets.UTF_8)
      validateJson(payload)
      payload
    else if ref.startsWith("outbox://") then resolveOutbox(ref.substring("outbox://".length()))
    else throw new IllegalArgumentException(s"unsupported payload_ref scheme: $ref")

  private def resolveOutbox(relativePath: String): String =
    if relativePath.isBlank then throw new IllegalArgumentException("invalid blank outbox path")
    val relative = java.nio.file.Path.of(relativePath)
    if relative.isAbsolute || relative.normalize().startsWith("..") then
      throw new IllegalArgumentException("invalid outbox path escapes base")
    try
      val outboxBase = java.nio.file.Path.of(syncOutboxDir).toRealPath()
      val unresolved = outboxBase.resolve(relative.normalize()).normalize()
      if !unresolved.startsWith(outboxBase) then throw new IllegalArgumentException("invalid outbox path escapes base")
      val resolved = unresolved.toRealPath()
      if !resolved.startsWith(outboxBase) then throw new IllegalArgumentException("invalid outbox path escapes base")
      val payload = java.nio.file.Files.readString(resolved)
      validateJson(payload)
      payload // returned after successful validation
    catch
      case e: java.io.InterruptedIOException =>
        Thread.currentThread().interrupt()
        throw new PostgresPayloadReadException("outbox payload read interrupted", e)
      case e: java.io.IOException =>
        throw new PostgresPayloadReadException(s"outbox payload read failed: ${e.getMessage}", e)

  private def validateJson(payload: String): Unit = StreamingPayloadRows.validate(payload)

  private def paddedBase64(encoded: String): String =
    val remainder = encoded.length % 4
    if remainder == 0 then encoded else encoded + "=".repeat(4 - remainder)
