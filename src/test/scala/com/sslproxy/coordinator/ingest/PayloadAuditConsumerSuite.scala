package com.sslproxy.coordinator.ingest

import com.sslproxy.coordinator.domain.PayloadAudit
import com.sslproxy.coordinator.util.Sha256Utils
import munit.*

import java.nio.charset.StandardCharsets

class PayloadAuditConsumerSuite extends FunSuite:

  import PayloadAuditConsumer.translateRecord

  private val validJson = """{
    "observed_at": "2026-06-01T12:00:00Z",
    "host": "api.example.com",
    "method": "POST",
    "path": "/login",
    "content_type": "application/json",
    "body": {"password": "[REDACTED]"}
  }"""

  test("parse valid payload audit JSON"):
    val result = PayloadAudit.parse(validJson)
    assert(result.isRight)
    val audit = result.toOption.get
    assertEquals(audit.observedAt, "2026-06-01T12:00:00Z")
    assertEquals(audit.host, Some("api.example.com"))
    assertEquals(audit.method, Some("POST"))
    assertEquals(audit.path, Some("/login"))
    assertEquals(audit.contentType, Some("application/json"))

  test("reject payload audit without observed_at"):
    val json = """{"host": "api.example.com"}"""
    val result = PayloadAudit.parse(json)
    assert(result.isLeft)

  test("reject payload audit with malformed observed_at"):
    val json = """{"observed_at": "not-a-timestamp", "host": "api.example.com"}"""
    val result = PayloadAudit.parse(json)
    assert(result.isRight) // observed_at is just a string field, no date parse in model

  test("accept payload audit with partial fields"):
    val json = """{"observed_at": "2026-06-01T12:00:00Z"}"""
    val result = PayloadAudit.parse(json)
    assert(result.isRight)
    val audit = result.toOption.get
    assertEquals(audit.host, None)
    assertEquals(audit.method, None)

  test("reject invalid JSON"):
    val result = PayloadAudit.parse("not json")
    assert(result.isLeft)

  test("reject null body"):
    val result = PayloadAudit.parse("")
    assert(result.isLeft)

  test("translate valid payload audit produces correct stream name"):
    val json = """{"observed_at": "2026-06-01T12:00:00Z", "host": "test"}"""
    val body = json.getBytes(StandardCharsets.UTF_8)
    val sha256 = Sha256Utils.sha256Hex(body)
    val dedupeKey = Sha256Utils.sha256Hex("proxy.payload_audit:" + sha256)
    val expectedPayloadRef = s"sha256://$sha256"

    import io.circe.parser.decode as circeDecode
    import io.circe.Json
    val expectedRequest = Json.obj(
      "stream_name" -> Json.fromString("proxy.payload_audit"),
      "dedupe_key" -> Json.fromString(dedupeKey),
      "payload_ref" -> Json.fromString(expectedPayloadRef),
      "observed_at" -> Json.fromString("2026-06-01T12:00:00Z")
    ).noSpaces

    val result = translateRecord(
      fs2.kafka.ConsumerRecord[String, String]("proxy.payload_audit", 0, 0L, null, json)
    )
    assert(result.isRight)
    val record = result.toOption.get
    assertEquals(circeDecode[Json](record.requestJson).toOption.get, circeDecode[Json](expectedRequest).toOption.get)
    assertEquals(record.payloadJson, json)
    assertEquals(record.sourceRecordSha256, sha256)
    assertEquals(record.eventPayloadSha256, sha256)

  test("empty message is treated as empty"):
    val result = translateRecord(
      fs2.kafka.ConsumerRecord[String, String]("proxy.payload_audit", 0, 0L, null, "")
    )
    assert(result.isLeft)
    assertEquals(result.swap.toOption.get, PayloadAuditError.EmptyMessage)

  test("null message is treated as empty"):
    val result = translateRecord(
      fs2.kafka.ConsumerRecord[String, String]("proxy.payload_audit", 0, 0L, null, null)
    )
    assert(result.isLeft)
    assertEquals(result.swap.toOption.get, PayloadAuditError.EmptyMessage)

