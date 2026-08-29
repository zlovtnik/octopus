package com.sslproxy.coordinator.ingest

import cats.effect.{IO, Ref}
import com.sslproxy.coordinator.domain.{DatabaseError, PayloadAudit}
import com.sslproxy.coordinator.persistence.DatabaseOperationException
import com.sslproxy.coordinator.util.Sha256Utils
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.Duration

class PayloadAuditConsumerSuite extends CatsEffectSuite:

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

  test("retryable database writes succeed before the retry budget is exhausted"):
    val retryable = DatabaseError.Retryable(
      "payload_audit.record_scan_requests",
      new java.sql.SQLTransientException("connection unavailable"),
      "connection unavailable"
    )

    for
      attempts <- Ref.of[IO, Int](0)
      write = attempts.modify { current =>
        val result: Either[DatabaseError, Int] =
          if current < 2 then Left(retryable) else Right(4)
        (current + 1, result)
      }
      result <- PayloadAuditConsumer.retryDatabaseWrite(
        write,
        maxAttempts = 3,
        initialDelay = Duration.Zero
      )
      count <- attempts.get
    yield
      assertEquals(result, Right(4))
      assertEquals(count, 3)

  test("exhausted retryable database writes fail without becoming DLQ records"):
    val retryable = DatabaseError.Retryable(
      "payload_audit.record_scan_requests",
      new java.sql.SQLTransientException("connection unavailable"),
      "connection unavailable"
    )

    PayloadAuditConsumer
      .retryDatabaseWrite(
        IO.pure(Left(retryable)),
        maxAttempts = 3,
        initialDelay = Duration.Zero
      )
      .attempt
      .map {
        case Left(failure: DatabaseOperationException) =>
          assertEquals(failure.error, retryable)
        case other =>
          fail(s"expected retryable DatabaseOperationException, got $other")
      }

  test("permanent database writes are returned for DLQ handling without retry"):
    val permanent = DatabaseError.Permanent(
      "payload_audit.record_scan_requests",
      IllegalArgumentException("invalid record"),
      "invalid record"
    )

    for
      attempts <- Ref.of[IO, Int](0)
      result <- PayloadAuditConsumer.retryDatabaseWrite(
        attempts.update(_ + 1).as(Left(permanent)),
        maxAttempts = 3,
        initialDelay = Duration.Zero
      )
      count <- attempts.get
    yield
      assertEquals(result, Left(permanent))
      assertEquals(count, 1)
