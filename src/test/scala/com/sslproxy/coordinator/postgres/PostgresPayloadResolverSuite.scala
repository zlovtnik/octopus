package com.sslproxy.coordinator.postgres

import com.sslproxy.coordinator.domain.ScanRequestRecord
import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.Json
import munit.FunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64

class PostgresPayloadResolverSuite extends FunSuite:

  test("resolved scan request keeps source and event hashes distinct"):
    val payload = " \n{\"event_type\":\"wifi_data_frame\",\"sensor_id\":\"sensor-1\",\"text\":\"\u20ac\"}\t"
    val payloadRef =
      "inline://json/" + Base64.getUrlEncoder.withoutPadding
        .encodeToString(payload.getBytes(StandardCharsets.UTF_8))
    val requestJson = Json
      .obj(
        "stream_name" -> Json.fromString("wireless.audit"),
        "dedupe_key" -> Json.fromString(Sha256Utils.sha256Hex(payload)),
        "payload_ref" -> Json.fromString(payloadRef),
        "observed_at" -> Json.fromString("2026-07-27T12:00:00Z")
      )
      .noSpaces

    val source = ScanRequestRecord.decodeWire(requestJson).fold(throw _, identity)
    val resolved = new PostgresPayloadResolver("/unused").resolve(source)

    assertEquals(source.sourceRecordSha256, Sha256Utils.sha256Hex(requestJson))
    assertEquals(resolved.eventPayloadSha256, Sha256Utils.sha256Hex(payload))
    assertNotEquals(resolved.sourceRecordSha256, resolved.eventPayloadSha256)
    assertEquals(resolved.payloadJson, payload)
    assertEquals(resolved.inspection.map(_.eventKind), Right(Some("wifi_data_frame")))

  test("resolver reads an outbox payload without allowing path escape"):
    val directory = Files.createTempDirectory("octopus-hydration-resolver-")
    val payload = " \n{\"event_type\":\"proxy_event\",\"ok\":true,\"text\":\"\u20ac\"}\t"
    val payloadFile = directory.resolve("event.json")
    Files.writeString(payloadFile, payload)
    val resolver = new PostgresPayloadResolver(directory.toString)

    assertEquals(resolver.resolvePayload("outbox://event.json"), payload)
    val source = ScanRequestRecord("request", "source-hash", "proxy.events", "dedupe", "now", "outbox://event.json")
    val resolved = resolver.resolve(source)
    assertEquals(resolved.payloadJson, payload)
    assertEquals(resolved.eventPayloadSha256, Sha256Utils.sha256Hex(payload))
    assertEquals(resolved.inspection.map(_.eventKind), Right(Some("proxy_event")))
    intercept[IllegalArgumentException] {
      resolver.resolvePayload("outbox://../event.json")
    }

  test("both resolver entry points reject invalid inline and outbox JSON"):
    val directory = Files.createTempDirectory("octopus-invalid-resolver-")
    val resolver = new PostgresPayloadResolver(directory.toString)
    List("", "null", "{} {}", "[1,broken]").foreach { payload =>
      Files.writeString(directory.resolve("event.json"), payload): Unit
      val inline = "inline://json/" + Base64.getUrlEncoder.withoutPadding
        .encodeToString(payload.getBytes(StandardCharsets.UTF_8))
      List(inline, "outbox://event.json").foreach { ref =>
        val source = ScanRequestRecord("request", "hash", "proxy.events", "dedupe", "now", ref)
        intercept[IllegalArgumentException](resolver.resolvePayload(ref)): Unit
        intercept[IllegalArgumentException](resolver.resolve(source)): Unit
      }
    }
