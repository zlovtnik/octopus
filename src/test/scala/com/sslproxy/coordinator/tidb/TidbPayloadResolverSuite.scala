package com.sslproxy.coordinator.tidb

import com.sslproxy.coordinator.domain.ScanRequestRecord
import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.Json
import munit.FunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64

class TidbPayloadResolverSuite extends FunSuite:

  test("resolved scan request keeps source and event hashes distinct"):
    val payload = """{"event_type":"wifi_data_frame","sensor_id":"sensor-1"}"""
    val payloadRef =
      "inline://json/" + Base64.getUrlEncoder.withoutPadding
        .encodeToString(payload.getBytes(StandardCharsets.UTF_8))
    val requestJson = Json.obj(
      "stream_name" -> Json.fromString("wireless.audit"),
      "dedupe_key" -> Json.fromString(Sha256Utils.sha256Hex(payload)),
      "payload_ref" -> Json.fromString(payloadRef),
      "observed_at" -> Json.fromString("2026-07-27T12:00:00Z")
    ).noSpaces

    val source = ScanRequestRecord.decodeWire(requestJson).fold(throw _, identity)
    val resolved = new TidbPayloadResolver("/unused").resolve(source)

    assertEquals(source.sourceRecordSha256, Sha256Utils.sha256Hex(requestJson))
    assertEquals(resolved.eventPayloadSha256, Sha256Utils.sha256Hex(payload))
    assertNotEquals(resolved.sourceRecordSha256, resolved.eventPayloadSha256)
    assertEquals(resolved.payloadJson, payload)

  test("resolver reads an outbox payload without allowing path escape"):
    val directory = Files.createTempDirectory("octopus-hydration-resolver-")
    val payload = """{"event_type":"proxy_event","ok":true}"""
    val payloadFile = directory.resolve("event.json")
    Files.writeString(payloadFile, payload)
    val resolver = new TidbPayloadResolver(directory.toString)

    assertEquals(resolver.resolvePayload("outbox://event.json"), payload)
    intercept[IllegalArgumentException] {
      resolver.resolvePayload("outbox://../event.json")
    }
