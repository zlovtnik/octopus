package com.sslproxy.coordinator.kafka

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.Json
import io.circe.parser.parse
import munit.FunSuite

class WirelessHeartbeatStreamSuite extends FunSuite:
  private val heartbeat = Json.obj(
    "sensor_id" -> Json.fromString("sensor-1"),
    "observed_at" -> Json.fromString("2026-10-25T12:00:00Z")
  )

  test("heartbeat defaults retain broker hash separately from the enriched payload"):
    val raw = heartbeat.noSpaces
    val decoded = WirelessHeartbeatStream.decode(raw).toOption.get.record
    val payload = parse(decoded.payloadJson).toOption.get
    assertEquals(decoded.sourceRecordSha256, Sha256Utils.sha256Hex(raw))
    assertEquals(payload.hcursor.get[String]("sensor_id"), Right("sensor-1"))
    assertEquals(payload.hcursor.get[Int]("schema_version"), Right(1))
    assertEquals(payload.hcursor.get[String]("produced_at"), Right("2026-10-25T12:00:00Z"))
    assertEquals(decoded.streamName, "wireless.sensor.heartbeat")

  test("blank sensor identities are poison instead of manufacturing sensor rows"):
    List("", "   ").foreach { sensorId =>
      assert(WirelessHeartbeatStream.decode(heartbeat.mapObject(_.add("sensor_id", Json.fromString(sensorId))).noSpaces).isLeft)
    }

  test("malformed observed and produced timestamps are poison"):
    List("observed_at", "produced_at").foreach { field =>
      assert(WirelessHeartbeatStream.decode(heartbeat.mapObject(_.add(field, Json.fromString("yesterday"))).noSpaces).isLeft)
    }

  test("explicit event identifiers and schema versions stay strict"):
    List(
      heartbeat.mapObject(_.add("event_id", Json.fromInt(17))),
      heartbeat.mapObject(_.add("event_id", Json.fromString(" "))),
      heartbeat.mapObject(_.add("schema_version", Json.fromInt(0))),
      heartbeat.mapObject(_.add("schema_version", Json.fromString("one")))
    ).foreach(payload => assert(WirelessHeartbeatStream.decode(payload.noSpaces).isLeft))
