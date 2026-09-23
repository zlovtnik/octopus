package com.sslproxy.coordinator.postgres

import io.circe.Json
import munit.FunSuite

class StreamingPayloadRowsSuite extends FunSuite:
  test("top-level arrays retain nested structures and escaped strings") {
    val payload = """[{"v":"a\\b\"c","nested":[1,2]},{"v":2}]"""
    scala.util.Using.resource(StreamingPayloadRows.open(PostgresSinkTarget.ProxyEvents, payload, 1024)) { rows =>
      assertEquals(rows.toList, io.circe.parser.parse(payload).toOption.get.asArray.get.toList)
    }
  }

  test("client metadata after the array is merged into every row") {
    val payload = """{"clients":[{"sensor_id":"old","id":1},{"id":2}],"sensor_id":"parent","observed_at":"now"}"""
    scala.util.Using.resource(StreamingPayloadRows.open(PostgresSinkTarget.WirelessClientInventory, payload, 1024)) { rows =>
      val result = rows.toList
      assertEquals(result.size, 2)
      result.foreach { row =>
        assertEquals(row.hcursor.get[String]("sensor_id"), Right("parent"))
        assertEquals(row.hcursor.get[String]("snapshot_at"), Right("now"))
      }
    }
  }

  test("probe envelope ignores non-row fields") {
    scala.util.Using.resource(StreamingPayloadRows.open(PostgresSinkTarget.WirelessProbeRequests,
      """{"probes":[{"id":1}],"other":[1,2]}""", 1024)) { rows =>
      assertEquals(rows.toList, List(Json.obj("id" -> Json.fromInt(1))))
    }
  }

  test("malformed tail is rejected before yielding any rows") {
    intercept[IllegalArgumentException] {
      StreamingPayloadRows.open(PostgresSinkTarget.ProxyEvents, """[{"id":1},oops]""", 1024)
    }
  }

  test("oversized later row is checked only when pulled") {
    val payload = "[{\"id\":1},{\"large\":\"" + "x".repeat(2048) + "\"}]"
    scala.util.Using.resource(StreamingPayloadRows.open(PostgresSinkTarget.ProxyEvents, payload, 1024)) { rows =>
      assertEquals(rows.next(), Json.obj("id" -> Json.fromInt(1)))
      intercept[IllegalArgumentException](rows.next())
    }
  }
