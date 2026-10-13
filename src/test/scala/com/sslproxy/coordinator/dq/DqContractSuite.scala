package com.sslproxy.coordinator.dq

import io.circe.Json
import io.circe.parser.parse
import munit.FunSuite
import java.nio.charset.StandardCharsets

class DqContractSuite extends FunSuite:
  private def json(raw: String): Json = parse(raw).fold(throw _, identity)

  test("dispatch pages preserve an ordered prefix and stay inside the actual UTF8 HTTP byte limit") {
    val text = new String(Character.toChars(0x1f600)).repeat(10000)
    val items =
      List.range(0, 100).map(index => Json.obj("index" -> Json.fromInt(index), "manifest" -> Json.fromString(text)))
    val page = DqJson.dispatchPage(items)
    val selected = page.hcursor.get[Vector[Json]]("items").fold(throw _, identity)
    assert(selected.size > 1 && selected.size < items.size)
    assertEquals(selected, items.take(selected.size).toVector)
    assert(page.noSpaces.getBytes(StandardCharsets.UTF_8).length <= DqJson.DispatchPageBytes)
    val next = Json.obj("items" -> Json.fromValues(items.take(selected.size + 1)))
    assert(next.noSpaces.getBytes(StandardCharsets.UTF_8).length > DqJson.DispatchPageBytes)
    assertEquals(DqJson.dispatchPage(Nil), Json.obj("items" -> Json.arr()))
  }

  test("artifact HTTP connect, read, write and overall calls expire below the coordination lease") {
    List(10, 60, 3600).foreach { lease =>
      val client = DqArtifacts.httpClient(lease)
      assert(client.connectTimeoutMillis > 0 && client.connectTimeoutMillis < lease * 1000)
      assert(client.readTimeoutMillis > 0 && client.readTimeoutMillis < lease * 1000)
      assert(client.writeTimeoutMillis > 0 && client.writeTimeoutMillis < lease * 1000)
      assert(client.callTimeoutMillis > 0 && client.callTimeoutMillis < lease * 1000)
      assert(!client.retryOnConnectionFailure)
      client.dispatcher.executorService.shutdown()
      client.connectionPool.evictAll()
    }
  }

  test("canonical numbers retain exact integer precision and survive PostgreSQL JSONB normalization") {
    List("1e3" -> "1000", "1.00" -> "1", "-0" -> "0", "1.2300e-2" -> "0.0123", "9007199254740993" -> "9007199254740993")
      .foreach { case (raw, expected) =>
        assertEquals(DqJson.canonical(json(raw)), expected)
        assertEquals(DqJson.sha256(json(raw)), DqJson.sha256(json(expected)))
        assertEquals(DqArtifacts.digest(json(raw)), DqArtifacts.digest(json(expected)))
      }
  }

  test("unsupported or malformed schema constraints fail registration instead of weakening validation") {
    List(
      """{"type":["string","null"]}""",
      """{"type":"unknown"}""",
      """{"properties":[]}""",
      """{"required":"id"}""",
      """{"required":["id","id"]}""",
      """{"additionalProperties":{}}""",
      """{"minimum":"3"}""",
      """{"minItems":-1}""",
      """{"pattern":true}""",
      """{"oneOf":[]}""",
      """{"enum":[]}"""
    ).foreach { raw =>
      intercept[DqError](DqJson.checkSchema(json(raw)))
    }
  }

  test("canonical Unicode matches Go key ordering and line separator escaping") {
    val high = new String(Character.toChars(0x1f600))
    val low = new String(Character.toChars(0xe000))
    val separators = new String(Character.toChars(0x2028)) + new String(Character.toChars(0x2029))
    val value = Json.obj(high -> Json.fromInt(2), low -> Json.fromInt(1), "text" -> Json.fromString(separators))
    assertEquals(DqJson.canonical(value), "{\"text\":\"\\u2028\\u2029\",\"" + low + "\":1,\"" + high + "\":2}")
    assertEquals(DqJson.canonical(Json.fromString("\\u2028")), "\"\\\\u2028\"")
  }

  test("supported schema validation counts Unicode code points and accepts exact large integers") {
    val stringRule = json("""{"type":"string","maxLength":1}""")
    val oneCodePoint = Json.fromString(new String(Character.toChars(0x1f600)))
    DqJson.checkSchema(stringRule)
    DqJson.validate(stringRule, oneCodePoint)
    val _ = intercept[DqError](DqJson.validate(stringRule, Json.fromString("ab")))
    DqJson.validate(json("""{"type":"integer"}"""), json("999999999999999999999999999999"))
    intercept[DqError](DqJson.validate(json("""{"type":"integer"}"""), json("1.1")))
  }

  test("worker records quarantine malformed UTF8 and cross-installation keys before offset acknowledgement") {
    val config = DqConfig(installationId = "test", namespace = "default")
    val envelope = json("""{"event_id":"event","installation_id":"test","namespace":"default","run_id":"run"}""")
    val good = DqRuntime.receipt(
      config,
      "group",
      "dq.worker-reports.v1",
      0,
      1,
      "test/default/run",
      envelope.noSpaces.getBytes(StandardCharsets.UTF_8)
    )
    assert(good.hcursor.downField("envelope").focus.isDefined)
    val badKey = DqRuntime.receipt(
      config,
      "group",
      "dq.worker-reports.v1",
      0,
      1,
      "other/default/run",
      envelope.noSpaces.getBytes(StandardCharsets.UTF_8)
    )
    assertEquals(badKey.hcursor.get[String]("quarantine_reason").toOption, Some("invalid_record"))
    val badUtf8 = DqRuntime.receipt(
      config,
      "group",
      "dq.worker-reports.v1",
      0,
      2,
      "test/default/run",
      Array(0xc3.toByte, 0x28.toByte)
    )
    assertEquals(badUtf8.hcursor.get[String]("quarantine_reason").toOption, Some("invalid_record"))
  }

  test("shadow parity aggregates repeated source pairs with the same maximum confidence as legacy") {
    val snapshot =
      json("""{"pairs":[{"mac_a":"a","mac_b":"b","confidence":0.90},{"mac_a":"a","mac_b":"b","confidence":0.95}]}""")
    val proposal = DqProposal(
      "run",
      "attempt",
      1L,
      "prepare_candidates",
      "revision",
      "policy",
      Vector(DqCandidateProposal("a", "b", 0.95, "pair")),
      Vector.empty,
      DqProposalSummary(1, 0)
    )
    val comparison = DqApply.shadowComparison(snapshot, proposal)
    assertEquals(comparison.hcursor.get[Int]("confidence_differences").toOption, Some(0))
    assertEquals(comparison.hcursor.get[Int]("legacy_candidate_count").toOption, Some(1))
  }
