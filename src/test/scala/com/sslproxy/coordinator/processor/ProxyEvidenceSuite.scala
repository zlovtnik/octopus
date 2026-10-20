package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import munit.FunSuite
import java.sql.Timestamp
import java.time.Instant

class ProxyEvidenceSuite extends FunSuite:
  test("frozen producer envelopes retain source identities across duplicate delivery"):
    val resource = scala.io.Source.fromResource("contracts/proxy-events-v1.json")
    val corpus = try io.circe.parser.parse(resource.mkString).toOption.get finally resource.close()
    val rows = corpus.hcursor.downField("observations").focus.get.asArray.get.toList.map { observation =>
      io.circe.parser.parse(observation.hcursor.get[String]("raw_json").toOption.get).toOption.get
    }
    val transformed = com.sslproxy.coordinator.postgres.PostgresTransformService.transform(
      com.sslproxy.coordinator.postgres.PostgresSinkTarget.ProxyEvents, rows).proxyEvents
    assertEquals(transformed.map(_.eventId).distinct.size, 8)
    assertEquals(transformed.head.eventId, transformed.last.eventId)
    assertEquals(transformed.map(_.eventType).toSet, ProxyEvidence.AllowedEvents)
  private val observed = Timestamp.from(Instant.parse("2026-10-19T12:00:00Z"))
  private val row = Json.obj("host" -> "ChatGPT.com:443".asJson, "type" -> "http_proxied".asJson,
    "classification" -> "essential_api".asJson, "bytes_up" -> 0.asJson, "bytes_down" -> 0.asJson,
    "device_id" -> "sensitive-registration".asJson, "peer_ip" -> "192.0.2.4".asJson,
    "wg_pubkey" -> "secret-key".asJson, "client_ua" -> "identifying-user-agent".asJson)

  test("all allowlisted events retain deterministic identity without embedding identities"):
    ProxyEvidence.AllowedEvents.foreach { event =>
      val payload = row.deepMerge(Json.obj("type" -> event.asJson))
      val source = ProxyEvidence.source(payload, "event-id", 1L, observed).get
      val first = SearchDocumentPreparation.prepare(source).toOption.get
      val replay = SearchDocumentPreparation.prepare(source).toOption.get
      assertEquals(first, replay)
      assertEquals(first.host, Some("ChatGPT.com:443"))
      List("sensitive-registration", "192.0.2.4", "secret-key", "identifying-user-agent").foreach { value =>
        assert(!first.normalizedText.contains(value)); assert(!first.detailJson.contains(value))
      }
    }

  test("legacy zero is unavailable; measured terminal zero is explicit"):
    val legacy = ProxyEvidence.normalized(row)
    assertEquals(legacy.hcursor.downField("measurement").get[String]("bytes_status"), Right("legacy_unknown"))
    assertEquals(legacy.hcursor.downField("measurement").get[Option[Long]]("bytes_up"), Right(None))
    val modern = row.deepMerge(Json.obj("evidence_version" -> ProxyEvidence.Version.asJson,
      "measurement" -> Json.obj("version" -> "proxy-measurement-v1".asJson, "bytes_status" -> "measured_terminal".asJson)))
    assertEquals(ProxyEvidence.normalized(modern).hcursor.downField("measurement").get[Long]("bytes_up"), Right(0L))

  test("sanitized and truncated authorities cannot acquire exact grouping fields"):
    List(row.deepMerge(Json.obj("sanitization_status" -> "sanitized".asJson)),
      row.deepMerge(Json.obj("authority_truncated" -> true.asJson))).foreach { value =>
      val evidence = ProxyEvidence.normalized(value)
      assertEquals(evidence.hcursor.get[Option[String]]("host_normalized"), Right(None))
      assertEquals(evidence.hcursor.get[Option[String]]("registrable_domain"), Right(None))
      val public = ProxyEvidence.publicEvidence(evidence)
      assert(!public.noSpaces.contains("ChatGPT"))
    }

  test("same semantic text and changed evidence keep document identity"):
    val source = ProxyEvidence.source(row, "source", 1L, observed).get
    val first = SearchDocumentPreparation.prepare(source).toOption.get
    val correction = SearchDocumentPreparation.prepare(source.copy(evidenceRevision = Some(2L), detailJson = "{\"correction\":true}")).toOption.get
    assertEquals(first.documentId, correction.documentId)
    assertEquals(first.normalizedSha256, correction.normalizedSha256)
    assertNotEquals(first.evidenceRevision, correction.evidenceRevision)

  test("proxy event v2 text is a stable privacy-safe golden template"):
    val document = SearchDocumentPreparation.prepare(ProxyEvidence.source(row, "golden", 1L, observed).get).toOption.get
    assertEquals(document.normalizedText,
      "proxy event http_proxied chatgpt.com essential_api allowed observation transfer legacy_unknown connection duration unavailable")

  test("proxy-only processor has no wireless dependency or shared preparation path"):
    assertEquals(ProcessorCatalog.byId(ProcessorId.ProxySearchPreparer).dependencies, Nil)
    assert(!com.sslproxy.coordinator.postgres.sql.SearchPreparationSql.supportedKinds.exists(_.sourceTable == "proxy_events"))
