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

  test("legacy host publication excludes unsafe values and preserves safe compatibility hosts"):
    List("user:password@example.com:443", "example.com\tcredential", "example.com\ncredential", "x" * 1025, "\u00e9" * 513).foreach { host =>
      assertEquals(ProxyEvidence.safeHost(Some(host), sanitized = false), None)
    }
    assertEquals(ProxyEvidence.safeHost(Some("private.example"), sanitized = true), None)
    assertEquals(ProxyEvidence.safeHost(Some("hmac:synthetic"), sanitized = true), Some("hmac:synthetic"))
    assertEquals(ProxyEvidence.safeHost(Some("WINDOW.Example:443"), sanitized = false), Some("WINDOW.Example:443"))
    assertEquals(ProxyEvidence.safeHost(None, sanitized = false), None)
    val credentialRow = row.deepMerge(Json.obj("host" -> "user:password@example.com:443".asJson,
      "authority_raw" -> "example.com:443".asJson, "authority_source" -> "connect_authority".asJson))
    val document = SearchDocumentPreparation.prepare(ProxyEvidence.source(credentialRow, "safe-host", 1L, observed).get).toOption.get
    assertEquals(document.host, None)
    assert(!document.detailJson.contains("password"))
    assert(!document.title.exists(_.contains("password")))
    assert(!document.normalizedText.contains("password"))

  test("fallback hosts must parse independently of the normalized authority"):
    val unsafe = List("example.com/path?token=secret", "example.com?token=secret", "example.com#secret",
      "example.com\\secret", "example.com%2Fsecret", "example.com:99999", "hmac:token?secret", "hmac:token/secret")
    unsafe.foreach { host =>
      assertEquals(ProxyEvidence.safeHost(Some(host), sanitized = false), None)
      assertEquals(ProxyEvidence.safeHost(Some(host), sanitized = true), None)
      List(Json.obj(), Json.obj("authority_raw" -> "safe.example:443".asJson,
        "authority_source" -> "connect_authority".asJson)).foreach { authority =>
        val payload = row.deepMerge(Json.obj("host" -> host.asJson)).deepMerge(authority)
        val source = ProxyEvidence.source(payload, "unsafe-host", 1L, observed).get
        val document = SearchDocumentPreparation.prepare(source).toOption.get
        assertEquals(document.host, None)
        assertEquals(io.circe.parser.parse(document.detailJson).toOption.get.hcursor.get[Option[String]]("host"), Right(None))
        assert(!document.title.exists(_.contains(host)))
        assert(!document.tags.exists(_._1 == "host"))
        assert(!document.normalizedText.contains("secret"))
      }
    }
    List("WINDOW.Example:443", "192.0.2.4:443", "[2001:db8::1]:443", "2001:db8::1", "hmac:synthetic", "hmac:deadbeef").foreach { host =>
      assertEquals(ProxyEvidence.safeHost(Some(host), sanitized = false), Some(host))
    }

  test("duration evidence requires both supported envelope versions"):
    val measurement = Json.obj("duration_status" -> "connection_elapsed".asJson)
    val versions = List((None, None), (None, Some("proxy-measurement-v1")),
      (Some("unknown"), Some("proxy-measurement-v1")), (Some(ProxyEvidence.Version), None),
      (Some(ProxyEvidence.Version), Some("unknown")), (Some(ProxyEvidence.Version), Some("proxy-measurement-v1")))
    versions.foreach { case (envelope, version) =>
      val supported = envelope.contains(ProxyEvidence.Version) && version.contains("proxy-measurement-v1")
      val payload = row.deepMerge(Json.obj("evidence_version" -> envelope.asJson, "duration_ms" -> 1234.asJson,
        "measurement" -> measurement.deepMerge(Json.obj("version" -> version.asJson))))
      val normalized = ProxyEvidence.normalized(payload).hcursor.downField("measurement")
      assertEquals(normalized.get[String]("duration_status"), Right(if supported then "connection_elapsed" else "legacy_unknown"))
      assertEquals(normalized.get[Option[Long]]("duration_ms"), Right(Option.when(supported)(1234L)))
      val source = ProxyEvidence.source(payload, "duration", 1L, observed).get
      assertEquals(source.searchText.contains("short connection duration"), supported)
    }

  test("legacy zero is unavailable; measured terminal zero is explicit"):
    val legacy = ProxyEvidence.normalized(row)
    assertEquals(legacy.hcursor.downField("measurement").get[String]("bytes_status"), Right("legacy_unknown"))
    assertEquals(legacy.hcursor.downField("measurement").get[Option[Long]]("bytes_up"), Right(None))
    val modern = row.deepMerge(Json.obj("evidence_version" -> ProxyEvidence.Version.asJson,
      "measurement" -> Json.obj("version" -> "proxy-measurement-v1".asJson, "bytes_status" -> "measured_terminal".asJson)))
    assertEquals(ProxyEvidence.normalized(modern).hcursor.downField("measurement").get[Long]("bytes_up"), Right(0L))

  test("public measurement evidence retains supported source and termination without copying arbitrary values"):
    val versions = List((None, None), (None, Some("proxy-measurement-v1")),
      (Some(ProxyEvidence.Version), None), (Some(ProxyEvidence.Version), Some("unknown")),
      (Some(ProxyEvidence.Version), Some("proxy-measurement-v1")))
    versions.foreach { case (envelope, version) =>
      val supported = envelope.contains(ProxyEvidence.Version) && version.contains("proxy-measurement-v1")
      List("measured_terminal", "partial", "unavailable", "private-device").foreach { termination =>
        List("successful_socket_writes", "unavailable", "private-device").foreach { source =>
          val payload = row.deepMerge(Json.obj("evidence_version" -> envelope.asJson,
            "measurement" -> Json.obj("version" -> version.asJson, "bytes_status" -> "partial".asJson,
              "source" -> source.asJson, "termination" -> termination.asJson)))
          val measurement = ProxyEvidence.publicEvidence(ProxyEvidence.normalized(payload)).hcursor.downField("measurement")
          assertEquals(measurement.get[String]("source"),
            Right(if supported && Set("successful_socket_writes", "unavailable")(source) then source else "legacy_unknown"))
          assertEquals(measurement.get[String]("termination"),
            Right(if supported && Set("measured_terminal", "partial", "unavailable")(termination) then termination else "legacy_unknown"))
          assert(!measurement.focus.get.noSpaces.contains("private-device"))
        }
      }
    }

  test("ClientHello parser revisions are preserved without inventing legacy provenance"):
    for
      modern <- List(false, true)
      version <- List(None, Some("proxy-clienthello-v1"), Some("proxy-clienthello-v2"), Some("private-device"))
    do
      val payload = row.deepMerge(Json.obj(
        "evidence_version" -> Option.when(modern)(ProxyEvidence.Version).asJson,
        "tls_evidence" -> Json.obj("parser_version" -> version.asJson)))
      val expected = version.filter(_ => modern)
        .filter(Set("proxy-clienthello-v1", "proxy-clienthello-v2")).getOrElse("legacy_unknown")
      val published = ProxyEvidence.publicEvidence(ProxyEvidence.normalized(payload))
      assertEquals(published.hcursor.downField("tls").get[String]("parser_version"), Right(expected))
      assert(!published.noSpaces.contains("private-device"))

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
