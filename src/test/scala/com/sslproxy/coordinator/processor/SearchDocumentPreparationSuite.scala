package com.sslproxy.coordinator.processor

import munit.FunSuite

import java.sql.Timestamp
import java.time.Instant

class SearchDocumentPreparationSuite extends FunSuite:
  private val source = SearchDocumentSource(
    kind = SearchDocumentKind.Event,
    sourceKey = "event-1",
    sourceMac = Some("aa:bb:cc:dd:ee:ff"),
    locationId = Some("lab"),
    sensorId = Some("sensor-1"),
    observedAt = Timestamp.from(Instant.parse("2026-08-03T12:00:00Z")),
    bssid = Some("11:22:33:44:55:66"),
    ssid = Some("Example WiFi"),
    frameSubtype = Some("probe_request"),
    securityFlags = 3,
    handshakeCaptured = false,
    searchText = "  EXAMPLE\tWiFi  probe_request example ",
    detailJson = "{}"
  )

  test("normalization tokenization and identifiers are deterministic"):
    val first = SearchDocumentPreparation.prepare(source)
    val second = SearchDocumentPreparation.prepare(source)

    assertEquals(first, second)
    val document = first.fold(fail(_), identity)
    assertEquals(document.normalizedText, "example wifi probe_request example")
    assertEquals(
      document.tokens,
      List(
        ("example", 0.5d, 2),
        ("probe_request", 0.25d, 1),
        ("wifi", 0.25d, 1)
      )
    )
    assertEquals(document.normalizedSha256.length, 64)
    assertEquals(
      document.tags.map(_._1),
      List(
        "bssid",
        "frame_subtype",
        "location_id",
        "sensor_id",
        "source_mac",
        "ssid"
      )
    )

  test("blank content is rejected without side effects"):
    assert(SearchDocumentPreparation.prepare(source.copy(searchText = "  ")).isLeft)

  test("document identifiers are isolated by source table"):
    val event = SearchDocumentPreparation.prepare(source).fold(fail(_), identity)
    val device =
      SearchDocumentPreparation.prepare(source.copy(kind = SearchDocumentKind.Device)).fold(fail(_), identity)

    assertNotEquals(event.documentId, device.documentId)

  test("proxy compatibility identity hashes exact normalized prose independently of evidence"):
    val proxy = source.copy(
      kind = SearchDocumentKind.ProxyEvent,
      sourceKey = "80fbe8b2-b482-4bdc-a497-a6c15043eaa8",
      searchText = "  PROXY\tEVENT http_proxied ChatGPT.com:443 allowed  ",
      detailJson = "{\"bytes_up\":12}",
      evidenceRevision = Some(1L),
      templateVersion = "proxy-event-v2",
      normalizationVersion = ProxyAuthority.Version
    )
    val first = SearchDocumentPreparation.prepare(proxy).fold(fail(_), identity)
    val correction = SearchDocumentPreparation.prepare(proxy.copy(
      observedAt = Timestamp.from(proxy.observedAt.toInstant.plusSeconds(1)),
      detailJson = "{\"bytes_up\":34}",
      evidenceRevision = Some(2L),
      inputRevision = "nomic-prefix-v1:" + "a" * 64
    )).fold(fail(_), identity)

    assertEquals(first.normalizedText, "proxy event http_proxied chatgpt.com:443 allowed")
    assertEquals(first.normalizedSha256, "753c8f4e6bf77d4360aaea2eeb565a1ee9e425b7dedadc0d906cca2546de30d4")
    assertEquals(first.documentId, "5b9b6aff-ba7c-39b4-bb38-7ea2af4b2e91")
    assertEquals(correction.documentId, first.documentId)
    assertEquals(correction.normalizedSha256, first.normalizedSha256)
    assertEquals(correction.tokens, first.tokens)
    assertEquals(correction.evidenceRevision, Some(2L))
    assertEquals(correction.observedAt, Timestamp.from(proxy.observedAt.toInstant.plusSeconds(1)))
    assertNotEquals(correction.detailJson, first.detailJson)
    assertNotEquals(correction.inputRevision, first.inputRevision)

  test("proxy compatibility namespaces isolate event and blocked window identities"):
    val event = source.copy(
      kind = SearchDocumentKind.ProxyEvent,
      sourceKey = "80fbe8b2-b482-4bdc-a497-a6c15043eaa8"
    )
    val window = event.copy(
      kind = SearchDocumentKind.ProxyBlockedHostWindow,
      sourceKey = "proxy-window:80fbe8b2b4824bdca497a6c15043eaa8"
    )
    val eventDocument = SearchDocumentPreparation.prepare(event).fold(fail(_), identity)
    val windowDocument = SearchDocumentPreparation.prepare(window).fold(fail(_), identity)

    assertEquals(eventDocument.normalizedSha256, windowDocument.normalizedSha256)
    assertNotEquals(eventDocument.documentId, windowDocument.documentId)

  test("authority normalization remains separate from compatibility prose hashing"):
    val explicit = source.copy(searchText = "ChatGPT.com:443")
    val absent = source.copy(searchText = "ChatGPT.com")
    val explicitDocument = SearchDocumentPreparation.prepare(explicit).fold(fail(_), identity)
    val absentDocument = SearchDocumentPreparation.prepare(absent).fold(fail(_), identity)
    val explicitAuthority = ProxyAuthority.normalize(Some(explicit.searchText), "http_authority")
    val absentAuthority = ProxyAuthority.normalize(Some(absent.searchText), "http_authority")

    assertEquals(explicitAuthority.host_normalized, absentAuthority.host_normalized)
    assertEquals(explicitAuthority.port, Some(443))
    assertEquals(absentAuthority.port, None)
    assertNotEquals(explicitDocument.documentId, absentDocument.documentId)
    assertNotEquals(explicitDocument.normalizedSha256, absentDocument.normalizedSha256)

  test("proxy documents retain typed metadata without copying raw payloads into text"):
    val proxy = source.copy(
      kind = SearchDocumentKind.ProxyEvent,
      sourceKey = "80fbe8b2-b482-4bdc-a497-a6c15043eaa8",
      sourceMac = None,
      bssid = None,
      ssid = None,
      frameSubtype = None,
      searchText = "proxy event http_proxied api.example cdn allowed",
      detailJson = "{\"bytes_up\":12,\"bytes_down\":34}",
      host = Some("api.example"),
      proxyDeviceId = Some("8df7e18f-219e-4c33-9427-11fe3ae63ee8"),
      proxyEventType = Some("http_proxied"),
      blocked = Some(false),
      classification = Some("cdn")
    )

    val document = SearchDocumentPreparation.prepare(proxy).fold(fail(_), identity)

    assertEquals(document.host, Some("api.example"))
    assertEquals(document.blocked, Some(false))
    assertEquals(document.classification, Some("cdn"))
    assert(!document.normalizedText.contains("bytes_up"))
    assert(document.tags.contains("blocked" -> "false"))
