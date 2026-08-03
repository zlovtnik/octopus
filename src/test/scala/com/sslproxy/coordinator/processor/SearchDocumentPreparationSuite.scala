package com.sslproxy.coordinator.processor

import munit.FunSuite

import java.sql.Timestamp
import java.time.Instant

class SearchDocumentPreparationSuite extends FunSuite:
  private val source = SearchDocumentSource(
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
    assertEquals(document.tokens, List(
      ("example", 0.5d, 2),
      ("probe_request", 0.25d, 1),
      ("wifi", 0.25d, 1)
    ))
    assertEquals(document.normalizedSha256.length, 64)
    assertEquals(document.tags.map(_._1), List(
      "bssid", "frame_subtype", "location_id", "sensor_id", "source_mac", "ssid"
    ))

  test("blank content is rejected without side effects"):
    assert(SearchDocumentPreparation.prepare(source.copy(searchText = "  ")).isLeft)
