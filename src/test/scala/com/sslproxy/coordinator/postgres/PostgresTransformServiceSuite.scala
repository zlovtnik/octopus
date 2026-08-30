package com.sslproxy.coordinator.postgres

import io.circe.Json
import io.circe.parser.*
import munit.*

class PostgresTransformServiceSuite extends FunSuite:

  test("transform ProxyEvents creates events and blocked rollups"):
    val row = parse("""{"type": "tls_scan", "host": "example.com", "time": "2026-07-20T12:00:00Z", "blocked": true,
       "bytes_up": 100, "bytes_down": 200, "blocked_bytes": 300, "verdict": "MALICIOUS"}""").toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.ProxyEvents, List(row))
    assertEquals(result.proxyEvents.size, 1)
    assertEquals(result.blockedEvents.size, 1)
    assertEquals(result.proxyEvents.head.eventType, "tls_scan")
    assertEquals(result.proxyEvents.head.host, "example.com")
    assertEquals(result.proxyEvents.head.bytesUp, 100L)
    assertEquals(result.proxyEvents.head.bytesDown, 200L)
    assertEquals(result.proxyEvents.head.blocked, true)

  test("transform ProxyEvents without blocked does not create blocked"):
    val row = parse(
      """{"type": "tls_scan", "host": "example.com", "time": "2026-07-20T12:00:00Z", "blocked": false}"""
    ).toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.ProxyEvents, List(row))
    assertEquals(result.proxyEvents.size, 1)
    assertEquals(result.blockedEvents.size, 0)

  test("transform ProxyEvents bounds blocked risk scores to DECIMAL(10,4)"):
    val high = parse(
      """{"type":"tls_scan","host":"high.example","time":"2026-07-20T12:00:00Z","blocked":true,"risk_score":1000000}"""
    ).toOption.get
    val low = parse(
      """{"type":"tls_scan","host":"low.example","time":"2026-07-20T12:00:00Z","blocked":true,"metrics":{"risk_score":-1000000}}"""
    ).toOption.get

    val result = PostgresTransformService.transform(PostgresSinkTarget.ProxyEvents, List(high, low))

    assertEquals(result.blockedEvents.map(_.riskScore), List(Some(999999.9999d), Some(-999999.9999d)))

  test("transform ProxyPayloadAudit creates audit records"):
    val row = parse("""{"host": "example.com", "observed_at": "2026-07-20T12:00:00Z", "byte_offset": 0,
       "is_encrypted": true, "truncated": true}""").toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.ProxyPayloadAudit, List(row))
    assertEquals(result.proxyPayloadAudit.size, 1)
    assertEquals(result.proxyPayloadAudit.head.host, "example.com")
    assertEquals(result.proxyPayloadAudit.head.isEncrypted, true)
    assertEquals(result.proxyPayloadAudit.head.truncated, true)

  test("transform WirelessAuditFrames creates frame records"):
    val row = parse("""{"event_type": "probe_request", "observed_at": "2026-07-20T12:00:00Z",
       "sensor_id": "s1", "location_id": "l1", "interface": "wlan0", "channel": 6,
       "frame_subtype": "probe_req", "raw_len": 100, "retry": true, "protected": true}""").toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.WirelessAuditFrames, List(row))
    assertEquals(result.wirelessAuditFrames.size, 1)
    assertEquals(result.wirelessAuditFrames.head.sensorId, "s1")
    assertEquals(result.wirelessAuditFrames.head.isRetry, true)
    assertEquals(result.wirelessAuditFrames.head.isProtected, true)

  test("transform WirelessBandwidth creates bandwidth records"):
    val row = parse("""{"window_start": "2026-07-20T12:00:00Z", "window_end": "2026-07-20T12:05:00Z",
       "sensor_id": "s1", "location_id": "l1", "interface": "wlan0", "channel": 6,
       "source_mac": "aa:bb:cc:dd:ee:01", "destination_bssid": "aa:bb:cc:dd:ee:02",
       "bytes": 1000, "frame_count": 50, "threshold_exceeded": 1, "external_bssid": true,
       "window_is_partial": true}""").toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.WirelessBandwidth, List(row))
    assertEquals(result.wirelessBandwidth.size, 1)
    assertEquals(result.wirelessBandwidth.head.thresholdExceeded, true)
    assertEquals(result.wirelessBandwidth.head.externalBssid, true)
    assertEquals(result.wirelessBandwidth.head.windowIsPartial, true)

  test("transform WirelessRogueAp creates alert"):
    val row = parse("""{"detected_at": "2026-07-20T12:00:00Z", "sensor_id": "s1", "location_id": "l1",
       "interface": "wlan0", "channel": 6, "bssid": "aa:bb:cc:dd:ee:01"}""").toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.WirelessRogueAp, List(row))
    assertEquals(result.wirelessRogueAp.size, 1)

  test("transform WirelessClientInventory already has merged fields"):
    val row = parse("""{"sensor_id": "s1", "location_id": "l1", "snapshot_at": "2026-07-20T12:00:00Z",
       "client_mac": "aa:bb:cc:dd:ee:01", "last_seen": "2026-07-20T12:00:00Z",
       "first_seen": "2026-07-20T11:00:00Z", "is_authorized": true}""").toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.WirelessClientInventory, List(row))
    assertEquals(result.wirelessClientInventory.size, 1)
    assertEquals(result.wirelessClientInventory.head.sensorId, "s1")
    assertEquals(result.wirelessClientInventory.head.clientMac, "aa:bb:cc:dd:ee:01")
    assertEquals(result.wirelessClientInventory.head.isAuthorized, true)

  test("transform WirelessProbeRequests handles individual probe row"):
    val row = parse("""{"client_mac": "aa:bb:cc:dd:ee:01", "ssid": "TestNet",
       "first_seen": "2026-07-20T12:00:00Z", "last_seen": "2026-07-20T12:05:00Z",
       "probe_count": 10}""").toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.WirelessProbeRequests, List(row))
    assertEquals(result.wirelessProbeRequests.size, 1)
    assertEquals(result.wirelessProbeRequests.head.ssid, "TestNet")

  test("transform attack sequence represents an absent SSID as None"):
    val row = parse(
      """{"detected_at":"2026-07-20T12:00:00Z","sensor_id":"s1","location_id":"l1",
        |"first_event_at":"2026-07-20T11:59:00Z","last_event_at":"2026-07-20T12:00:00Z"}""".stripMargin
    ).toOption.get

    val result = PostgresTransformService.transform(PostgresSinkTarget.WirelessAttackSequence, List(row))

    assertEquals(result.wirelessAttackSequence.size, 1)
    assertEquals(result.wirelessAttackSequence.head.ssid, None)

  test("transform attack sequence preserves an empty hidden SSID"):
    val row = parse(
      """{"detected_at":"2026-07-20T12:00:00Z","sensor_id":"s1","location_id":"l1","ssid":"",
        |"first_event_at":"2026-07-20T11:59:00Z","last_event_at":"2026-07-20T12:00:00Z"}""".stripMargin
    ).toOption.get

    val result = PostgresTransformService.transform(PostgresSinkTarget.WirelessAttackSequence, List(row))

    assertEquals(result.wirelessAttackSequence.head.ssid, Some(""))

  test("transform handshake alerts accepts detected_at and minimizes handshake evidence"):
    val row = parse(
      """{"detected_at":"2026-07-20T12:00:00Z","sensor_id":"s1","location_id":"l1",
        |"interface":"wlan0","bssid":"aa:bb:cc:dd:ee:01","client_mac":"aa:bb:cc:dd:ee:02",
        |"pmkid":"sensitive-pmkid"}""".stripMargin
    ).toOption.get

    val result = PostgresTransformService.transform(PostgresSinkTarget.WirelessHandshakeAlert, List(row))
    val alert = result.wirelessHandshakeAlert.head

    assertEquals(alert.detectedAt.toString, "2026-07-20T12:00Z")
    assertEquals(alert.pmkidSha256.map(_.length), Some(64))
    assert(!alert.toString.contains("sensitive-pmkid"))

  test("inputRowCount returns correct counts"):
    val row =
      parse("""{"type": "tls_scan", "host": "a.com", "time": "2026-07-20T12:00:00Z", "blocked": false}""").toOption.get
    val result = PostgresTransformService.transform(PostgresSinkTarget.ProxyEvents, List(row))
    assertEquals(result.inputRowCount(PostgresSinkTarget.ProxyEvents), 1)
    assertEquals(result.inputRowCount(PostgresSinkTarget.WirelessAuditFrames), 0)
