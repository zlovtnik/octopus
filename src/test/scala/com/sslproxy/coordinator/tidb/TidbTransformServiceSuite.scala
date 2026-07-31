package com.sslproxy.coordinator.tidb

import io.circe.Json
import io.circe.parser.*
import munit.*

class TidbTransformServiceSuite extends FunSuite:

  test("transform ProxyEvents creates events and blocked rollups"):
    val row = parse("""{"type": "tls_scan", "host": "example.com", "time": "2026-07-20T12:00:00Z", "blocked": true,
       "bytes_up": 100, "bytes_down": 200, "blocked_bytes": 300, "verdict": "MALICIOUS"}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.ProxyEvents, List(row))
    assertEquals(result.proxyEvents.size, 1)
    assertEquals(result.blockedEvents.size, 1)
    assertEquals(result.proxyEvents.head.eventType, "tls_scan")
    assertEquals(result.proxyEvents.head.host, "example.com")
    assertEquals(result.proxyEvents.head.bytesUp, 100L)
    assertEquals(result.proxyEvents.head.bytesDown, 200L)

  test("transform ProxyEvents without blocked does not create blocked"):
    val row = parse("""{"type": "tls_scan", "host": "example.com", "time": "2026-07-20T12:00:00Z", "blocked": false}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.ProxyEvents, List(row))
    assertEquals(result.proxyEvents.size, 1)
    assertEquals(result.blockedEvents.size, 0)

  test("transform ProxyPayloadAudit creates audit records"):
    val row = parse("""{"host": "example.com", "observed_at": "2026-07-20T12:00:00Z", "byte_offset": 0}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.ProxyPayloadAudit, List(row))
    assertEquals(result.proxyPayloadAudit.size, 1)
    assertEquals(result.proxyPayloadAudit.head.host, "example.com")

  test("transform WirelessAuditFrames creates frame records"):
    val row = parse("""{"event_type": "probe_request", "observed_at": "2026-07-20T12:00:00Z",
       "sensor_id": "s1", "location_id": "l1", "interface": "wlan0", "channel": 6,
       "frame_subtype": "probe_req", "raw_len": 100}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.WirelessAuditFrames, List(row))
    assertEquals(result.wirelessAuditFrames.size, 1)
    assertEquals(result.wirelessAuditFrames.head.sensorId, "s1")

  test("transform WirelessBandwidth creates bandwidth records"):
    val row = parse("""{"window_start": "2026-07-20T12:00:00Z", "window_end": "2026-07-20T12:05:00Z",
       "sensor_id": "s1", "location_id": "l1", "interface": "wlan0", "channel": 6,
       "source_mac": "aa:bb:cc:dd:ee:01", "destination_bssid": "aa:bb:cc:dd:ee:02",
       "bytes": 1000, "frame_count": 50, "threshold_exceeded": 0, "external_bssid": 0}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.WirelessBandwidth, List(row))
    assertEquals(result.wirelessBandwidth.size, 1)

  test("transform WirelessRogueAp creates alert"):
    val row = parse("""{"detected_at": "2026-07-20T12:00:00Z", "sensor_id": "s1", "location_id": "l1",
       "interface": "wlan0", "channel": 6, "bssid": "aa:bb:cc:dd:ee:01"}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.WirelessRogueAp, List(row))
    assertEquals(result.wirelessRogueAp.size, 1)

  test("transform WirelessClientInventory already has merged fields"):
    val row = parse("""{"sensor_id": "s1", "location_id": "l1", "snapshot_at": "2026-07-20T12:00:00Z",
       "client_mac": "aa:bb:cc:dd:ee:01", "last_seen": "2026-07-20T12:00:00Z",
       "first_seen": "2026-07-20T11:00:00Z", "is_authorized": true}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.WirelessClientInventory, List(row))
    assertEquals(result.wirelessClientInventory.size, 1)
    assertEquals(result.wirelessClientInventory.head.sensorId, "s1")
    assertEquals(result.wirelessClientInventory.head.clientMac, "aa:bb:cc:dd:ee:01")

  test("transform WirelessProbeRequests handles individual probe row"):
    val row = parse("""{"client_mac": "aa:bb:cc:dd:ee:01", "ssid": "TestNet",
       "first_seen": "2026-07-20T12:00:00Z", "last_seen": "2026-07-20T12:05:00Z",
       "probe_count": 10}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.WirelessProbeRequests, List(row))
    assertEquals(result.wirelessProbeRequests.size, 1)
    assertEquals(result.wirelessProbeRequests.head.ssid, "TestNet")

  test("transform attack sequence preserves a hidden SSID"):
    val row = parse(
      """{"detected_at":"2026-07-20T12:00:00Z","sensor_id":"s1","location_id":"l1",
        |"first_event_at":"2026-07-20T11:59:00Z","last_event_at":"2026-07-20T12:00:00Z"}""".stripMargin
    ).toOption.get

    val result = TidbTransformService.transform(TidbSinkTarget.WirelessAttackSequence, List(row))

    assertEquals(result.wirelessAttackSequence.size, 1)
    assertEquals(result.wirelessAttackSequence.head.ssid, None)

  test("inputRowCount returns correct counts"):
    val row = parse("""{"type": "tls_scan", "host": "a.com", "time": "2026-07-20T12:00:00Z", "blocked": false}""").toOption.get
    val result = TidbTransformService.transform(TidbSinkTarget.ProxyEvents, List(row))
    assertEquals(result.inputRowCount(TidbSinkTarget.ProxyEvents), 1)
    assertEquals(result.inputRowCount(TidbSinkTarget.WirelessAuditFrames), 0)
