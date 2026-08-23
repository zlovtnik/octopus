package com.sslproxy.coordinator.postgres

import munit.*

class PostgresSinkTargetSuite extends FunSuite:

  test("fromStreamName returns ProxyEvents for proxy.events"):
    assertEquals(PostgresSinkTarget.fromStreamName("proxy.events"), Some(PostgresSinkTarget.ProxyEvents))

  test("fromStreamName returns WirelessAuditFrames for wireless.audit"):
    assertEquals(PostgresSinkTarget.fromStreamName("wireless.audit"), Some(PostgresSinkTarget.WirelessAuditFrames))

  test("fromStreamName returns None for unknown stream"):
    assertEquals(PostgresSinkTarget.fromStreamName("unknown.stream"), None)

  test("fromStreamName accepts legacy aliases"):
    assertEquals(PostgresSinkTarget.fromStreamName("wireless.rogue_ap"), Some(PostgresSinkTarget.WirelessRogueAp))
    assertEquals(PostgresSinkTarget.fromStreamName("wireless.deauth_flood"), Some(PostgresSinkTarget.WirelessDeauthFlood))
    assertEquals(PostgresSinkTarget.fromStreamName("wireless.signal_anomaly"), Some(PostgresSinkTarget.WirelessSignalAnomaly))
    assertEquals(PostgresSinkTarget.fromStreamName("wireless.pmf_attack"), Some(PostgresSinkTarget.WirelessPmfAttack))
    assertEquals(PostgresSinkTarget.fromStreamName("wireless.client_inventory"), Some(PostgresSinkTarget.WirelessClientInventory))
    assertEquals(PostgresSinkTarget.fromStreamName("wireless.probe_requests"), Some(PostgresSinkTarget.WirelessProbeRequests))

  test("handshake alert accepts the canonical and firmware topic names"):
    assertEquals(
      PostgresSinkTarget.fromStreamName("wireless.alert.handshake"),
      Some(PostgresSinkTarget.WirelessHandshakeAlert)
    )
    assertEquals(
      PostgresSinkTarget.fromStreamName("wifi.alert.handshake"),
      Some(PostgresSinkTarget.WirelessHandshakeAlert)
    )

  test("checksumTag is non-empty for all targets"):
    for target <- PostgresSinkTarget.values do
      assert(target.checksumTag.nonEmpty, s"${target} has empty checksumTag")
