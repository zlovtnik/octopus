package com.sslproxy.coordinator.tidb

/** Maps stream names to TiDB sink targets. */
enum TidbSinkTarget(val checksumTag: String):
  case ProxyEvents              extends TidbSinkTarget("proxy.events")
  case ProxyPayloadAudit        extends TidbSinkTarget("proxy.payload_audit")
  case WirelessAuditFrames      extends TidbSinkTarget("wireless.audit")
  case WirelessBandwidth        extends TidbSinkTarget("audit.wireless.bandwidth")
  case WirelessRogueAp          extends TidbSinkTarget("wireless.alert.rogue_ap")
  case WirelessDeauthFlood      extends TidbSinkTarget("wireless.alert.deauth_flood")
  case WirelessSignalAnomaly    extends TidbSinkTarget("wireless.alert.signal_anomaly")
  case WirelessPmfAttack        extends TidbSinkTarget("wireless.alert.pmf_attack")
  case WirelessClientInventory  extends TidbSinkTarget("wireless.client.inventory")
  case WirelessProbeRequests    extends TidbSinkTarget("wireless.probe.flush")
  case WirelessAttackSequence   extends TidbSinkTarget("wireless.alert.attack_sequence")
  case WirelessSequenceAlert    extends TidbSinkTarget("wireless.alert.sequence")
  case WirelessHandshakeAlert   extends TidbSinkTarget("wireless.alert.handshake")

object TidbSinkTarget:
  def fromStreamName(streamName: String): Option[TidbSinkTarget] =
    streamName match
      case "proxy.events"                        => Some(ProxyEvents)
      case "proxy.payload_audit"                 => Some(ProxyPayloadAudit)
      case "wireless.audit"                      => Some(WirelessAuditFrames)
      case "audit.wireless.bandwidth"            => Some(WirelessBandwidth)
      case "wireless.rogue_ap" | "wireless.alert.rogue_ap" => Some(WirelessRogueAp)
      case "wireless.deauth_flood" | "wireless.alert.deauth_flood" => Some(WirelessDeauthFlood)
      case "wireless.signal_anomaly" | "wireless.alert.signal_anomaly" => Some(WirelessSignalAnomaly)
      case "wireless.pmf_attack" | "wireless.alert.pmf_attack" => Some(WirelessPmfAttack)
      case "wireless.client_inventory" | "wireless.client.inventory" => Some(WirelessClientInventory)
      case "wireless.probe_requests" | "wireless.probe.flush" => Some(WirelessProbeRequests)
      case "wireless.attack_sequence" | "wireless.alert.attack_sequence" => Some(WirelessAttackSequence)
      case "wireless.sequence" | "wireless.alert.sequence" | "wireless.sequence_alert" => Some(WirelessSequenceAlert)
      case "wifi.alert.handshake" | "wireless.alert.handshake" | "wireless.handshake" =>
        Some(WirelessHandshakeAlert)
      case _                                     => None
