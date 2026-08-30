package com.sslproxy.coordinator.postgres

/** Maps stream names to PostgreSQL sink targets. */
enum PostgresSinkTarget(val checksumTag: String):
  case ProxyEvents extends PostgresSinkTarget("proxy.events")
  case ProxyPayloadAudit extends PostgresSinkTarget("proxy.payload_audit")
  case WirelessAuditFrames extends PostgresSinkTarget("wireless.audit")
  case WirelessBandwidth extends PostgresSinkTarget("audit.wireless.bandwidth")
  case WirelessRogueAp extends PostgresSinkTarget("wireless.alert.rogue_ap")
  case WirelessDeauthFlood extends PostgresSinkTarget("wireless.alert.deauth_flood")
  case WirelessSignalAnomaly extends PostgresSinkTarget("wireless.alert.signal_anomaly")
  case WirelessPmfAttack extends PostgresSinkTarget("wireless.alert.pmf_attack")
  case WirelessClientInventory extends PostgresSinkTarget("wireless.client.inventory")
  case WirelessProbeRequests extends PostgresSinkTarget("wireless.probe.flush")
  case WirelessAttackSequence extends PostgresSinkTarget("wireless.alert.attack_sequence")
  case WirelessSequenceAlert extends PostgresSinkTarget("wireless.alert.sequence")
  case WirelessHandshakeAlert extends PostgresSinkTarget("wireless.alert.handshake")

object PostgresSinkTarget:
  def fromStreamName(streamName: String): Option[PostgresSinkTarget] =
    streamName match
      case "proxy.events" => Some(ProxyEvents)
      case "proxy.payload_audit" => Some(ProxyPayloadAudit)
      case "wireless.audit" => Some(WirelessAuditFrames)
      case "audit.wireless.bandwidth" => Some(WirelessBandwidth)
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
      case _ => None
