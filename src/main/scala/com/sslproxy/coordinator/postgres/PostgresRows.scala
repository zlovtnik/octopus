package com.sslproxy.coordinator.postgres

import java.time.OffsetDateTime

/** Container for all insert row lists. */
final case class PostgresRowSet(
    proxyEvents: List[ProxyEventInsert],
    blockedEvents: List[BlockedEventInsert],
    proxyPayloadAudit: List[ProxyPayloadAuditInsert],
    wirelessAuditFrames: List[WirelessAuditFrameInsert],
    wirelessBandwidth: List[WirelessBandwidthInsert],
    wirelessRogueAp: List[WirelessRogueApInsert],
    wirelessDeauthFlood: List[WirelessDeauthFloodInsert],
    wirelessSignalAnomaly: List[WirelessSignalAnomalyInsert],
    wirelessPmfAttack: List[WirelessPmfAttackInsert],
    wirelessClientInventory: List[WirelessClientInventoryInsert],
    wirelessProbeRequests: List[WirelessProbeRequestInsert],
    wirelessAttackSequence: List[WirelessAttackSequenceInsert],
    wirelessSequenceAlert: List[WirelessSequenceAlertInsert],
    wirelessHandshakeAlert: List[WirelessHandshakeAlertInsert]
):
  def inputRowCount(target: PostgresSinkTarget): Int =
    target match
      case PostgresSinkTarget.ProxyEvents             => proxyEvents.size
      case PostgresSinkTarget.ProxyPayloadAudit       => proxyPayloadAudit.size
      case PostgresSinkTarget.WirelessAuditFrames     => wirelessAuditFrames.size
      case PostgresSinkTarget.WirelessBandwidth       => wirelessBandwidth.size
      case PostgresSinkTarget.WirelessRogueAp         => wirelessRogueAp.size
      case PostgresSinkTarget.WirelessDeauthFlood     => wirelessDeauthFlood.size
      case PostgresSinkTarget.WirelessSignalAnomaly   => wirelessSignalAnomaly.size
      case PostgresSinkTarget.WirelessPmfAttack       => wirelessPmfAttack.size
      case PostgresSinkTarget.WirelessClientInventory => wirelessClientInventory.size
      case PostgresSinkTarget.WirelessProbeRequests   => wirelessProbeRequests.size
      case PostgresSinkTarget.WirelessAttackSequence  => wirelessAttackSequence.size
      case PostgresSinkTarget.WirelessSequenceAlert   => wirelessSequenceAlert.size
      case PostgresSinkTarget.WirelessHandshakeAlert  => wirelessHandshakeAlert.size

object PostgresRowSet:
  val empty: PostgresRowSet = PostgresRowSet(
    proxyEvents = Nil,
    blockedEvents = Nil,
    proxyPayloadAudit = Nil,
    wirelessAuditFrames = Nil,
    wirelessBandwidth = Nil,
    wirelessRogueAp = Nil,
    wirelessDeauthFlood = Nil,
    wirelessSignalAnomaly = Nil,
    wirelessPmfAttack = Nil,
    wirelessClientInventory = Nil,
    wirelessProbeRequests = Nil,
    wirelessAttackSequence = Nil,
    wirelessSequenceAlert = Nil,
    wirelessHandshakeAlert = Nil
  )

// ---------------------------------------------------------------------------
// Insert record types
// ---------------------------------------------------------------------------

final case class ProxyEventInsert(
    eventTime: OffsetDateTime,
    eventType: String,
    host: String,
    peerIp: Option[String],
    wgPubkey: Option[String],
    deviceId: Option[String],
    identitySource: String,
    peerHostname: Option[String],
    clientUa: Option[String],
    bytesUp: Long,
    bytesDown: Long,
    statusCode: Option[Long],
    blocked: Boolean,
    obfuscationProfile: Option[String],
    correlationId: Option[String],
    parentEventId: Option[String],
    eventSequence: Option[Long],
    durationMs: Option[Long],
    reason: Option[String],
    rawJson: Option[String]
)

final case class BlockedEventInsert(
    rowSequence: Long,
    host: String,
    blockedBytes: Long,
    frequencyHz: Option[Double],
    riskScore: Option[Double],
    category: Option[String],
    verdict: String,
    tarpitHeldMs: Long,
    iatMs: Option[Long],
    consecutiveBlocks: Option[Long],
    lastVerdict: Option[String],
    tlsVer: Option[String],
    alpn: Option[String],
    ja3Lite: Option[String],
    resolvedIp: Option[String],
    asnOrg: Option[String]
)

final case class ProxyPayloadAuditInsert(
    correlationId: String,
    host: String,
    direction: String,
    capturedAt: OffsetDateTime,
    byteOffset: Long,
    payloadObjectKey: Option[String],
    contentType: Option[String],
    httpMethod: Option[String],
    httpStatus: Option[Long],
    httpPath: Option[String],
    isEncrypted: Boolean,
    truncated: Boolean,
    peerIp: Option[String],
    notes: Option[String]
)

final case class WirelessAuditFrameInsert(
    rowSequence: Long,
    eventType: String,
    observedAt: OffsetDateTime,
    sensorId: String,
    locationId: String,
    iface: String,
    channel: Long,
    frameType: Option[String],
    frameSubtype: String,
    bssid: Option[String],
    sourceMac: Option[String],
    destinationMac: Option[String],
    transmitterMac: Option[String],
    receiverMac: Option[String],
    destinationBssid: Option[String],
    ssid: Option[String],
    signalDbm: Option[Long],
    sequenceNumber: Option[Long],
    rawLen: Long,
    isRetry: Boolean,
    isMoreData: Boolean,
    isPowerSave: Boolean,
    isProtected: Boolean,
    isToDs: Boolean,
    isFromDs: Boolean,
    isHandshake: Boolean,
    securityFlags: Long,
    deviceId: Option[String],
    username: Option[String],
    identitySource: String,
    tags: Option[String],
    anomalyReasons: Option[String],
    rawJson: Option[String],
    regDomain: Option[String]
)

final case class WirelessBandwidthInsert(
    rowSequence: Long,
    schemaVersion: Long,
    windowStart: OffsetDateTime,
    windowEnd: OffsetDateTime,
    sensorId: String,
    locationId: String,
    iface: String,
    channel: Long,
    sourceMac: String,
    destinationBssid: String,
    ssid: Option[String],
    bytes: Long,
    frameCount: Long,
    retryCount: Long,
    moreDataCount: Long,
    powerSaveCount: Long,
    strongestSignalDbm: Option[Long],
    histUnder100: Long,
    hist100500: Long,
    hist5001000: Long,
    hist10001500: Long,
    interArrivalP50Ms: Option[Long],
    externalBssid: Boolean,
    thresholdExceeded: Boolean,
    wallClockDeltaMs: Option[Long],
    windowIsPartial: Boolean,
    publishedAt: Option[OffsetDateTime]
)

final case class WirelessRogueApInsert(
    rowSequence: Long,
    detectedAt: OffsetDateTime,
    sensorId: String,
    locationId: String,
    iface: String,
    channel: Long,
    rogueBssid: String,
    ssid: Option[String],
    signalDbm: Option[Long],
    ssidImpersonation: Long,
    rawJson: Option[String]
)

final case class WirelessDeauthFloodInsert(
    rowSequence: Long,
    detectedAt: OffsetDateTime,
    sensorId: String,
    locationId: String,
    iface: String,
    channel: Long,
    attackerMac: Option[String],
    targetBssid: Option[String],
    targetSsid: Option[String],
    deauthCount: Long,
    windowSecs: Long,
    threshold: Long,
    signalDbm: Option[Long],
    rawJson: Option[String]
)

final case class WirelessSignalAnomalyInsert(
    rowSequence: Long,
    detectedAt: OffsetDateTime,
    sensorId: String,
    locationId: String,
    sourceMac: String,
    bssid: Option[String],
    ssid: Option[String],
    channel: Long,
    baselineDbm: Long,
    observedDbm: Long,
    dbmDelta: Long,
    configuredDelta: Long
)

final case class WirelessPmfAttackInsert(
    rowSequence: Long,
    detectedAt: OffsetDateTime,
    sensorId: String,
    locationId: String,
    targetMac: String,
    targetBssid: Option[String],
    ssid: Option[String],
    channel: Option[Long],
    attackTag: String,
    reconnectWindowMs: Option[Long]
)

final case class WirelessClientInventoryInsert(
    sensorId: String,
    locationId: String,
    snapshotAt: OffsetDateTime,
    clientMac: String,
    bssid: Option[String],
    ssid: Option[String],
    deviceId: Option[String],
    username: Option[String],
    identitySource: Option[String],
    lastSeen: OffsetDateTime,
    firstSeen: OffsetDateTime,
    signalDbm: Option[Long],
    isAuthorized: Boolean
)

final case class WirelessProbeRequestInsert(
    rowSequence: Long,
    clientMac: String,
    ssid: String,
    knownBssid: Option[String],
    firstSeen: OffsetDateTime,
    lastSeen: OffsetDateTime,
    probeCount: Long
)

final case class WirelessAttackSequenceInsert(
    rowSequence: Long,
    detectedAt: OffsetDateTime,
    sensorId: String,
    locationId: String,
    ssid: Option[String],
    attackChain: Option[String],
    firstEventAt: OffsetDateTime,
    lastEventAt: OffsetDateTime,
    factorBreakdown: Option[String],
    explanation: Option[String],
    rawJson: Option[String]
)

final case class WirelessSequenceAlertInsert(
    rowSequence: Long,
    detectedAt: OffsetDateTime,
    sensorId: String,
    locationId: String,
    sessionKey: String,
    sourceMac: Option[String],
    bssid: Option[String],
    ssid: Option[String],
    attackTag: String,
    sequence: Option[String],
    firstEventAt: OffsetDateTime,
    lastEventAt: OffsetDateTime,
    factorBreakdown: Option[String],
    explanation: Option[String],
    rawJson: Option[String]
)

final case class WirelessHandshakeAlertInsert(
    rowSequence: Long,
    detectedAt: OffsetDateTime,
    sensorId: String,
    locationId: String,
    iface: String,
    bssid: String,
    clientMac: String,
    signalDbm: Option[Long],
    pmkidSha256: Option[String]
)
