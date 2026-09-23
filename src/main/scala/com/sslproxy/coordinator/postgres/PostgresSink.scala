package com.sslproxy.coordinator.postgres

import cats.effect.IO

trait PostgresLoadTransaction:
  def insertChunk(
    batchId: String,
    target: PostgresSinkTarget,
    rows: PostgresRowSet,
    rowOffset: Long
  ): IO[Long]

/** PostgreSQL sink trait — 10 insert methods. */
trait PostgresSink:
  def withLoadTransaction[A](use: PostgresLoadTransaction => IO[A]): IO[A]

  def insertProxyEvents(
    batchId: String,
    rows: List[ProxyEventInsert],
    blockedRows: List[BlockedEventInsert],
    rowOffset: Long = 0L
  ): IO[Long]

  def insertProxyPayloadAudit(batchId: String, rows: List[ProxyPayloadAuditInsert]): IO[Long]

  def insertWirelessAuditFrames(batchId: String, rows: List[WirelessAuditFrameInsert]): IO[Long]

  def insertWirelessBandwidth(batchId: String, rows: List[WirelessBandwidthInsert]): IO[Long]

  def insertWirelessRogueAp(batchId: String, rows: List[WirelessRogueApInsert]): IO[Long]

  def insertWirelessDeauthFlood(batchId: String, rows: List[WirelessDeauthFloodInsert]): IO[Long]

  def insertWirelessSignalAnomaly(batchId: String, rows: List[WirelessSignalAnomalyInsert]): IO[Long]

  def insertWirelessPmfAttack(batchId: String, rows: List[WirelessPmfAttackInsert]): IO[Long]

  def insertWirelessClientInventory(batchId: String, rows: List[WirelessClientInventoryInsert]): IO[Long]

  def insertWirelessProbeRequests(batchId: String, rows: List[WirelessProbeRequestInsert]): IO[Long]

  def insertWirelessAttackSequence(batchId: String, rows: List[WirelessAttackSequenceInsert]): IO[Long]

  def insertWirelessSequenceAlert(batchId: String, rows: List[WirelessSequenceAlertInsert]): IO[Long]

  def insertWirelessHandshakeAlert(batchId: String, rows: List[WirelessHandshakeAlertInsert]): IO[Long]
