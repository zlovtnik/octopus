package com.sslproxy.coordinator.tidb.sql

import doobie.Fragment
import doobie.implicits.*
import io.circe.Json

import java.sql.Timestamp

object WirelessSql:
  def lookupDevice(mac: String): Fragment =
    val normalizedMac = mac.trim.toLowerCase(java.util.Locale.ROOT)
    sql"""SELECT JSON_OBJECT(
            'device_id', mac_id,
            'username', username,
            'display_name', display_name,
            'hostname', hostname
          ) AS device_json
          FROM devices
          WHERE mac_id = $normalizedMac
          LIMIT 1"""

  val AuthorizedNetworksQuery: Fragment =
    sql"""SELECT COALESCE(
            JSON_ARRAYAGG(
              JSON_OBJECT(
                'ssid', ssid,
                'bssid', LOWER(bssid),
                'location_id', location_id,
                'label', label,
                'enabled', enabled
              )
            ),
            '[]'
          ) AS networks_json
          FROM wireless_authorized_networks
          WHERE enabled = TRUE"""

  def upsertClientProbe(
      ssid: String,
      clientMac: String,
      observedBssid: Option[String],
      firstSeen: Option[Timestamp],
      lastSeen: Option[Timestamp],
      probeCount: Long,
      locationId: Option[String],
      batchId: String
  ): Fragment =
    val normalizedSsid = ssid.trim
    val normalizedBssid = observedBssid.map(_.trim.toLowerCase(java.util.Locale.ROOT))
    sql"""INSERT INTO wireless_clients (
            ssid, client_mac, known_bssid, first_seen, last_seen,
            probe_count, location_id, last_probe_batch_id
          ) VALUES (
            $normalizedSsid, $clientMac,
            (SELECT MAX(authorized.bssid) FROM wireless_authorized_networks authorized
             WHERE authorized.ssid = $normalizedSsid AND authorized.enabled = TRUE
               AND ($normalizedBssid IS NULL OR authorized.bssid = $normalizedBssid)
               AND ($locationId IS NULL OR authorized.location_id = $locationId)
             HAVING COUNT(*) = 1),
            $firstSeen, $lastSeen, $probeCount, $locationId, $batchId
          ) ON DUPLICATE KEY UPDATE
            first_seen = LEAST(wireless_clients.first_seen, VALUES(first_seen)),
            last_seen = GREATEST(wireless_clients.last_seen, VALUES(last_seen)),
            probe_count = CASE
              WHEN wireless_clients.last_probe_batch_id IS NULL
                OR wireless_clients.last_probe_batch_id != VALUES(last_probe_batch_id)
              THEN wireless_clients.probe_count + VALUES(probe_count)
              ELSE wireless_clients.probe_count
            END,
            known_bssid = COALESCE(VALUES(known_bssid), wireless_clients.known_bssid),
            location_id = COALESCE(VALUES(location_id), wireless_clients.location_id),
            last_probe_batch_id = VALUES(last_probe_batch_id)"""

  def upsertBacklog(
      dedupeKey: String,
      streamName: String,
      payload: Json,
      failureStage: String
  ): Fragment =
    sql"""INSERT INTO sync_backlog (
            dedupe_key, stream_name, payload, failure_stage, status,
            attempt_count, next_attempt_at, created_at, updated_at
          ) VALUES (
            $dedupeKey, $streamName, ${payload.noSpaces}, $failureStage, 'pending',
            0, CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
          ) ON DUPLICATE KEY UPDATE
            payload = VALUES(payload),
            failure_stage = VALUES(failure_stage),
            status = CASE WHEN sync_backlog.status = 'synced' THEN 'synced' ELSE 'pending' END,
            updated_at = CURRENT_TIMESTAMP(6)"""

  def oldestPending(limit: Int): Fragment =
    sql"""SELECT dedupe_key, stream_name, CAST(payload AS CHAR), failure_stage,
                 attempt_count, created_at
          FROM sync_backlog
          WHERE status IN ('pending', 'sync_failed')
            AND next_attempt_at <= CURRENT_TIMESTAMP(6)
          ORDER BY created_at, stream_name, dedupe_key
          LIMIT $limit"""

  def markSynced(dedupeKey: String, streamName: String): Fragment =
    sql"""UPDATE sync_backlog
          SET status = 'synced', last_error = NULL, updated_at = CURRENT_TIMESTAMP(6)
          WHERE dedupe_key = $dedupeKey AND stream_name = $streamName
            AND status <> 'synced'"""

  def markFailed(dedupeKey: String, streamName: String, error: String): Fragment =
    sql"""UPDATE sync_backlog
          SET status = 'failed', last_error = $error, updated_at = CURRENT_TIMESTAMP(6)
          WHERE dedupe_key = $dedupeKey AND stream_name = $streamName
            AND status IN ('pending', 'sync_failed')"""

  def pruneSynced(cutoff: Timestamp, limit: Int): Fragment =
    sql"""DELETE FROM sync_backlog
          WHERE status = 'synced' AND updated_at < $cutoff
          ORDER BY updated_at, stream_name, dedupe_key
          LIMIT $limit"""
