package com.sslproxy.coordinator.postgres.sql

import doobie.Fragment
import doobie.implicits.*
import io.circe.Json

import java.sql.Timestamp

object WirelessSql:
  def lookupDevice(mac: String): Fragment =
    val normalizedMac = mac.trim.toLowerCase(java.util.Locale.ROOT)
    sql"""SELECT jsonb_build_object(
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
            jsonb_agg(
              jsonb_build_object(
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
    val normalizedMac = clientMac.trim.toLowerCase(java.util.Locale.ROOT)
    sql"""INSERT INTO wireless_clients (
            ssid, client_mac, known_bssid, first_seen, last_seen,
            probe_count, location_id, last_probe_batch_id
          ) VALUES (
            $normalizedSsid, $normalizedMac,
            (SELECT MAX(authorized.bssid) FROM wireless_authorized_networks authorized
             WHERE authorized.enabled = TRUE
               AND (authorized.ssid IS NULL OR authorized.ssid = $normalizedSsid)
               AND (
                 authorized.bssid IS NULL
                 OR (
                   CAST($normalizedBssid AS TEXT) IS NOT NULL
                   AND authorized.bssid = CAST($normalizedBssid AS TEXT)
                 )
               )
               AND (
                 authorized.location_id IS NULL
                 OR (
                   CAST($locationId AS TEXT) IS NOT NULL
                   AND authorized.location_id = CAST($locationId AS TEXT)
                 )
               )
             HAVING COUNT(*) = 1),
            $firstSeen, $lastSeen, $probeCount, $locationId, $batchId
          ) ON CONFLICT (ssid, client_mac) DO UPDATE SET
            first_seen = LEAST(COALESCE(wireless_clients.first_seen, EXCLUDED.first_seen), COALESCE(EXCLUDED.first_seen, wireless_clients.first_seen)),
            last_seen = GREATEST(COALESCE(wireless_clients.last_seen, EXCLUDED.last_seen), COALESCE(EXCLUDED.last_seen, wireless_clients.last_seen)),
            probe_count = CASE
              WHEN wireless_clients.last_probe_batch_id IS NULL
                OR wireless_clients.last_probe_batch_id != EXCLUDED.last_probe_batch_id
              THEN wireless_clients.probe_count + EXCLUDED.probe_count
              ELSE wireless_clients.probe_count
            END,
            known_bssid = COALESCE(EXCLUDED.known_bssid, wireless_clients.known_bssid),
            location_id = COALESCE(EXCLUDED.location_id, wireless_clients.location_id),
            last_probe_batch_id = EXCLUDED.last_probe_batch_id"""

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
            0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
          ) ON CONFLICT (dedupe_key, stream_name) DO UPDATE SET
            payload = EXCLUDED.payload,
            failure_stage = EXCLUDED.failure_stage,
            status = CASE WHEN sync_backlog.status = 'synced' THEN 'synced' ELSE 'pending' END,
            updated_at = CURRENT_TIMESTAMP"""

  def oldestPending(limit: Int): Fragment =
    sql"""SELECT dedupe_key, stream_name, CAST(payload AS TEXT), failure_stage,
                 attempt_count, created_at
          FROM sync_backlog
          WHERE status IN ('pending', 'sync_failed')
            AND next_attempt_at <= CURRENT_TIMESTAMP
          ORDER BY created_at, stream_name, dedupe_key
          LIMIT ${limit.max(1)}"""

  def markSynced(dedupeKey: String, streamName: String): Fragment =
    sql"""UPDATE sync_backlog
          SET status = 'synced', last_error = NULL, updated_at = CURRENT_TIMESTAMP
          WHERE dedupe_key = $dedupeKey AND stream_name = $streamName
            AND status <> 'synced'"""

  def markFailed(
      dedupeKey: String,
      streamName: String,
      status: String,
      error: String,
      delaySeconds: Long
  ): Fragment =
    sql"""UPDATE sync_backlog
          SET status = $status,
              last_error = $error,
              next_attempt_at = (CURRENT_TIMESTAMP + (${delaySeconds.max(0L)}) * INTERVAL '1 second'),
              attempt_count = attempt_count + 1,
              updated_at = CURRENT_TIMESTAMP
          WHERE dedupe_key = $dedupeKey AND stream_name = $streamName
            AND status IN ('pending', 'sync_failed')"""

  def pruneSynced(cutoff: Timestamp, limit: Int): Fragment =
    sql"""DELETE FROM sync_backlog
          WHERE (dedupe_key, stream_name) IN (
            SELECT dedupe_key, stream_name
            FROM sync_backlog
            WHERE status = 'synced' AND updated_at < $cutoff
            ORDER BY updated_at, stream_name, dedupe_key
            LIMIT ${limit.max(1)}
          )"""
