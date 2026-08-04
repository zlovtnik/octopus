package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import doobie.ConnectionIO
import doobie.implicits.*

object ProjectionSql:
  def generateShadowAlerts(
      windowSeconds: Int,
      signalThresholdDbm: Int,
      presenceWindowSeconds: Int
  ): ConnectionIO[List[String]] =
    val window = windowSeconds.max(1)
    val presenceWindow = presenceWindowSeconds.max(1)
    val insert =
      sql"""INSERT INTO wireless_shadow_alerts (
               source_mac, first_occurred_at, last_occurred_at, occurrence_count,
               destination_bssid, ssid, sensor_id, location_id, signal_dbm,
               reason, evidence, created_at, updated_at
             )
             SELECT
               w.source_mac, w.observed_at, w.observed_at, 1,
               w.destination_bssid, w.ssid, w.sensor_id, w.location_id, w.signal_dbm,
               'strong_wireless_without_proxy_presence',
               JSON_OBJECT(
                 'window_seconds', $window,
                 'signal_threshold_dbm', $signalThresholdDbm,
                 'presence_window_seconds', $presenceWindow
               ),
               CURRENT_TIMESTAMP(6), CURRENT_TIMESTAMP(6)
             FROM (
               SELECT DISTINCT
                 e.dedupe_key,
                 e.source_mac,
                 e.observed_at,
                 COALESCE(e.destination_bssid, e.bssid) AS destination_bssid,
                 e.ssid,
                 e.sensor_id,
                 e.location_id,
                 e.signal_dbm
               FROM sync_events e
               WHERE e.stream_name = 'wireless.audit'
                 AND e.observed_at >= TIMESTAMPADD(SECOND, -$window, CURRENT_TIMESTAMP(6))
                 AND e.payload IS NOT NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM wireless_shadow_alert_inputs applied
                   WHERE applied.dedupe_key = e.dedupe_key
                 )
             ) w
             WHERE w.source_mac IS NOT NULL
               AND w.source_mac REGEXP '^[0-9a-f]{2}(:[0-9a-f]{2}){5}$$'
               AND w.signal_dbm >= $signalThresholdDbm
               AND NOT EXISTS (
                 SELECT 1 FROM wireless_authorized_networks awn
                 WHERE awn.enabled = TRUE
                   AND (awn.location_id IS NULL OR awn.location_id = w.location_id)
                   AND (awn.ssid IS NULL OR (w.ssid IS NOT NULL AND awn.ssid = w.ssid))
                   AND (awn.bssid IS NULL OR (w.destination_bssid IS NOT NULL AND awn.bssid = w.destination_bssid))
               )
               AND NOT EXISTS (
                 SELECT 1 FROM devices d
                 WHERE d.mac_id = w.source_mac
                   AND d.last_seen >= TIMESTAMPADD(SECOND, -$presenceWindow, CURRENT_TIMESTAMP(6))
               )
             ON DUPLICATE KEY UPDATE
               last_occurred_at = GREATEST(wireless_shadow_alerts.last_occurred_at, VALUES(last_occurred_at)),
               occurrence_count = wireless_shadow_alerts.occurrence_count + 1,
               destination_bssid = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(destination_bssid), wireless_shadow_alerts.destination_bssid),
               ssid = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(ssid), wireless_shadow_alerts.ssid),
               sensor_id = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(sensor_id), wireless_shadow_alerts.sensor_id),
               location_id = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(location_id), wireless_shadow_alerts.location_id),
               signal_dbm = IF(VALUES(last_occurred_at) >= wireless_shadow_alerts.last_occurred_at, VALUES(signal_dbm), wireless_shadow_alerts.signal_dbm),
               updated_at = CURRENT_TIMESTAMP(6)""".update.run

    val markInputs =
      sql"""INSERT INTO wireless_shadow_alert_inputs (dedupe_key, source_mac, projected_at)
             SELECT w.dedupe_key, w.source_mac, CURRENT_TIMESTAMP(6)
             FROM (
               SELECT DISTINCT
                 e.dedupe_key,
                 e.source_mac,
                 COALESCE(e.destination_bssid, e.bssid) AS destination_bssid,
                 e.ssid,
                 e.location_id,
                 e.signal_dbm
               FROM sync_events e
               WHERE e.stream_name = 'wireless.audit'
                 AND e.observed_at >= TIMESTAMPADD(SECOND, -$window, CURRENT_TIMESTAMP(6))
                 AND e.payload IS NOT NULL
                 AND NOT EXISTS (
                   SELECT 1 FROM wireless_shadow_alert_inputs applied
                   WHERE applied.dedupe_key = e.dedupe_key
                 )
             ) w
             WHERE w.source_mac IS NOT NULL
               AND w.source_mac REGEXP '^[0-9a-f]{2}(:[0-9a-f]{2}){5}$$'
               AND w.signal_dbm >= $signalThresholdDbm
               AND NOT EXISTS (
                 SELECT 1 FROM wireless_authorized_networks awn
                 WHERE awn.enabled = TRUE
                   AND (awn.location_id IS NULL OR awn.location_id = w.location_id)
                   AND (awn.ssid IS NULL OR (w.ssid IS NOT NULL AND awn.ssid = w.ssid))
                   AND (awn.bssid IS NULL OR (w.destination_bssid IS NOT NULL AND awn.bssid = w.destination_bssid))
               )
               AND NOT EXISTS (
                 SELECT 1 FROM devices d
                 WHERE d.mac_id = w.source_mac
                   AND d.last_seen >= TIMESTAMPADD(SECOND, -$presenceWindow, CURRENT_TIMESTAMP(6))
               )
             ON DUPLICATE KEY UPDATE dedupe_key = VALUES(dedupe_key)""".update.run

    val select =
      sql"""SELECT JSON_OBJECT(
               'event_type', 'shadow_device',
               'first_occurred_at', first_occurred_at,
               'last_occurred_at', last_occurred_at,
               'source_mac', source_mac,
               'occurrence_count', occurrence_count,
               'destination_bssid', destination_bssid,
               'ssid', ssid,
               'sensor_id', sensor_id,
               'location_id', location_id,
               'signal_dbm', signal_dbm,
               'reason', reason,
               'evidence', evidence
             ) AS alert_json
             FROM wireless_shadow_alerts
             WHERE updated_at >= TIMESTAMPADD(SECOND, -2, CURRENT_TIMESTAMP(6))"""
        .query[String]
        .to[List]

    insert *> markInputs *> select
