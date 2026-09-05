package com.sslproxy.coordinator.postgres.sql

import doobie.{ConnectionIO, Fragment}
import doobie.implicits.*

object ProjectionSql:
  def generateShadowAlerts(
    windowSeconds: Int,
    signalThresholdDbm: Int,
    presenceWindowSeconds: Int,
    batchLimit: Int
  ): ConnectionIO[List[String]] =
    val window = windowSeconds.max(1)
    val presenceWindow = presenceWindowSeconds.max(1)
    val limit = batchLimit.max(1)
    val runId = java.util.UUID.randomUUID().toString
    val candidates = shadowAlertCandidates(window, signalThresholdDbm, presenceWindow, limit)
    (fr"""WITH candidates AS MATERIALIZED (""" ++ candidates ++ fr"""),
             ranked AS (
               SELECT candidates.*,
                      MIN(observed_at) OVER (PARTITION BY source_mac) AS first_seen,
                      COUNT(*) OVER (PARTITION BY source_mac) AS input_count,
                      ROW_NUMBER() OVER (
                        PARTITION BY source_mac ORDER BY observed_at DESC, dedupe_key DESC
                      ) AS position
               FROM candidates
             ), upserted AS (
             INSERT INTO wireless_shadow_alerts (
               source_mac, first_occurred_at, last_occurred_at, occurrence_count,
               destination_bssid, ssid, sensor_id, location_id, signal_dbm,
               reason, evidence, created_at, updated_at, projection_run_id
             )
             SELECT
               w.source_mac, w.first_seen, w.observed_at, w.input_count,
               w.destination_bssid, w.ssid, w.sensor_id, w.location_id, w.signal_dbm,
               'strong_wireless_without_proxy_presence',
               jsonb_build_object(
                 'window_seconds', $window,
                 'signal_threshold_dbm', $signalThresholdDbm,
                 'presence_window_seconds', $presenceWindow
               ),
               CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, $runId
             FROM ranked w
             WHERE w.position = 1
             ON CONFLICT (source_mac) DO UPDATE SET
               first_occurred_at = LEAST(wireless_shadow_alerts.first_occurred_at, EXCLUDED.first_occurred_at),
               last_occurred_at = GREATEST(wireless_shadow_alerts.last_occurred_at, EXCLUDED.last_occurred_at),
               occurrence_count = wireless_shadow_alerts.occurrence_count + EXCLUDED.occurrence_count,
               destination_bssid = CASE WHEN EXCLUDED.last_occurred_at >= wireless_shadow_alerts.last_occurred_at THEN EXCLUDED.destination_bssid ELSE wireless_shadow_alerts.destination_bssid END,
               ssid = CASE WHEN EXCLUDED.last_occurred_at >= wireless_shadow_alerts.last_occurred_at THEN EXCLUDED.ssid ELSE wireless_shadow_alerts.ssid END,
               sensor_id = CASE WHEN EXCLUDED.last_occurred_at >= wireless_shadow_alerts.last_occurred_at THEN EXCLUDED.sensor_id ELSE wireless_shadow_alerts.sensor_id END,
               location_id = CASE WHEN EXCLUDED.last_occurred_at >= wireless_shadow_alerts.last_occurred_at THEN EXCLUDED.location_id ELSE wireless_shadow_alerts.location_id END,
               signal_dbm = CASE WHEN EXCLUDED.last_occurred_at >= wireless_shadow_alerts.last_occurred_at THEN EXCLUDED.signal_dbm ELSE wireless_shadow_alerts.signal_dbm END,
               projection_run_id = EXCLUDED.projection_run_id,
               updated_at = CURRENT_TIMESTAMP
             RETURNING source_mac, first_occurred_at, last_occurred_at, occurrence_count,
                       destination_bssid, ssid, sensor_id, location_id, signal_dbm, reason, evidence
             ), marked AS (
             INSERT INTO wireless_shadow_alert_inputs (dedupe_key, source_mac, projected_at)
             SELECT w.dedupe_key, w.source_mac, CURRENT_TIMESTAMP
             FROM candidates w
             ON CONFLICT (dedupe_key) DO NOTHING
             )
             SELECT jsonb_build_object(
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
             FROM upserted""")
      .query[String]
      .to[List]

  private def shadowAlertCandidates(
    windowSeconds: Int,
    signalThresholdDbm: Int,
    presenceWindowSeconds: Int,
    batchLimit: Int
  ): Fragment =
    fr"""SELECT DISTINCT
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
           AND e.observed_at >= (CURRENT_TIMESTAMP + (-$windowSeconds) * INTERVAL '1 second')
           AND e.payload IS NOT NULL
           AND NOT EXISTS (
             SELECT 1 FROM wireless_shadow_alert_inputs applied
             WHERE applied.dedupe_key = e.dedupe_key
           )
           AND e.source_mac IS NOT NULL
           AND e.source_mac ~ '^[0-9A-Fa-f]{2}(:[0-9A-Fa-f]{2}){5}$$'
           AND e.signal_dbm >= $signalThresholdDbm
           AND NOT EXISTS (
             SELECT 1 FROM wireless_authorized_networks awn
             WHERE awn.enabled = TRUE
               AND (awn.location_id IS NULL OR awn.location_id = e.location_id)
               AND (awn.ssid IS NULL OR (e.ssid IS NOT NULL AND awn.ssid = e.ssid))
               AND (
                 awn.bssid IS NULL
                 OR (
                   COALESCE(e.destination_bssid, e.bssid) IS NOT NULL
                   AND awn.bssid = COALESCE(e.destination_bssid, e.bssid)
                 )
               )
           )
           AND NOT EXISTS (
             SELECT 1 FROM devices d
             WHERE d.mac_id = e.source_mac
               AND d.last_seen >= (CURRENT_TIMESTAMP + (-$presenceWindowSeconds) * INTERVAL '1 second')
           )
         ORDER BY e.observed_at, e.dedupe_key
         LIMIT $batchLimit"""
