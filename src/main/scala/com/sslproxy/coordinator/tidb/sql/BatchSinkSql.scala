package com.sslproxy.coordinator.tidb.sql

object BatchSinkSql:
  val ConnectivityQuery: String = "SELECT 1"

  val InsertProxyEvents: String =
    """INSERT INTO proxy_events (
      |  batch_id, row_sequence, event_timestamp_utc, event_time, event_type, host,
      |  peer_ip, wg_pubkey, device_id, identity_source, peer_hostname, client_ua,
      |  bytes_up, bytes_down, status_code, blocked, obfuscation_profile,
      |  correlation_id, parent_event_id, event_sequence, duration_ms, reason, raw_json
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

  val UpsertBlockedHostRollups: String =
    """INSERT INTO proxy_blocked_host_rollups (
      |  host, blocked_attempts, blocked_bytes, frequency_hz, verdict, category,
      |  risk_score, tarpit_held_ms, iat_ms, consecutive_blocks, last_verdict,
      |  tls_ver, alpn, ja3_lite, resolved_ip, asn_org, updated_at, first_seen
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE
      |  blocked_attempts = blocked_attempts + 1,
      |  blocked_bytes = blocked_bytes + VALUES(blocked_bytes),
      |  frequency_hz = IFNULL(VALUES(frequency_hz), frequency_hz),
      |  verdict = IFNULL(VALUES(verdict), verdict),
      |  category = IFNULL(VALUES(category), category),
      |  risk_score = IFNULL(VALUES(risk_score), risk_score),
      |  tarpit_held_ms = tarpit_held_ms + IFNULL(VALUES(tarpit_held_ms), 0),
      |  iat_ms = IFNULL(VALUES(iat_ms), iat_ms),
      |  consecutive_blocks = IFNULL(VALUES(consecutive_blocks), consecutive_blocks + 1),
      |  last_verdict = IFNULL(VALUES(last_verdict), IFNULL(VALUES(verdict), last_verdict)),
      |  tls_ver = IFNULL(VALUES(tls_ver), tls_ver),
      |  alpn = IFNULL(VALUES(alpn), alpn),
      |  ja3_lite = IFNULL(VALUES(ja3_lite), ja3_lite),
      |  resolved_ip = IFNULL(VALUES(resolved_ip), resolved_ip),
      |  asn_org = IFNULL(VALUES(asn_org), asn_org),
      |  updated_at = CURRENT_TIMESTAMP(6)""".stripMargin

  val InsertProxyPayloadAudit: String =
    """INSERT INTO proxy_payload_audit (
      |  correlation_id, host, direction, captured_at, byte_offset,
      |  payload_object_key, content_type, http_method, http_status, http_path,
      |  is_encrypted, truncated, peer_ip, notes
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin

  val InsertWirelessAuditFrames: String =
    """INSERT INTO wireless_audit_frames (
      |  batch_id, row_sequence, event_type, observed_at, sensor_id, location_id,
      |  interface, channel, band, frame_type, frame_subtype, bssid, source_mac,
      |  destination_mac, transmitter_mac, receiver_mac, destination_bssid, ssid,
      |  signal_dbm, sequence_number, raw_len, is_retry, is_more_data, is_power_save,
      |  is_protected, is_to_ds, is_from_ds, is_handshake, security_flags,
      |  device_id, username, identity_source, tags, anomaly_reasons, raw_json
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

  val UpsertWirelessSensors: String =
    """INSERT INTO wireless_sensors (
      |  sensor_id, location_id, interface, reg_domain, first_seen_at, last_seen_at
      |) VALUES (?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE
      |  location_id = VALUES(location_id),
      |  interface = VALUES(interface),
      |  reg_domain = VALUES(reg_domain),
      |  last_seen_at = VALUES(last_seen_at)""".stripMargin

  val InsertWirelessBandwidthWindows: String =
    """INSERT INTO wireless_bandwidth_windows (
      |  batch_id, row_sequence, schema_version, window_start, window_end,
      |  sensor_id, location_id, interface, channel, band, source_mac, destination_bssid,
      |  ssid, bytes, frame_count, retry_count, more_data_count, power_save_count,
      |  strongest_signal_dbm, hist_under_100, hist_100_500, hist_500_1000,
      |  hist_1000_1500, inter_arrival_p50_ms, external_bssid, threshold_exceeded,
      |  wall_clock_delta_ms, window_is_partial, published_at
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

  val UpsertBandwidthAlerts: String =
    """INSERT INTO wireless_alerts (
      |  alert_type, batch_id, row_sequence, alert_date, detected_at, sensor_id,
      |  location_id, primary_mac, secondary_mac, ssid, signal_dbm, details_json, raw_json,
      |  created_at, updated_at, bytes
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE
      |  batch_id = VALUES(batch_id),
      |  row_sequence = VALUES(row_sequence),
      |  detected_at = VALUES(detected_at),
      |  location_id = VALUES(location_id),
      |  ssid = VALUES(ssid),
      |  bytes = VALUES(bytes),
      |  details_json = VALUES(details_json),
      |  updated_at = CURRENT_TIMESTAMP(6)""".stripMargin

  val UpsertWirelessAlerts: String =
    """INSERT INTO wireless_alerts (
      |  alert_type, batch_id, row_sequence, detected_at, sensor_id, location_id,
      |  interface, channel, primary_mac, secondary_mac, ssid, signal_dbm,
      |  details_json, raw_json, created_at, updated_at
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE
      |  detected_at = VALUES(detected_at),
      |  sensor_id = VALUES(sensor_id),
      |  location_id = VALUES(location_id),
      |  interface = VALUES(interface),
      |  channel = VALUES(channel),
      |  primary_mac = VALUES(primary_mac),
      |  secondary_mac = VALUES(secondary_mac),
      |  ssid = VALUES(ssid),
      |  signal_dbm = VALUES(signal_dbm),
      |  details_json = VALUES(details_json),
      |  raw_json = VALUES(raw_json),
      |  updated_at = VALUES(updated_at)""".stripMargin

  val UpsertWirelessClientInventory: String =
    """INSERT INTO wireless_client_inventory (
      |  sensor_id, location_id, snapshot_at, client_mac, bssid, ssid,
      |  device_id, username, identity_source, last_seen, first_seen,
      |  signal_dbm, is_authorized, created_at
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE
      |  location_id = VALUES(location_id),
      |  bssid = VALUES(bssid),
      |  ssid = VALUES(ssid),
      |  device_id = VALUES(device_id),
      |  username = VALUES(username),
      |  identity_source = VALUES(identity_source),
      |  last_seen = VALUES(last_seen),
      |  first_seen = VALUES(first_seen),
      |  signal_dbm = VALUES(signal_dbm),
      |  is_authorized = VALUES(is_authorized)""".stripMargin

  val InsertWirelessProbeRequests: String =
    """INSERT INTO wireless_probe_requests (
      |  batch_id, row_sequence, client_mac, ssid, known_bssid,
      |  first_seen, last_seen, probe_count, created_at
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON DUPLICATE KEY UPDATE batch_id = VALUES(batch_id)""".stripMargin

object SchemaChecksSql:
  def tableLookup(tableCount: Int): Either[String, String] =
    placeholders(tableCount).map { values =>
      s"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME IN ($values)"
    }

  def columnLookup(columnCount: Int): Either[String, String] =
    if columnCount <= 0 then Left("column count must be positive")
    else
      val predicates = List.fill(columnCount)("(TABLE_NAME = ? AND COLUMN_NAME = ?)").mkString(" OR ")
      Right(s"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND ($predicates)")

  private def placeholders(count: Int): Either[String, String] =
    if count <= 0 then Left("placeholder count must be positive")
    else Right(List.fill(count)("?").mkString(", "))
