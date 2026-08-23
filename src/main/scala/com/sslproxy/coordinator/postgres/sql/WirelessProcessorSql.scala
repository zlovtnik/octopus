package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import doobie.{ConnectionIO, Fragment}
import doobie.implicits.*

object WirelessProcessorSql:
  def normalize(limit: Int): ConnectionIO[Int] =
    val batchLimit = limit.max(1)

    val frames =
      sql"""INSERT INTO wireless_frames (
               dedupe_key, sensor_id, location_id, schema_version, observed_at,
               frame_type, frame_subtype, source_mac, transmitter_mac, receiver_mac,
               bssid, destination_bssid, bssid_oui, ssid, created_at, updated_at
             )
             SELECT e.dedupe_key, e.sensor_id, e.location_id, COALESCE(e.schema_version, 1), e.observed_at,
                    e.frame_type, e.frame_subtype, e.source_mac, e.transmitter_mac, e.receiver_mac,
                    e.bssid, e.destination_bssid,
                    CASE
                      WHEN COALESCE(e.bssid, e.source_mac) IS NULL THEN NULL
                      ELSE UPPER(REPLACE(LEFT(COALESCE(e.bssid, e.source_mac), 8), ':', ''))
                    END,
                    e.ssid, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
             FROM sync_events e
             LEFT JOIN wireless_frames frame ON frame.dedupe_key = e.dedupe_key
             WHERE e.stream_name = 'wireless.audit'
               AND e.payload_archived = false
               AND e.payload IS NOT NULL
               AND frame.dedupe_key IS NULL
             ORDER BY e.observed_at, e.dedupe_key
             LIMIT $batchLimit
             ON CONFLICT (dedupe_key) DO UPDATE SET updated_at = EXCLUDED.updated_at""".update.run

    def radio: ConnectionIO[Int] =
      sql"""INSERT INTO wireless_frame_radio (
               dedupe_key, signal_dbm, noise_dbm, frequency_mhz, channel_flags,
               data_rate_kbps, antenna_id, tsft, fragment_number, channel_number,
               tsft_delta_us, wall_clock_delta_ms
             )
             SELECT e.dedupe_key, e.signal_dbm, e.noise_dbm, e.frequency_mhz, e.channel_flags,
                    e.data_rate_kbps, e.antenna_id, e.tsft, e.fragment_number, e.channel_number,
                    e.tsft_delta_us, e.wall_clock_delta_ms
             FROM sync_events e
             JOIN wireless_frames frame ON frame.dedupe_key = e.dedupe_key
             LEFT JOIN wireless_frame_radio child ON child.dedupe_key = e.dedupe_key
             WHERE e.stream_name = 'wireless.audit' AND child.dedupe_key IS NULL
             ORDER BY e.observed_at, e.dedupe_key
             LIMIT $batchLimit
             ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key""".update.run

    def qos: ConnectionIO[Int] =
      sql"""INSERT INTO wireless_frame_qos (
               dedupe_key, qos_tid, qos_eosp, qos_ack_policy, qos_ack_policy_label,
               qos_amsdu, more_data, retry, power_save, protected, frame_control_flags
             )
             SELECT e.dedupe_key, e.qos_tid, e.qos_eosp, e.qos_ack_policy, e.qos_ack_policy_label,
                    e.qos_amsdu, e.more_data, e.retry, e.power_save, e.protected, e.frame_control_flags
             FROM sync_events e
             JOIN wireless_frames frame ON frame.dedupe_key = e.dedupe_key
             LEFT JOIN wireless_frame_qos child ON child.dedupe_key = e.dedupe_key
             WHERE e.stream_name = 'wireless.audit' AND child.dedupe_key IS NULL
             ORDER BY e.observed_at, e.dedupe_key
             LIMIT $batchLimit
             ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key""".update.run

    def network: ConnectionIO[Int] =
      sql"""INSERT INTO wireless_frame_network (
               dedupe_key, llc_oui, ethertype, ethertype_name, src_ip, dst_ip,
               ip_ttl, ip_protocol, ip_protocol_name, src_port, dst_port,
               transport_protocol, transport_length, transport_checksum, app_protocol
             )
             SELECT e.dedupe_key, e.llc_oui, e.ethertype, e.ethertype_name, e.src_ip, e.dst_ip,
                    e.ip_ttl, e.ip_protocol, e.ip_protocol_name, e.src_port, e.dst_port,
                    e.transport_protocol, e.transport_length, e.transport_checksum, e.app_protocol
             FROM sync_events e
             JOIN wireless_frames frame ON frame.dedupe_key = e.dedupe_key
             LEFT JOIN wireless_frame_network child ON child.dedupe_key = e.dedupe_key
             WHERE e.stream_name = 'wireless.audit' AND child.dedupe_key IS NULL
             ORDER BY e.observed_at, e.dedupe_key
             LIMIT $batchLimit
             ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key""".update.run

    def appSignals: ConnectionIO[Int] =
      sql"""INSERT INTO wireless_frame_app_signals (
               dedupe_key, ssdp_message_type, ssdp_st, ssdp_mx, ssdp_usn,
               dhcp_requested_ip, dhcp_hostname, dhcp_vendor_class, dns_query_name, mdns_name
             )
             SELECT e.dedupe_key, e.ssdp_message_type, e.ssdp_st, e.ssdp_mx, e.ssdp_usn,
                    e.dhcp_requested_ip, e.dhcp_hostname, e.dhcp_vendor_class, e.dns_query_name, e.mdns_name
             FROM sync_events e
             JOIN wireless_frames frame ON frame.dedupe_key = e.dedupe_key
             LEFT JOIN wireless_frame_app_signals child ON child.dedupe_key = e.dedupe_key
             WHERE e.stream_name = 'wireless.audit' AND child.dedupe_key IS NULL
             ORDER BY e.observed_at, e.dedupe_key
             LIMIT $batchLimit
             ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key""".update.run

    def identity: ConnectionIO[Int] =
      sql"""INSERT INTO wireless_frame_identity (
               dedupe_key, username, event_type, session_key, retransmit_key,
               frame_fingerprint, payload_visibility, identity_source, device_fingerprint,
               wps_device_name, wps_manufacturer, wps_model_name, handshake_captured, normalized_text
             )
             SELECT e.dedupe_key, e.username, e.event_type, e.session_key, e.retransmit_key,
                    e.frame_fingerprint, e.payload_visibility, e.identity_source, e.device_fingerprint,
                    e.wps_device_name, e.wps_manufacturer, e.wps_model_name,
                    e.handshake_captured, e.wireless_search_text
             FROM sync_events e
             JOIN wireless_frames frame ON frame.dedupe_key = e.dedupe_key
             LEFT JOIN wireless_frame_identity child ON child.dedupe_key = e.dedupe_key
             WHERE e.stream_name = 'wireless.audit' AND child.dedupe_key IS NULL
             ORDER BY e.observed_at, e.dedupe_key
             LIMIT $batchLimit
             ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key""".update.run

    def security: ConnectionIO[Int] =
      sql"""INSERT INTO wireless_frame_security (
               dedupe_key, large_frame, mixed_encryption, dedupe_or_replay_suspect,
               raw_len, security_flags, risk_score, tags, signal_status, adjacent_mac_hint
             )
             SELECT e.dedupe_key, e.large_frame, e.mixed_encryption, e.dedupe_or_replay_suspect,
                    e.raw_len, e.security_flags, e.risk_score, COALESCE(e.tags, jsonb_build_array()),
                    e.signal_status, e.adjacent_mac_hint
             FROM sync_events e
             JOIN wireless_frames frame ON frame.dedupe_key = e.dedupe_key
             LEFT JOIN wireless_frame_security child ON child.dedupe_key = e.dedupe_key
             WHERE e.stream_name = 'wireless.audit' AND child.dedupe_key IS NULL
             ORDER BY e.observed_at, e.dedupe_key
             LIMIT $batchLimit
             ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key""".update.run

    for
      frameCount <- frames
      radioCount <- radio
      qosCount <- qos
      networkCount <- network
      appSignalCount <- appSignals
      identityCount <- identity
      securityCount <- security
    yield frameCount + radioCount + qosCount + networkCount +
      appSignalCount + identityCount + securityCount

  def projectInventory(limit: Int): ConnectionIO[Int] =
    val batchLimit = limit.max(1)
    val candidates = inventoryCandidates(batchLimit)
    val devices =
      (fr"""INSERT INTO devices (
               mac_id, display_name, username, hostname, os_hint, mac_hint,
               first_seen, last_seen, entity_version, updated_at
             )
             SELECT frame.source_mac,
                    MAX(NULLIF(identity_row.wps_device_name, '')),
                    MAX(NULLIF(identity_row.username, '')),
                    MAX(NULLIF(app_row.dhcp_hostname, '')),
                    MAX(NULLIF(identity_row.wps_manufacturer, '')),
                    frame.source_mac,
                    MIN(frame.observed_at), MAX(frame.observed_at), 1, CURRENT_TIMESTAMP
             FROM wireless_frames frame
             JOIN (""" ++ candidates ++ fr""") candidate
               ON candidate.dedupe_key = frame.dedupe_key
             LEFT JOIN wireless_frame_identity identity_row ON identity_row.dedupe_key = frame.dedupe_key
             LEFT JOIN wireless_frame_app_signals app_row ON app_row.dedupe_key = frame.dedupe_key
             WHERE frame.source_mac IS NOT NULL
             GROUP BY frame.source_mac
             ON CONFLICT (mac_id) DO UPDATE SET
               entity_version = devices.entity_version + 1,
               display_name = COALESCE(EXCLUDED.display_name, devices.display_name),
               username = COALESCE(EXCLUDED.username, devices.username),
               hostname = COALESCE(EXCLUDED.hostname, devices.hostname),
               os_hint = COALESCE(EXCLUDED.os_hint, devices.os_hint),
               first_seen = LEAST(devices.first_seen, EXCLUDED.first_seen),
               last_seen = GREATEST(devices.last_seen, EXCLUDED.last_seen),
               updated_at = CURRENT_TIMESTAMP""").update.run

    val clients =
      (fr"""INSERT INTO wireless_clients (
               ssid, client_mac, known_bssid, first_seen, last_seen,
               probe_count, location_id, updated_at
             )
             SELECT frame.ssid, frame.source_mac, MAX(frame.bssid),
                    MIN(frame.observed_at), MAX(frame.observed_at), COUNT(*),
                    MAX(frame.location_id), CURRENT_TIMESTAMP
             FROM wireless_frames frame
             JOIN (""" ++ candidates ++ fr""") candidate
               ON candidate.dedupe_key = frame.dedupe_key
             WHERE frame.source_mac IS NOT NULL AND frame.ssid IS NOT NULL
             GROUP BY frame.ssid, frame.source_mac
             ON CONFLICT (ssid, client_mac) DO UPDATE SET
               known_bssid = COALESCE(EXCLUDED.known_bssid, wireless_clients.known_bssid),
               first_seen = LEAST(wireless_clients.first_seen, EXCLUDED.first_seen),
               last_seen = GREATEST(wireless_clients.last_seen, EXCLUDED.last_seen),
               probe_count = wireless_clients.probe_count + EXCLUDED.probe_count,
               location_id = COALESCE(EXCLUDED.location_id, wireless_clients.location_id),
               updated_at = CURRENT_TIMESTAMP""").update.run

    val markInputs =
      (fr"""INSERT INTO wireless_inventory_projection_inputs (dedupe_key, projected_at)
             SELECT candidate.dedupe_key, CURRENT_TIMESTAMP
             FROM (""" ++ candidates ++ fr""") candidate
             ON CONFLICT (dedupe_key) DO UPDATE SET dedupe_key = EXCLUDED.dedupe_key""").update.run

    for
      deviceCount <- devices
      clientCount <- clients
      _ <- markInputs
    yield deviceCount + clientCount

  private def inventoryCandidates(batchLimit: Int): Fragment =
    fr"""SELECT candidate.dedupe_key
         FROM wireless_frames candidate
         WHERE NOT EXISTS (
           SELECT 1 FROM wireless_inventory_projection_inputs applied
           WHERE applied.dedupe_key = candidate.dedupe_key
         )
         ORDER BY candidate.created_at, candidate.dedupe_key
         LIMIT $batchLimit"""
