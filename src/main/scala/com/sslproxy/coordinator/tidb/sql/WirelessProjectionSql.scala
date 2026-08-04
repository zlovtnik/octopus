package com.sslproxy.coordinator.tidb.sql

import cats.syntax.all.*
import doobie.{ConnectionIO, Fragment}
import doobie.implicits.*

object WirelessProjectionSql:
  private def jsonExtract(path: String): Fragment =
    fr0"JSON_EXTRACT(payload, $path)"

  private def jsonType(path: String): Fragment =
    val extracted = jsonExtract(path)
    fr0"JSON_TYPE($extracted)"

  private def jsonUnquoted(path: String): Fragment =
    val extracted = jsonExtract(path)
    fr0"JSON_UNQUOTE($extracted)"

  private def coalesceJson(projections: List[Fragment]): Fragment =
    fr0"COALESCE(" ++ projections.intercalate(fr0", ") ++ fr0")"

  private def jsonText(maxLength: Int, path: String, aliases: String*): Fragment =
    jsonText(maxLength, preserveEmpty = false, path, aliases*)

  private def jsonPresentText(maxLength: Int, path: String, aliases: String*): Fragment =
    jsonText(maxLength, preserveEmpty = true, path, aliases*)

  private def jsonText(
      maxLength: Int,
      preserveEmpty: Boolean,
      path: String,
      aliases: String*
  ): Fragment =
    coalesceJson((path :: aliases.toList).map { candidate =>
      val candidateType = jsonType(candidate)
      val candidateText = jsonUnquoted(candidate)
      val projected = if preserveEmpty then candidateText else fr0"NULLIF($candidateText, '')"
      fr0"""CASE
           WHEN $candidateType = 'STRING'
            AND CHAR_LENGTH($candidateText) <= $maxLength
           THEN $projected
           ELSE NULL
         END"""
    })

  private def jsonInteger(
      path: String,
      aliases: String*
  )(
      positiveMax: String = "2147483647",
      negativeMagnitudeMax: String = "2147483648"
  ): Fragment =
    coalesceJson((path :: aliases.toList).map { candidate =>
      val candidateType = jsonType(candidate)
      val candidateText = jsonUnquoted(candidate)
      val trimmed = fr0"TRIM($candidateText)"
      val magnitude =
        fr0"COALESCE(NULLIF(TRIM(LEADING '0' FROM TRIM(LEADING '-' FROM TRIM(LEADING '+' FROM $trimmed))), ''), '0')"
      val magnitudeLimit =
        fr0"CASE WHEN LEFT($trimmed, 1) = '-' THEN $negativeMagnitudeMax ELSE $positiveMax END"
      val safeText =
        fr0"""CASE
             WHEN $candidateType IN ('INTEGER', 'UNSIGNED INTEGER', 'STRING')
              AND REGEXP_LIKE($trimmed, '^[+-]?[0-9]+$$')
              AND (
                CHAR_LENGTH($magnitude) < CHAR_LENGTH($magnitudeLimit)
                OR (
                  CHAR_LENGTH($magnitude) = CHAR_LENGTH($magnitudeLimit)
                  AND $magnitude <= $magnitudeLimit
                )
              )
             THEN $trimmed
             ELSE NULL
           END"""
      fr0"CAST($safeText AS SIGNED)"
    })

  private def jsonDouble(path: String, aliases: String*): Fragment =
    coalesceJson((path :: aliases.toList).map { candidate =>
      val candidateType = jsonType(candidate)
      val candidateText = jsonUnquoted(candidate)
      val trimmed = fr0"TRIM($candidateText)"
      val scientific = fr0"REGEXP_LIKE($trimmed, '^[+-]?[0-9]([.][0-9]+)?[eE][+-]?[0-9]{1,3}$$')"
      val exponent =
        fr0"CAST(CASE WHEN $scientific THEN SUBSTRING_INDEX(LOWER($trimmed), 'e', -1) ELSE NULL END AS SIGNED)"
      val mantissa =
        fr0"CAST(CASE WHEN $scientific THEN SUBSTRING_INDEX(LOWER($trimmed), 'e', 1) ELSE NULL END AS DECIMAL(18,16))"
      val safeText =
        fr0"""CASE
             WHEN $candidateType IN ('INTEGER', 'UNSIGNED INTEGER', 'DOUBLE', 'STRING')
              AND (
                REGEXP_LIKE($trimmed, '^[+-]?([0-9]{1,308}([.][0-9]*)?|[.][0-9]+)$$')
                OR (
                  $scientific
                  AND $exponent BETWEEN -324 AND 308
                  AND ($exponent < 308 OR ABS($mantissa) <= 1.7976931348623157)
                )
              )
             THEN $trimmed
             ELSE NULL
           END"""
      fr0"CAST($safeText AS DOUBLE)"
    })

  private def jsonBoolean(path: String, aliases: String*): Fragment =
    coalesceJson((path :: aliases.toList).map { candidate =>
      val candidateType = jsonType(candidate)
      val candidateText = jsonUnquoted(candidate)
      fr0"""CASE
             WHEN $candidateType IN ('BOOLEAN', 'INTEGER', 'UNSIGNED INTEGER', 'STRING') THEN
               CASE LOWER(TRIM($candidateText))
                 WHEN 'true' THEN 1
                 WHEN 'false' THEN 0
                 WHEN '1' THEN 1
                 WHEN '0' THEN 0
                 ELSE NULL
               END
             ELSE NULL
           END"""
    })

  private def jsonArray(path: String): Fragment =
    val extracted = jsonExtract(path)
    val extractedType = jsonType(path)
    fr0"CASE WHEN $extractedType = 'ARRAY' THEN $extracted ELSE NULL END"

  def hydrate(dedupeKey: String): ConnectionIO[Int] =
    val intMax = "2147483647"
    val intMinMagnitude = "2147483648"
    val longMax = "9223372036854775807"
    val longMinMagnitude = "9223372036854775808"

    val project =
      val sensorId = jsonText(64, "$.sensor_id")
      val locationId = jsonText(128, "$.location_id")
      val username = jsonText(255, "$.username")
      val eventType = jsonText(64, "$.event_type", "$.type")
      val schemaVersion = jsonInteger("$.schema_version")()
      val frameType = jsonText(32, "$.frame_type", "$.mac.frame_type")
      val frameSubtype = jsonText(64, "$.frame_subtype", "$.mac.frame_subtype")
      val sourceMac = jsonText(17, "$.source_mac", "$.mac.source_mac")
      val transmitterMac = jsonText(17, "$.transmitter_mac", "$.mac.transmitter_mac")
      val receiverMac = jsonText(17, "$.receiver_mac", "$.mac.receiver_mac")
      val bssid = jsonText(17, "$.bssid", "$.mac.bssid")
      val destinationBssid = jsonText(
        17,
        "$.destination_bssid",
        "$.destination_mac",
        "$.mac.destination_mac",
        "$.mac.bssid"
      )
      val ssid = jsonPresentText(256, "$.ssid")
      val signalDbm = jsonInteger("$.signal_dbm", "$.rf.signal_dbm")()
      val noiseDbm = jsonInteger("$.noise_dbm", "$.rf.noise_dbm")()
      val frequencyMhz = jsonInteger("$.frequency_mhz", "$.rf.frequency_mhz")()
      val channelFlags = jsonInteger("$.channel_flags", "$.rf.channel_flags.raw")()
      val dataRateKbps = jsonInteger("$.data_rate_kbps", "$.rf.data_rate_kbps")()
      val antennaId = jsonInteger("$.antenna_id", "$.rf.antenna_id")()
      val tsft = jsonInteger("$.tsft", "$.rf.tsft")(longMax, longMinMagnitude)
      val fragmentNumber = jsonInteger("$.fragment_number", "$.mac.fragment_number")()
      val channelNumber = jsonInteger("$.channel_number", "$.channel", "$.rf.channel_number")()
      val signalStatus = jsonText(64, "$.signal_status", "$.rf.signal_status")
      val adjacentMacHint = jsonText(512, "$.adjacent_mac_hint", "$.mac.adjacent_mac_hint")
      val qosTid = jsonInteger("$.qos_tid", "$.qos.tid")()
      val qosEosp = jsonBoolean("$.qos_eosp", "$.qos.eosp")
      val qosAckPolicy = jsonInteger("$.qos_ack_policy", "$.qos.ack_policy")()
      val qosAckPolicyLabel = jsonText(64, "$.qos_ack_policy_label", "$.qos.ack_policy_label")
      val qosAmsdu = jsonBoolean("$.qos_amsdu", "$.qos.amsdu")
      val llcOui = jsonText(16, "$.llc_oui", "$.llc_snap.oui")
      val ethertype = jsonInteger("$.ethertype", "$.llc_snap.ethertype")()
      val ethertypeName = jsonText(64, "$.ethertype_name", "$.llc_snap.ethertype_name")
      val srcIp = jsonText(45, "$.src_ip", "$.network.src_ip")
      val dstIp = jsonText(45, "$.dst_ip", "$.network.dst_ip")
      val ipTtl = jsonInteger("$.ip_ttl", "$.network.ttl")()
      val ipProtocol = jsonInteger("$.ip_protocol", "$.network.protocol")()
      val ipProtocolName = jsonText(64, "$.ip_protocol_name", "$.network.protocol_name")
      val srcPort = jsonInteger("$.src_port", "$.transport.src_port")()
      val dstPort = jsonInteger("$.dst_port", "$.transport.dst_port")()
      val transportProtocol = jsonText(32, "$.transport_protocol", "$.transport.protocol")
      val transportLength = jsonInteger("$.transport_length", "$.transport.length")()
      val transportChecksum = jsonInteger("$.transport_checksum", "$.transport.checksum")()
      val appProtocol = jsonText(64, "$.app_protocol", "$.application.protocol")
      val ssdpMessageType = jsonText(64, "$.ssdp_message_type", "$.application.ssdp.message_type")
      val ssdpSt = jsonText(512, "$.ssdp_st", "$.application.ssdp.st")
      val ssdpMx = jsonText(64, "$.ssdp_mx", "$.application.ssdp.mx")
      val ssdpUsn = jsonText(512, "$.ssdp_usn", "$.application.ssdp.usn")
      val dhcpRequestedIp = jsonText(45, "$.dhcp_requested_ip", "$.application.dhcp.requested_ip")
      val dhcpHostname = jsonText(253, "$.dhcp_hostname", "$.application.dhcp.hostname")
      val dhcpVendorClass = jsonText(255, "$.dhcp_vendor_class", "$.application.dhcp.vendor_class")
      val dnsQueryName = jsonText(253, "$.dns_query_name", "$.application.dns.query_names[0]")
      val mdnsName = jsonText(253, "$.mdns_name", "$.application.mdns.query_names[0]")
      val sessionKey = jsonText(255, "$.session_key", "$.correlation.session_key")
      val retransmitKey = jsonText(255, "$.retransmit_key", "$.correlation.retransmit_key")
      val frameFingerprint = jsonText(255, "$.frame_fingerprint", "$.correlation.frame_fingerprint")
      val payloadVisibility = jsonText(64, "$.payload_visibility", "$.correlation.payload_visibility")
      val tsftDeltaUs = jsonInteger("$.tsft_delta_us", "$.correlation.tsft_delta_us")(
        longMax,
        longMinMagnitude
      )
      val wallClockDeltaMs = jsonInteger(
        "$.wall_clock_delta_ms",
        "$.correlation.wall_clock_delta_ms"
      )(longMax, longMinMagnitude)
      val largeFrame = jsonBoolean("$.large_frame", "$.anomalies.large_frame")
      val mixedEncryption = jsonBoolean("$.mixed_encryption", "$.anomalies.mixed_encryption")
      val dedupeOrReplaySuspect = jsonBoolean(
        "$.dedupe_or_replay_suspect",
        "$.anomalies.dedupe_or_replay_suspect"
      )
      val rawLen = jsonInteger("$.raw_len", "$.rf.raw_len")(intMax, intMinMagnitude)
      val frameControlFlags = jsonInteger("$.frame_control_flags")(intMax, intMinMagnitude)
      val moreData = jsonBoolean("$.more_data", "$.mac.more_data")
      val retry = jsonBoolean("$.retry", "$.mac.retry")
      val powerSave = jsonBoolean("$.power_save", "$.mac.power_save")
      val protectedFlag = jsonBoolean("$.protected", "$.mac.protected")
      val securityFlags = jsonInteger("$.security_flags")(intMax, intMinMagnitude)
      val riskScore = jsonDouble("$.risk_score")
      val identitySource = jsonText(64, "$.identity_source")
      val tags = jsonArray("$.tags")
      val wpsDeviceName = jsonText(255, "$.wps_device_name")
      val wpsManufacturer = jsonText(255, "$.wps_manufacturer")
      val wpsModelName = jsonText(255, "$.wps_model_name")
      val deviceFingerprint = jsonText(255, "$.device_fingerprint")
      val handshakeCaptured = jsonBoolean("$.handshake_captured")

      fr"""UPDATE sync_events
            SET
              sensor_id = $sensorId,
              location_id = $locationId,
              username = $username,
              event_type = $eventType,
              schema_version = COALESCE($schemaVersion, 1),
              frame_type = $frameType,
              frame_subtype = $frameSubtype,
              source_mac = LOWER($sourceMac),
              transmitter_mac = LOWER($transmitterMac),
              receiver_mac = LOWER($receiverMac),
              bssid = LOWER($bssid),
              destination_bssid = LOWER($destinationBssid),
              ssid = $ssid,
              signal_dbm = $signalDbm,
              noise_dbm = $noiseDbm,
              frequency_mhz = $frequencyMhz,
              channel_flags = $channelFlags,
              data_rate_kbps = $dataRateKbps,
              antenna_id = $antennaId,
              tsft = $tsft,
              fragment_number = $fragmentNumber,
              channel_number = $channelNumber,
              signal_status = $signalStatus,
              adjacent_mac_hint = LOWER($adjacentMacHint),
              qos_tid = $qosTid,
              qos_eosp = $qosEosp,
              qos_ack_policy = $qosAckPolicy,
              qos_ack_policy_label = $qosAckPolicyLabel,
              qos_amsdu = $qosAmsdu,
              llc_oui = $llcOui,
              ethertype = $ethertype,
              ethertype_name = $ethertypeName,
              src_ip = $srcIp,
              dst_ip = $dstIp,
              ip_ttl = $ipTtl,
              ip_protocol = $ipProtocol,
              ip_protocol_name = $ipProtocolName,
              src_port = $srcPort,
              dst_port = $dstPort,
              transport_protocol = $transportProtocol,
              transport_length = $transportLength,
              transport_checksum = $transportChecksum,
              app_protocol = $appProtocol,
              ssdp_message_type = $ssdpMessageType,
              ssdp_st = $ssdpSt,
              ssdp_mx = $ssdpMx,
              ssdp_usn = $ssdpUsn,
              dhcp_requested_ip = $dhcpRequestedIp,
              dhcp_hostname = $dhcpHostname,
              dhcp_vendor_class = $dhcpVendorClass,
              dns_query_name = $dnsQueryName,
              mdns_name = $mdnsName,
              session_key = $sessionKey,
              retransmit_key = $retransmitKey,
              frame_fingerprint = $frameFingerprint,
              payload_visibility = $payloadVisibility,
              tsft_delta_us = $tsftDeltaUs,
              wall_clock_delta_ms = $wallClockDeltaMs,
              large_frame = COALESCE($largeFrame, 0),
              mixed_encryption = $mixedEncryption,
              dedupe_or_replay_suspect = COALESCE($dedupeOrReplaySuspect, 0),
              raw_len = COALESCE($rawLen, 0),
              frame_control_flags = COALESCE($frameControlFlags, 0),
              more_data = COALESCE($moreData, 0),
              retry = COALESCE($retry, 0),
              power_save = COALESCE($powerSave, 0),
              protected = COALESCE($protectedFlag, 0),
              security_flags = COALESCE($securityFlags, 0),
              risk_score = $riskScore,
              identity_source = $identitySource,
              tags = $tags,
              wps_device_name = $wpsDeviceName,
              wps_manufacturer = $wpsManufacturer,
              wps_model_name = $wpsModelName,
              device_fingerprint = $deviceFingerprint,
              handshake_captured = COALESCE($handshakeCaptured, 0),
              updated_at = CURRENT_TIMESTAMP(6)
            WHERE dedupe_key = $dedupeKey
              AND stream_name = 'wireless.audit'
              AND payload_archived = 0
              AND NOT EXISTS (
                SELECT 1
                FROM sync_event_tombstones tombstone
                WHERE tombstone.dedupe_key = sync_events.dedupe_key
                  AND tombstone.stream_name = sync_events.stream_name
                  AND tombstone.expires_at > CURRENT_TIMESTAMP(6)
              )
              AND payload IS NOT NULL""".update.run

    for
      projected <- project
      _ <- sql"""UPDATE sync_events
             SET wireless_search_text = NULLIF(LOWER(CONCAT_WS(
                   ' ', sensor_id, source_mac, bssid, destination_bssid, ssid,
                   wps_device_name, wps_manufacturer, wps_model_name,
                   device_fingerprint, app_protocol, src_ip, dst_ip, username
                 )), '')
             WHERE dedupe_key = $dedupeKey
               AND stream_name = 'wireless.audit'
               AND payload_archived = 0
               AND NOT EXISTS (
                 SELECT 1
                 FROM sync_event_tombstones tombstone
                 WHERE tombstone.dedupe_key = sync_events.dedupe_key
                   AND tombstone.stream_name = sync_events.stream_name
                   AND tombstone.expires_at > CURRENT_TIMESTAMP(6)
               )
               AND payload IS NOT NULL""".update.run
    yield projected
