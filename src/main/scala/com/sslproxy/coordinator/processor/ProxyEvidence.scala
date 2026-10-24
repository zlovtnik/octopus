package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import io.circe.generic.semiauto.deriveEncoder

/** Private retained evidence and a separately whitelisted Search publication.
  * Legacy zeros and ClientHello offers never become terminal measurements.
  */
object ProxyEvidence:
  private given io.circe.Encoder[ProxyAuthority] = deriveEncoder
  val Version = "proxy-evidence-v1"
  val TemplateVersion = "proxy-event-v2"
  val AllowedEvents = Set("block", "http_blocked", "session.blocked", "http_proxied", "http_error", "tunnel_open", "tunnel_close")
  private val Classifications = Set("ads_tracker", "analytics", "cdn", "essential_api", "auth", "unknown")

  def normalized(row: Json): Json =
    val cursor = row.hcursor
    val raw = cursor.get[String]("authority_raw").toOption.orElse(cursor.get[String]("host_raw").toOption)
      .orElse(cursor.get[String]("host").toOption)
    val source = cursor.get[String]("authority_source").getOrElse("legacy_host")
    val sanitized = cursor.get[String]("sanitization_status").contains("sanitized")
    val syntax = ProxyAuthority.normalize(raw, source, sanitized)
    // Legacy host scalars can be synthesized routing authorities. Their suffix
    // is not proof that a client explicitly supplied that port.
    val parsed = if source == "legacy_host" then syntax.copy(port = None, authority_normalized = None) else syntax
    val authority = if cursor.get[Boolean]("authority_truncated").getOrElse(false) || cursor.get[Boolean]("host_truncated").getOrElse(false) then
      parsed.copy(host_normalized = None, authority_normalized = None, port = None, registrable_domain = None, parse_status = "invalid")
    else parsed
    val modern = cursor.get[String]("evidence_version").contains(Version)
    val measurement = cursor.downField("measurement")
    val supportedMeasurement = modern && measurement.get[String]("version").contains("proxy-measurement-v1")
    val declaredStatus = measurement.get[String]("bytes_status").toOption
      .filter(_ => supportedMeasurement)
      .filter(Set("measured_terminal", "partial", "unavailable")).getOrElse("legacy_unknown")
    val countersValid = List("bytes_up", "bytes_down").forall(key => cursor.get[Long](key).toOption.exists(_ >= 0))
    val status = if Set("measured_terminal", "partial")(declaredStatus) && !countersValid then "unavailable_invalid_counters" else declaredStatus
    def nonnegative(key: String): Json = cursor.get[Long](key).toOption.filter(_ >= 0).asJson
    val tls = cursor.downField("tls_evidence")
    val attribution = cursor.downField("attribution")
    val safe = Json.obj(
      "authority_status" -> (if source == "legacy_host" then "legacy_unknown" else "observed").asJson,
      "evidence_version" -> (if modern then Version else "legacy_unknown").asJson,
      "measurement" -> Json.obj(
        "bytes_status" -> status.asJson,
        "bytes_up" -> (if Set("measured_terminal", "partial").contains(status) then nonnegative("bytes_up") else Json.Null),
        "bytes_down" -> (if Set("measured_terminal", "partial").contains(status) then nonnegative("bytes_down") else Json.Null),
        "duration_status" -> measurement.get[String]("duration_status").toOption
          .filter(_ => supportedMeasurement)
          .filter(Set("connection_elapsed", "header_latency", "unavailable")).getOrElse("legacy_unknown").asJson,
        "duration_ms" -> (if supportedMeasurement then nonnegative("duration_ms") else Json.Null), "byte_unit" -> "bytes".asJson,
        "directions" -> "up_client_to_destination_down_destination_to_client".asJson
      ),
      "tls" -> Json.obj(
        "semantics" -> "clienthello_offers_not_negotiated".asJson,
        "parse_status" -> tls.get[String]("parse_status").toOption
          .filter(Set("parsed", "incomplete", "non_tls_or_invalid", "parse_failure", "non_tls", "read_error", "timeout", "unavailable")).getOrElse("legacy_unknown").asJson,
        "parser_version" -> (if modern then "proxy-clienthello-v1" else "legacy_unknown").asJson,
        "offered_version" -> tls.get[String]("offered_version").toOption.filter(Set("TLS1.0", "TLS1.1", "TLS1.2", "TLS1.3")).asJson,
        "offered_alpn" -> tls.get[String]("offered_alpn").toOption.filter(_.matches("[A-Za-z0-9,./_-]{1,64}")).asJson
      ),
      "attribution" -> Json.obj(
        "method" -> attribution.get[String]("method").toOption.filter(Set("device_token", "peer_claim", "unknown")).getOrElse("legacy_unknown").asJson,
        "valid_from" -> attribution.get[String]("valid_from").toOption.filter(timestamp).asJson,
        "valid_until" -> attribution.get[String]("valid_until").toOption.filter(timestamp).asJson,
        "mapping_revision" -> attribution.get[String]("mapping_revision").toOption.filter(timestamp).asJson,
        "peer_reference" -> attribution.get[String]("peer_reference").toOption.filter(_.matches("[0-9a-f]{64}")).asJson,
        "reference_scope" -> "process_wireguard_peer".asJson,
        "reference_rotation" -> "process_restart_or_key_change".asJson,
        "reference_expiry" -> "process_lifetime".asJson
      ),
      "coalescing" -> Json.obj(
        "status" -> cursor.downField("coalescing").get[String]("status").toOption
          .filter(Set("emitted", "not_applied", "capacity_bypass")).getOrElse("legacy_unknown").asJson,
        "suppressed_previous_window" -> cursor.downField("coalescing").get[Long]("suppressed_previous_window").toOption.filter(_ >= 0).asJson,
        "attempts" -> "unknown".asJson
      )
    )
    authority.asJson.deepMerge(safe)

  def publicEvidence(evidence: Json): Json =
    evidence.mapObject(_.remove("authority_raw").remove("host_raw"))

  def safeHost(rawHost: Option[String], sanitized: Boolean): Option[String] =
    rawHost.filter { value =>
      val token = value.matches("hmac:[A-Za-z0-9_-]+") && value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= 1024
      token || (!sanitized && ProxyAuthority.normalize(Some(value), "legacy_host").host_normalized.isDefined)
    }

  def source(row: Json, eventId: String, revision: Long, observedAt: java.sql.Timestamp): Option[SearchDocumentSource] =
    val c = row.hcursor
    val eventType = c.get[String]("type").getOrElse("")
    val rawHost = c.get[String]("host").toOption
    if !AllowedEvents(eventType) || rawHost.forall(_.isEmpty) || rawHost.exists(_.exists(ch => Character.isISOControl(ch) || Character.isWhitespace(ch))) then None
    else
      val evidence = normalized(row)
      val safe = publicEvidence(evidence)
      val host = evidence.hcursor.get[String]("host_normalized").toOption
      val legacyHost = safeHost(rawHost, c.get[String]("sanitization_status").contains("sanitized"))
      val classification = c.get[String]("classification").toOption.orElse(c.get[String]("category").toOption)
        .filter(Classifications).getOrElse("unknown")
      val blocked = c.get[Boolean]("blocked").getOrElse(Set("block", "http_blocked", "session.blocked")(eventType))
      val tls = evidence.hcursor.downField("tls")
      val measurement = evidence.hcursor.downField("measurement")
      val bytesStatus = measurement.get[String]("bytes_status").getOrElse("unavailable")
      val transfer = if bytesStatus == "measured_terminal" then
        measurement.get[Long]("bytes_up").toOption.map {
          case 0 => "measured zero upload"
          case value if value < 65536 => "small measured upload"
          case value if value < 1048576 => "medium measured upload"
          case _ => "large measured upload"
        }.getOrElse("transfer unavailable")
      else s"transfer $bytesStatus"
      val duration = if measurement.get[String]("duration_status").contains("connection_elapsed") then
        measurement.get[Long]("duration_ms").toOption.map {
          case value if value < 1000 => "brief connection duration"
          case value if value < 60000 => "short connection duration"
          case _ => "extended connection duration"
        }.getOrElse("duration unavailable")
      else "connection duration unavailable"
      val outcome = if blocked then "blocked observation" else eventType match
        case "http_error" => "upstream error observation"
        case "tunnel_open" => "tunnel opened"
        case "tunnel_close" => "tunnel closed"
        case _ => "allowed observation"
      val text = List("proxy event", eventType, host.getOrElse("host unavailable"), classification,
        outcome, tls.get[String]("offered_version").toOption.map("clienthello offered version " + _).getOrElse(""),
        tls.get[String]("offered_alpn").toOption.map("clienthello offered alpn " + _).getOrElse(""), transfer, duration).mkString(" ")
      Some(SearchDocumentSource(SearchDocumentKind.ProxyEvent, eventId, None, None, None, observedAt,
        None, None, None, 0, false, text,
        safe.deepMerge(Json.obj("event_id" -> eventId.asJson, "event_type" -> eventType.asJson,
          "host" -> legacyHost.asJson, "proxy_device_id" -> c.get[String]("device_id").toOption.filter(uuid).asJson,
          "status_code" -> c.get[Int]("status_code").toOption.asJson,
          "bytes_up" -> c.get[Long]("bytes_up").getOrElse(0L).asJson,
          "bytes_down" -> c.get[Long]("bytes_down").getOrElse(0L).asJson,
          "correlation_id" -> c.get[String]("correlation_id").toOption.filter(uuid).asJson,
          "legacy_scalar_semantics" -> "unverified_unless_measurement_status_confirms".asJson,
          "blocked" -> blocked.asJson, "classification" -> classification.asJson)).noSpaces,
        host = legacyHost, proxyEventType = Some(eventType), blocked = Some(blocked), classification = Some(classification),
        evidenceRevision = Some(revision), templateVersion = TemplateVersion, normalizationVersion = ProxyAuthority.Version))

  private def timestamp(value: String): Boolean = scala.util.Try(java.time.OffsetDateTime.parse(value)).isSuccess
  private def uuid(value: String): Boolean = scala.util.Try(java.util.UUID.fromString(value)).isSuccess
