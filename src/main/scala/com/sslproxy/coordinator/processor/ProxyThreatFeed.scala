package com.sslproxy.coordinator.processor

import cats.syntax.all.*
import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.{Json, Printer}
import io.circe.syntax.*
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant}
import scala.util.Try

/** Local, versioned feed assertions over retained observations. A match is not
  * independent confirmation of a threat or physical-device ownership.
  */
object ProxyThreatFeed:
  val Version = "proxy-threat-feed-v1"
  private val MaximumRows = 10000
  private val MaximumSnapshots = 8
  private val MaximumIndicators = 256
  private val MaximumReferences = 64
  private val Canonical = Printer.noSpaces.copy(sortKeys = true)
  private val EventId = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"

  private final case class Policy(version: String, calibration: String, acceptance: String,
      alerts: Boolean, snapshots: List[Json])
  private final case class Indicator(id: String, kind: String, value: String, expires: Option[Instant])
  private final case class Snapshot(feed: String, version: String, publisher: String, uri: String,
      license: String, generated: Instant, retrieved: Instant, expires: Instant,
      checksum: String, indicators: List[Indicator])
  private final case class Window(deployment: String, scopeKind: String, scopeKey: String,
      key: String, start: Instant, end: Instant, complete: Boolean, policyVersion: String,
      calculationVersion: String, normalizationVersion: String, sourceWatermark: Long)
  private final case class Observation(id: String, at: Instant, host: Option[String],
      domain: Option[String], selected: Option[String])

  private def text(row: Json, key: String): Option[String] = row.hcursor.get[String](key).toOption
  private def bounded(value: String, max: Int): Boolean =
    value.nonEmpty && value.length <= max && !value.exists(Character.isISOControl)
  private def field(row: Json, key: String, max: Int = 128): Either[String, String] =
    text(row, key).filter(bounded(_, max)).toRight("missing_or_invalid_" + key)
  private def instant(row: Json, key: String): Either[String, Instant] =
    field(row, key, 64).flatMap(value => Try(Instant.parse(value)).toEither.leftMap(_ => "invalid_" + key))
  private def keys(row: Json, allowed: Set[String]): Boolean =
    row.asObject.exists(_.keys.forall(allowed))
  private def digest(value: Json): String =
    Sha256Utils.sha256Hex(Canonical.print(value).getBytes(StandardCharsets.UTF_8))

  /** SHA-256 of the sorted indicator objects serialized with sorted JSON keys.
    * Snapshot importers must use this exact contract, not raw-file checksums.
    */
  def snapshotChecksum(indicators: List[Json]): String =
    digest(indicators.sortBy(Canonical.print).asJson)

  private def parsePolicy(config: Json): Either[String, Policy] =
    for
      _ <- Either.cond(keys(config, Set("version", "calibration_reference", "b12_acceptance_reference", "alerts_enabled", "snapshots")), (), "invalid_policy_fields")
      version <- field(config, "version", 64)
      calibration <- field(config, "calibration_reference", 256)
      acceptance <- field(config, "b12_acceptance_reference", 256)
      alerts <- if config.hcursor.downField("alerts_enabled").focus.isEmpty then Right(false)
        else config.hcursor.get[Boolean]("alerts_enabled").leftMap(_ => "invalid_alerts_enabled")
      snapshots <- config.hcursor.get[List[Json]]("snapshots").leftMap(_ => "invalid_snapshots")
      _ <- Either.cond(snapshots.size <= MaximumSnapshots, (), "snapshot_bound_exceeded")
    yield Policy(version, calibration, acceptance, alerts, snapshots)

  private def dns(value: String): Option[(String, Option[String])] =
    val parsed = ProxyAuthority.normalize(Some(value), "query")
    Option.when(parsed.parse_status == "valid_dns" && parsed.port.isEmpty)(parsed)
      .flatMap(p => p.host_normalized.map(_ -> p.registrable_domain))

  private def address(value: String): Option[String] =
    Option.when(value.nonEmpty && value.length <= 45 &&
      value.forall(c => "0123456789abcdefABCDEF:.".contains(c)))(value)
      .map(v => ProxyAuthority.normalize(Some(v), "original_destination"))
      .filter(p => Set("valid_ipv4", "valid_ipv6")(p.parse_status) && p.port.isEmpty)
      .flatMap(_.host_normalized)

  private def parseIndicator(row: Json): Either[String, Indicator] =
    for
      _ <- Either.cond(keys(row, Set("indicator_id", "kind", "value", "expires_at")), (), "invalid_indicator_fields")
      id <- field(row, "indicator_id")
      kind <- field(row, "kind", 32)
      raw <- field(row, "value", 253)
      value <- (kind match
        case "exact_host" => dns(raw).map(_._1)
        case "exact_selected_ip" => address(raw)
        case "registrable_domain" => dns(raw).filter((host, domain) => domain.contains(host)).map(_._1)
        case _ => None).toRight("invalid_indicator_specificity_or_value")
      expires <- if row.hcursor.downField("expires_at").focus.isEmpty then Right(None)
        else instant(row, "expires_at").map(Some(_))
    yield Indicator(id, kind, value, expires)

  private def parseSnapshot(row: Json, asOf: Instant): Either[String, Snapshot] =
    for
      _ <- Either.cond(keys(row, Set("feed_id", "snapshot_version", "provenance", "license", "generated_at", "retrieved_at", "expires_at", "sha256", "indicators")), (), "invalid_snapshot_fields")
      feed <- field(row, "feed_id")
      version <- field(row, "snapshot_version")
      provenance <- row.hcursor.get[Json]("provenance").leftMap(_ => "invalid_provenance")
      _ <- Either.cond(keys(provenance, Set("publisher", "source_uri")), (), "invalid_provenance_fields")
      publisher <- field(provenance, "publisher", 256)
      uri <- field(provenance, "source_uri", 1024)
      _ <- Either.cond(Try(new URI(uri)).toOption.exists(u =>
        u.isAbsolute && u.getUserInfo == null && (u.getScheme match
          case "https" => Option(u.getHost).exists(_.nonEmpty)
          case "urn" => Option(u.getSchemeSpecificPart).exists(_.nonEmpty)
          case "file" => Option(u.getPath).exists(_.startsWith("/"))
          case _ => false)), (), "invalid_source_uri")
      license <- field(row, "license", 256)
      _ <- Either.cond(license == license.trim &&
        !Set("unknown", "unavailable", "unlicensed")(license.toLowerCase(java.util.Locale.ROOT)), (), "license_unavailable")
      generated <- instant(row, "generated_at")
      retrieved <- instant(row, "retrieved_at")
      expires <- instant(row, "expires_at")
      _ <- Either.cond(!generated.isAfter(retrieved) && !retrieved.isAfter(asOf) &&
        retrieved.isBefore(expires) && Duration.between(retrieved, expires).compareTo(Duration.ofDays(30)) <= 0,
        (), "invalid_snapshot_time_bounds")
      checksum <- field(row, "sha256", 64)
      raw <- row.hcursor.get[List[Json]]("indicators").leftMap(_ => "invalid_indicators")
      _ <- Either.cond(raw.size <= MaximumIndicators, (), "indicator_bound_exceeded")
      indicators <- raw.traverse(parseIndicator)
      _ <- Either.cond(checksum.matches("[0-9a-f]{64}") && checksum == snapshotChecksum(raw), (), "snapshot_checksum_mismatch")
      _ <- Either.cond(indicators.map(_.id).distinct.size == indicators.size &&
        indicators.map(i => (i.kind, i.value)).distinct.size == indicators.size, (), "duplicate_indicators")
    yield Snapshot(feed, version, publisher, uri, license, generated, retrieved, expires, checksum,
      indicators.sortBy(i => (i.kind, i.value, i.id)))

  private def parseWindow(p: ProxyBehavior.Projection, asOf: Instant): Either[String, Window] =
    val row = p.evidence
    for
      deployment <- field(row, "deployment_scope")
      _ <- Either.cond(bounded(p.key, 2048), (), "invalid_window_key")
      start <- instant(row, "window_start")
      end <- instant(row, "window_end")
      _ <- Either.cond(start.isBefore(end) && Duration.between(start, end).compareTo(Duration.ofDays(1)) <= 0 &&
        Try(end.plus(Duration.ofDays(7))).isSuccess, (), "invalid_window_bounds")
      scopeKind = text(row, "scope_kind").getOrElse("deployment")
      _ <- Either.cond(Set("deployment", "registration")(scopeKind), (), "unsupported_scope")
      scopeKey <- if scopeKind == "registration" then field(row, "scope_key", 256) else Right("deployment")
      policyVersion <- field(row, "policy_version", 64)
      calculationVersion <- field(row, "calculation_version", 64)
      normalizationVersion <- field(row, "normalization_version", 64)
      _ <- Either.cond(normalizationVersion == ProxyAuthority.Version, (), "unsupported_normalization")
    yield Window(deployment, scopeKind, scopeKey, p.key, start, end,
      p.disposition == "available" && !end.isAfter(asOf),
      policyVersion, calculationVersion, normalizationVersion, p.sourceWatermark)

  private def observations(rows: List[Json], window: Window, asOf: Instant): List[Observation] =
    rows.groupBy(row => text(row, "event_id")).toList.flatMap { (id, versions) =>
      val distinct = versions.distinct
      if distinct.size != 1 then Nil
      else
        val row = distinct.head
        val normalized = ProxyEvidence.normalized(row)
        (for
          eventId <- id.filter(_.matches(EventId))
          at <- text(row, "time").flatMap(v => Try(Instant.parse(v)).toOption)
            .filter(at => !at.isBefore(window.start) && at.isBefore(window.end) && !at.isAfter(asOf))
          kind <- text(row, "type").filter(k => ProxyEvidence.AllowedEvents(k) && k != "tunnel_close")
          _ <- Option.when(text(row, "evidence_version").contains(ProxyEvidence.Version))(())
        yield Observation(eventId, at, text(normalized, "host_normalized"),
          text(normalized, "registrable_domain"),
          Option.when(kind == "tunnel_open" && row.hcursor.get[Boolean]("blocked").contains(false))(())
            .flatMap(_ => text(row, "selected_ip").flatMap(address)))).toList
    }.sortBy(o => (o.at, o.id))

  private def signal(policy: Policy, snapshot: Snapshot, indicator: Indicator,
      rows: List[Observation], allRows: Int, window: Window, asOf: Instant): Json =
    val expires = (List(snapshot.expires, window.end.plus(Duration.ofDays(7))) ++ indicator.expires).min
    val exact = indicator.kind != "registrable_domain"
    val active = policy.alerts && exact && window.complete && expires.isAfter(asOf)
    val id = digest(List("proxy-feed-signal", window.deployment, window.scopeKind, window.scopeKey,
      window.key, window.start.toString, window.end.toString, snapshot.feed, indicator.id,
      indicator.kind, indicator.value).asJson)
    val body = Json.obj(
      "signal_id" -> id.asJson,
      "signal_kind" -> "local_threat_feed_correlation".asJson,
      "calculation_version" -> Version.asJson,
      "policy_version" -> policy.version.asJson,
      "calibration_reference" -> policy.calibration.asJson,
      "b12_acceptance_reference" -> policy.acceptance.asJson,
      "deployment_scope" -> window.deployment.asJson,
      "scope_kind" -> window.scopeKind.asJson,
      "scope_key" -> window.scopeKey.asJson,
      "window_key" -> window.key.asJson,
      "window_start" -> window.start.toString.asJson,
      "window_end" -> window.end.toString.asJson,
      "window_policy_version" -> window.policyVersion.asJson,
      "window_calculation_version" -> window.calculationVersion.asJson,
      "normalization_version" -> window.normalizationVersion.asJson,
      "feed_id" -> snapshot.feed.asJson,
      "snapshot_version" -> snapshot.version.asJson,
      "snapshot_sha256" -> snapshot.checksum.asJson,
      "feed_provenance" -> Json.obj("publisher" -> snapshot.publisher.asJson, "source_uri" -> snapshot.uri.asJson),
      "feed_license" -> snapshot.license.asJson,
      "feed_generated_at" -> snapshot.generated.toString.asJson,
      "feed_retrieved_at" -> snapshot.retrieved.toString.asJson,
      "feed_expires_at" -> snapshot.expires.toString.asJson,
      "indicator_id" -> indicator.id.asJson,
      "specificity" -> indicator.kind.asJson,
      "indicator_value" -> indicator.value.asJson,
      "sample_size" -> rows.size.asJson,
      "observation_denominator" -> allRows.asJson,
      "source_event_ids" -> rows.map(_.id).sorted.take(MaximumReferences).asJson,
      "omitted_source_event_count" -> (rows.size - MaximumReferences).max(0).asJson,
      "first_observed_at" -> rows.headOption.map(_.at.toString).asJson,
      "last_observed_at" -> rows.lastOption.map(_.at.toString).asJson,
      "source_watermark" -> window.sourceWatermark.asJson,
      "collection_complete" -> window.complete.asJson,
      "candidate" -> true.asJson,
      "state" -> (if active then "active" else "candidate").asJson,
      "valid_until" -> expires.toString.asJson,
      "threat_confirmation" -> "unavailable_external_feed_assertion_only".asJson,
      "ownership" -> "unavailable".asJson,
      "uncertainty" -> List("shared_infrastructure", "feed_assertion_not_independent_confirmation",
        "retained_observation_coverage", "registration_claim_not_physical_identity").asJson
    )
    body.deepMerge(Json.obj("signal_fingerprint" -> digest(body).asJson))

  /** The caller atomically retracts stored signal IDs absent from this result.
    * Consumers must also enforce valid_until, even between preparation ticks.
    */
  def evidence(rows: List[Json], window: ProxyBehavior.Projection, config: Json, asOf: Instant): Json =
    val disabled = config.asObject.exists(_.isEmpty)
    val parsed = if rows.size > MaximumRows then Left("source_row_bound_exceeded")
      else for
        policy <- parsePolicy(config)
        scope <- parseWindow(window, asOf)
      yield (policy, scope)
    parsed match
      case Left(reason) => Json.obj(
        "calculation_version" -> Version.asJson,
        "disposition" -> (if disabled then "disabled" else reason).asJson,
        "state" -> "retracted".asJson,
        "withdrawal_rule" -> "retract_all_absent_signal_ids".asJson,
        "signals" -> List.empty[Json].asJson,
        "snapshot_dispositions" -> List.empty[Json].asJson
      )
      case Right((policy, scope)) =>
        val results = policy.snapshots.map(raw => raw -> parseSnapshot(raw, asOf))
        val supported = results.flatMap(_._2.toOption)
        val collisions = policy.snapshots.flatMap(raw => text(raw, "feed_id").filter(bounded(_, 128)))
          .groupBy(identity).collect { case (feed, versions) if versions.size > 1 => feed }.toSet
        val snapshots = supported.filter(s => !collisions(s.feed) && s.expires.isAfter(asOf))
        val availableRows = observations(rows, scope, asOf)
        val hosts = availableRows.flatMap(row => row.host.map(_ -> row)).groupMap(_._1)(_._2)
        val domains = availableRows.flatMap(row => row.domain.map(_ -> row)).groupMap(_._1)(_._2)
        val addresses = availableRows.flatMap(row => row.selected.map(_ -> row)).groupMap(_._1)(_._2)
        val signals = snapshots.sortBy(s => (s.feed, s.version)).flatMap { snapshot =>
          snapshot.indicators.flatMap { indicator =>
            val matching = indicator.kind match
              case "exact_host" => hosts.getOrElse(indicator.value, Nil)
              case "registrable_domain" => domains.getOrElse(indicator.value, Nil)
              case "exact_selected_ip" => addresses.getOrElse(indicator.value, Nil)
              case _ => Nil
            Option.when(matching.nonEmpty && scope.end.plus(Duration.ofDays(7)).isAfter(asOf) &&
              indicator.expires.forall(_.isAfter(asOf)))(
              signal(policy, snapshot, indicator, matching, availableRows.size, scope, asOf))
          }
        }.sortBy(_.hcursor.get[String]("signal_id").getOrElse(""))
        val dispositions = results.map { (raw, result) =>
          Json.obj("feed_id" -> text(raw, "feed_id").filter(bounded(_, 128)).asJson,
            "snapshot_version" -> text(raw, "snapshot_version").filter(bounded(_, 128)).asJson,
            "disposition" -> result.fold(identity, s =>
              if collisions(s.feed) then "conflicting_feed_snapshots"
              else if !s.expires.isAfter(asOf) then "expired" else "available").asJson)
        }.sortBy(Canonical.print)
        val overflow = signals.size > MaximumIndicators
        val retainedSignals = if overflow then Nil else signals
        Json.obj(
          "calculation_version" -> Version.asJson,
          "policy_version" -> policy.version.asJson,
          "calibration_reference" -> policy.calibration.asJson,
          "b12_acceptance_reference" -> policy.acceptance.asJson,
          "disposition" -> (if overflow then "signal_bound_exceeded" else if snapshots.isEmpty then "no_available_snapshots" else if retainedSignals.isEmpty then "no_matches" else "correlation_evidence").asJson,
          "state" -> (if retainedSignals.exists(_.hcursor.get[String]("state").contains("active")) then "active" else if retainedSignals.nonEmpty then "candidate" else "retracted").asJson,
          "withdrawal_rule" -> "retract_all_absent_signal_ids".asJson,
          "signals" -> retainedSignals.asJson,
          "snapshot_dispositions" -> dispositions.asJson,
          "asn_or_peer_inference" -> "unavailable".asJson,
          "shared_infrastructure_caveat" -> "Shared domains and addresses can serve unrelated benign tenants".asJson
        )
