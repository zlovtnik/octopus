package com.sslproxy.coordinator.processor

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.{Json, Printer}
import io.circe.syntax.*
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant, OffsetDateTime}
import scala.util.Try

/** Deterministic projection of a bounded, separately attested source snapshot. */
object ProxyBehavior:
  val CalculationVersion = "proxy-behavior-v1"
  val FlowTemplateVersion = "proxy-flow-v1"

  final case class Policy(
    deploymentScope: String,
    policyVersion: String,
    pairingHorizon: Duration,
    windowSeconds: Int,
    coverageStart: Instant,
    coverageEnd: Instant,
    coverageAttested: Boolean,
    conflictsAttested: Boolean,
    asOf: Instant,
    lookbackStart: Instant,
    maxSourceRows: Int,
    maxWindows: Int
  )
  final case class Source(row: Json, revision: Long, conflicts: Json = Json.obj())
  final case class Projection(
    key: String,
    evidence: Json,
    semanticText: String,
    fingerprint: String,
    disposition: String,
    sourceWatermark: Long,
    observedAt: Instant
  )
  final case class Result(flows: List[Projection], windows: List[Projection], excludedObservations: Int)

  private final case class Observation(source: Source, id: String, at: Instant, eventType: String, endpoint: Option[String], host: Option[String])
  private val Uuid = "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
  private val BlockedTypes = Set("block", "http_blocked", "session.blocked")
  private val TunnelTypes = Set("tunnel_open", "tunnel_close")
  private val CanonicalPrinter = Printer.noSpaces.copy(sortKeys = true)

  private def text(row: Json, key: String): Option[String] = row.hcursor.get[String](key).toOption
  private def uuid(row: Json, key: String): Option[String] = text(row, key).filter(_.matches(Uuid))
  private def time(row: Json): Option[Instant] = text(row, "time").flatMap(value => Try(OffsetDateTime.parse(value).toInstant).toOption)
  private def nonnegative(row: Json, key: String): Option[Long] = row.hcursor.get[Long](key).toOption.filter(_ >= 0)
  private def identity(parts: String*): String = parts.asJson.noSpaces
  private def stamp(at: Instant): Json = at.toString.asJson
  private def endpointKey(row: Json): Option[String] =
    val normalized = ProxyEvidence.normalized(row)
    text(normalized, "host_normalized").map(name =>
      identity(name, normalized.hcursor.get[Int]("port").toOption.fold("unknown")(_.toString), ProxyAuthority.Version))

  def build(policy: Policy, sources: List[Source]): Either[String, Result] =
    val valid = policy.deploymentScope.nonEmpty && policy.deploymentScope.length <= 128 &&
      policy.policyVersion.nonEmpty && policy.policyVersion.length <= 64 &&
      !policy.pairingHorizon.isNegative && !policy.pairingHorizon.isZero && policy.pairingHorizon.compareTo(Duration.ofDays(7)) <= 0 &&
      policy.windowSeconds > 0 && policy.windowSeconds <= 86400 && 86400 % policy.windowSeconds == 0 &&
      policy.lookbackStart.isBefore(policy.asOf) && Duration.between(policy.lookbackStart, policy.asOf).compareTo(Duration.ofDays(7)) <= 0 &&
      policy.coverageStart.isBefore(policy.coverageEnd) &&
      policy.maxSourceRows > 0 && policy.maxSourceRows <= 10000 && policy.maxWindows > 0 && policy.maxWindows <= 168
    if !valid then Left("invalid proxy behavior policy")
    else if sources.size > policy.maxSourceRows then Left("proxy behavior source row bound exceeded")
    else if sources.exists(_.revision <= 0) then Left("invalid proxy behavior source revision")
    else
      val versions = sources.groupBy(source => uuid(source.row, "event_id")).values.toList.flatMap { group =>
        group.groupBy(_.row).values.map(_.maxBy(_.revision))
      }
      val observations = versions.flatMap { source =>
        for
          id <- uuid(source.row, "event_id")
          at <- time(source.row)
          eventType <- text(source.row, "type").filter(ProxyEvidence.AllowedEvents)
        yield
          val normalized = ProxyEvidence.normalized(source.row)
          val host = text(normalized, "host_normalized")
          val endpoint = endpointKey(source.row)
          Observation(source, id, at, eventType, endpoint, host)
      }.sortBy(o => (o.at, o.id))
      val conflicts = sources.filter(_.conflicts.asObject.exists(_.nonEmpty))
      val conflictingIds = versions.flatMap(source => uuid(source.row, "event_id")).groupBy(id => id)
        .collect { case (id, instances) if instances.size > 1 => id }.toSet ++ conflicts.flatMap(source => uuid(source.row, "event_id"))
      val conflictCorrelations = conflicts.flatMap { source =>
        source.conflicts.asObject.toList.flatMap(_.values).flatMap(_.asString).filter(_.matches(Uuid)) ++
          uuid(source.row, "correlation_id").toList
      }.toSet
      val tunnelGroups = versions.filter(source => text(source.row, "type").exists(TunnelTypes))
        .flatMap(source => uuid(source.row, "correlation_id").map(_ -> source)).groupMap(_._1)(_._2)
      val flows = (tunnelGroups.keySet ++ conflictCorrelations).toList.sorted.map { correlation =>
        flow(policy, correlation, tunnelGroups.getOrElse(correlation, Nil), conflicts, conflictingIds, conflictCorrelations)
      }.filter(flow => !flow.observedAt.isBefore(policy.lookbackStart) && flow.observedAt.isBefore(policy.asOf))
      val retained = observations.filter(o => !o.at.isBefore(policy.lookbackStart) && o.at.isBefore(policy.asOf))
      val supported = retained.filter(o => o.endpoint.isDefined && !conflictingIds(o.id))
      val endpointGroups = retained.filter(_.endpoint.isDefined).groupBy(_.endpoint.get).toList.sortBy(_._1)
      val starts = Iterator.iterate(windowStart(policy.lookbackStart, policy.windowSeconds))(_.plusSeconds(policy.windowSeconds.toLong))
        .takeWhile(_.isBefore(policy.asOf)).take(policy.maxWindows + 1).toList
      if BigInt(starts.size) * BigInt(endpointGroups.size) > policy.maxWindows then Left("proxy behavior window row bound exceeded")
      else
        val windows = endpointGroups.flatMap { (endpoint, endpointRows) =>
          starts.map(start => hostWindow(policy, endpoint, start, endpointRows, flows, conflictingIds))
        }
        Right(Result(flows, windows, retained.size - supported.size))

  private def windowStart(at: Instant, seconds: Int): Instant =
    Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond, seconds.toLong) * seconds.toLong)

  private def projection(policy: Policy, key: String, evidence: Json, semanticText: String, disposition: String, watermark: Long, at: Instant): Projection =
    val stable = evidence.deepMerge(Json.obj(
      "deployment_scope" -> policy.deploymentScope.asJson,
      "policy_version" -> policy.policyVersion.asJson,
      "calculation_version" -> CalculationVersion.asJson,
      "normalization_version" -> ProxyAuthority.Version.asJson,
      "source_watermark" -> watermark.asJson
    ))
    val fingerprint = Sha256Utils.sha256Hex(CanonicalPrinter.print(stable).getBytes(StandardCharsets.UTF_8))
    Projection(key, stable.deepMerge(Json.obj("calculated_at" -> stamp(policy.asOf))), semanticText, fingerprint, disposition, watermark, at)

  private def flow(
    policy: Policy,
    correlation: String,
    sources: List[Source],
    conflicts: List[Source],
    conflictingIds: Set[String],
    conflictCorrelations: Set[String]
  ): Projection =
    val ordered = sources.sortBy(source => (time(source.row).getOrElse(Instant.MIN), text(source.row, "event_id").getOrElse("")))
    val opens = ordered.filter(source => text(source.row, "type").contains("tunnel_open"))
    val terminals = ordered.filter(source => text(source.row, "type").contains("tunnel_close"))
    val openedAt = opens.headOption.flatMap(source => time(source.row))
    val closedAt = terminals.headOption.flatMap(source => time(source.row))
    val lower = closedAt.map(_.minus(policy.pairingHorizon)).orElse(openedAt)
    val upper = openedAt.map(_.plus(policy.pairingHorizon)).orElse(closedAt)
    val sourceValid = ordered.forall { source =>
      val row = source.row
      val expected = if text(row, "type").contains("tunnel_open") then 1 else 2
      uuid(row, "event_id").isDefined && time(row).isDefined && text(row, "host").exists(_.nonEmpty) &&
        row.hcursor.get[Int]("event_sequence").contains(expected) && row.hcursor.get[Boolean]("blocked").contains(false)
    }
    val disposition =
      if conflictCorrelations(correlation) || ordered.exists(source => uuid(source.row, "event_id").exists(conflictingIds)) || opens.size > 1 || terminals.size > 1 then "ambiguous"
      else if !sourceValid then "ineligible"
      else if openedAt.zip(closedAt).exists((open, close) => close.isBefore(open)) ||
        opens.headOption.zip(terminals.headOption).exists((open, close) =>
          text(open.row, "host") != text(close.row, "host") || endpointKey(open.row) != endpointKey(close.row)) then "inconsistent"
      else if closedAt.zip(upper).exists((close, end) => close.isAfter(end)) then "outside_horizon"
      else if !policy.conflictsAttested then "conflict_coverage_unavailable"
      else if policy.coverageAttested && lower.exists(_.isBefore(policy.coverageStart)) then "retention_truncated"
      else if !policy.coverageAttested || upper.exists(end => !end.isBefore(policy.coverageEnd)) then "coverage_limited"
      else if opens.isEmpty then "missing_open"
      else if terminals.isEmpty then "missing_terminal"
      else "validated"
    val terminal = Option.when(terminals.size == 1)(terminals.head.row)
    val measurement = terminal.map(row => transfer(row, disposition == "validated")).getOrElse(unavailableTransfer)
    val at = closedAt.orElse(openedAt).getOrElse(policy.lookbackStart)
    val normalized = ordered.headOption.map(source => ProxyEvidence.normalized(source.row))
    val host = normalized.flatMap(value => text(value, "host_normalized"))
    val endpoint = ordered.headOption.flatMap(source => endpointKey(source.row))
    val references = ordered.flatMap(source => uuid(source.row, "event_id").map(id => Json.obj("event_id" -> id.asJson, "revision" -> source.revision.asJson)))
    val relevantConflicts = conflicts.filter(source => source.conflicts.asObject.exists(_.values.exists(_.asString.contains(correlation))) || uuid(source.row, "correlation_id").contains(correlation))
    val watermark = (ordered ++ relevantConflicts).map(_.revision).maxOption.getOrElse(0L)
    val evidence = Json.obj(
      "correlation_id" -> correlation.asJson, "disposition" -> disposition.asJson,
      "source_event_ids" -> ordered.flatMap(source => uuid(source.row, "event_id")).distinct.sorted.asJson,
      "source_references" -> references.asJson, "host_normalized" -> host.asJson, "endpoint_key" -> endpoint.asJson,
      "opened_at" -> openedAt.map(_.toString).asJson, "closed_at" -> closedAt.map(_.toString).asJson,
      "pairing_horizon_ms" -> policy.pairingHorizon.toMillis.asJson,
      "observation_elapsed_ms" -> openedAt.zip(closedAt).filter((open, close) => !close.isBefore(open)).map((open, close) => Duration.between(open, close).toMillis).asJson
    ).deepMerge(measurement)
    val bytesStatus = text(measurement, "bytes_status").getOrElse("unavailable")
    val prose = List("proxy flow", host.getOrElse("host unavailable"), disposition, s"transfer $bytesStatus", "duration scope retained separately").mkString(" ")
    projection(policy, identity(policy.deploymentScope, correlation), evidence, prose, disposition, watermark, at)

  private val unavailableTransfer = Json.obj(
    "bytes_status" -> "unavailable".asJson, "bytes_up" -> Json.Null, "bytes_down" -> Json.Null,
    "partial_bytes_up" -> Json.Null, "partial_bytes_down" -> Json.Null,
    "duration_status" -> "unavailable".asJson, "duration_ms" -> Json.Null
  )

  private def transfer(row: Json, validated: Boolean): Json =
    val measurement = row.hcursor.downField("measurement")
    val versioned = text(row, "evidence_version").contains(ProxyEvidence.Version) &&
      measurement.get[String]("version").contains("proxy-measurement-v1")
    val provenance = versioned && measurement.get[String]("source").contains("successful_socket_writes") &&
      measurement.get[String]("byte_unit").contains("bytes") &&
      measurement.get[String]("up_direction").contains("client_to_destination") &&
      measurement.get[String]("down_direction").contains("destination_to_client")
    val counters = nonnegative(row, "bytes_up").zip(nonnegative(row, "bytes_down"))
    val declared = measurement.get[String]("bytes_status").toOption
    val destinationKnown = !text(row, "reason").contains("allowed_plaintext")
    val measured = validated && provenance && destinationKnown && declared.contains("measured_terminal") &&
      measurement.get[String]("termination").contains("measured_terminal") && counters.isDefined
    val partial = validated && provenance && declared.contains("partial") &&
      measurement.get[String]("termination").contains("partial") && counters.isDefined
    val status = if measured then "measured_terminal" else if partial then "partial" else if !versioned then "legacy_unknown" else "unavailable"
    val duration = Option.when(validated && versioned && measurement.get[String]("duration_status").contains("connection_elapsed"))(nonnegative(row, "duration_ms")).flatten
    Json.obj(
      "bytes_status" -> status.asJson,
      "bytes_up" -> Option.when(measured)(counters.get._1).asJson,
      "bytes_down" -> Option.when(measured)(counters.get._2).asJson,
      "partial_bytes_up" -> Option.when(partial)(counters.get._1).asJson,
      "partial_bytes_down" -> Option.when(partial)(counters.get._2).asJson,
      "duration_status" -> (if duration.isDefined then "connection_elapsed" else "unavailable").asJson,
      "duration_ms" -> duration.asJson
    )

  private def hostWindow(policy: Policy, endpoint: String, start: Instant, sources: List[Observation], flows: List[Projection], conflictingIds: Set[String]): Projection =
    val end = start.plusSeconds(policy.windowSeconds.toLong)
    val candidates = sources.filter(source => !source.at.isBefore(start) && source.at.isBefore(end))
    val inWindow = candidates.filterNot(source => conflictingIds(source.id))
    val associatedFlows = flows.filter(flow => text(flow.evidence, "endpoint_key").contains(endpoint) &&
      !flow.observedAt.isBefore(start) && flow.observedAt.isBefore(end))
    val validated = associatedFlows.filter(_.disposition == "validated")
    val measured = validated.filter(flow => text(flow.evidence, "bytes_status").contains("measured_terminal"))
    val blocked = inWindow.count(source => source.source.row.hcursor.get[Boolean]("blocked").getOrElse(BlockedTypes(source.eventType)))
    val latest = inWindow.lastOption
    val watermark = (candidates.map(_.source.revision) ++ associatedFlows.map(_.sourceWatermark)).maxOption.getOrElse(0L)
    val full = policy.coverageAttested && !start.isBefore(policy.coverageStart) && !end.isAfter(policy.coverageEnd) && !start.isBefore(policy.lookbackStart) && !end.isAfter(policy.asOf)
    val disposition = if candidates.size != inWindow.size then "ambiguous_source_evidence" else if full then "available" else "coverage_limited"
    val totalsStatus = if measured.isEmpty then "unavailable" else if measured.size == validated.size then "measured_terminal" else "measured_subset"
    val coveredStart = if start.isBefore(policy.coverageStart) then policy.coverageStart else start
    val coveredEnd = if end.isAfter(policy.coverageEnd) then policy.coverageEnd else end
    val overlap = coveredStart.isBefore(coveredEnd)
    val evidence = Json.obj(
      "endpoint_key" -> endpoint.asJson, "host_normalized" -> sources.head.host.asJson,
      "window_start" -> stamp(start), "window_end" -> stamp(end),
      "disposition" -> disposition.asJson, "observed_count" -> inWindow.size.asJson,
      "excluded_observations" -> (candidates.size - inWindow.size).asJson,
      "allowed_observations" -> (inWindow.size - blocked).asJson, "blocked_observations" -> blocked.asJson,
      "validated_flows" -> validated.size.asJson, "measured_flows" -> measured.size.asJson,
      "unavailable_flows" -> (validated.size - measured.size).asJson,
      "transfer_totals_status" -> totalsStatus.asJson,
      "bytes_up" -> Option.when(measured.nonEmpty)(measured.map(flow => BigInt(flow.evidence.hcursor.get[Long]("bytes_up").toOption.get)).sum).asJson,
      "bytes_down" -> Option.when(measured.nonEmpty)(measured.map(flow => BigInt(flow.evidence.hcursor.get[Long]("bytes_down").toOption.get)).sum).asJson,
      "last_event_id" -> latest.map(_.id).asJson, "last_event_time" -> latest.map(_.at.toString).asJson,
      "source_event_ids" -> inWindow.map(_.id).distinct.sorted.asJson,
      "flow_keys" -> validated.map(_.key).sorted.asJson,
      "coverage_start" -> Option.when(overlap)(coveredStart.toString).asJson,
      "coverage_end" -> Option.when(overlap)(coveredEnd.toString).asJson,
      "attempt_count" -> Json.Null
    )
    projection(policy, identity(policy.deploymentScope, endpoint, start.toString), evidence,
      "proxy host window " + sources.head.host.getOrElse("host unavailable") + " " + disposition,
      disposition, watermark, start)
