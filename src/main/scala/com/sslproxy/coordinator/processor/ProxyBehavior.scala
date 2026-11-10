package com.sslproxy.coordinator.processor

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.{Json, Printer}
import io.circe.syntax.*
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant, OffsetDateTime}
import scala.util.Try

/** Deterministic projection of a bounded, separately attested source snapshot.
  */
object ProxyBehavior:
  val CalculationVersion = "proxy-behavior-v4"
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
      maxWindows: Int,
      intelligencePolicy: Json = Json.obj(),
      evaluationTime: Option[Instant] = None
  )
  final case class Source(
      row: Json,
      revision: Long,
      conflicts: Json = Json.obj()
  )
  final case class Projection(
      key: String,
      evidence: Json,
      semanticText: String,
      fingerprint: String,
      disposition: String,
      sourceWatermark: Long,
      observedAt: Instant
  )
  final case class Result(
      flows: List[Projection],
      windows: List[Projection],
      excludedObservations: Int,
      destinations: List[Projection] = Nil,
      deviceWindows: List[Projection] = Nil,
      attributionWindows: List[Projection] = Nil,
      signals: List[Json] = Nil
  )

  private final case class Observation(
      source: Source,
      id: String,
      at: Instant,
      eventType: String,
      endpoint: Option[String],
      host: Option[String],
      attribution: ProxyBehaviorScopes.Attribution
  )
  private val Uuid =
    "[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
  private val BlockedTypes = Set("block", "http_blocked", "session.blocked")
  private val TunnelTypes = Set("tunnel_open", "tunnel_close")
  private val CanonicalPrinter = Printer.noSpaces.copy(sortKeys = true)

  private def text(row: Json, key: String): Option[String] =
    row.hcursor.get[String](key).toOption
  private def uuid(row: Json, key: String): Option[String] =
    text(row, key).filter(_.matches(Uuid))
  private def time(row: Json): Option[Instant] = text(row, "time").flatMap(
    value => Try(OffsetDateTime.parse(value).toInstant).toOption
  )
  private def nonnegative(row: Json, key: String): Option[Long] =
    row.hcursor.get[Long](key).toOption.filter(_ >= 0)
  private def identity(parts: String*): String = parts.asJson.noSpaces
  private def stamp(at: Instant): Json = at.toString.asJson
  private def endpointKey(row: Json): Option[String] =
    val normalized = ProxyEvidence.normalized(row)
    text(normalized, "host_normalized").map(name =>
      identity(
        name,
        normalized.hcursor
          .get[Int]("port")
          .toOption
          .fold("unknown")(_.toString),
        ProxyAuthority.Version
      )
    )

  def build(policy: Policy, sources: List[Source]): Either[String, Result] =
    val valid =
      policy.deploymentScope.nonEmpty && policy.deploymentScope.length <= 128 &&
        policy.policyVersion.nonEmpty && policy.policyVersion.length <= 64 &&
        !policy.pairingHorizon.isNegative && !policy.pairingHorizon.isZero && policy.pairingHorizon
          .compareTo(Duration.ofDays(7)) <= 0 &&
        policy.windowSeconds > 0 && policy.windowSeconds <= 86400 && 86400 % policy.windowSeconds == 0 &&
        policy.lookbackStart.isBefore(policy.asOf) && Duration
          .between(policy.lookbackStart, policy.asOf)
          .compareTo(Duration.ofDays(7)) <= 0 &&
        policy.coverageStart.isBefore(policy.coverageEnd) &&
        policy.maxSourceRows > 0 && policy.maxSourceRows <= 10000 && policy.maxWindows > 0 && policy.maxWindows <= 168
    if !valid then Left("invalid proxy behavior policy")
    else if sources.size > policy.maxSourceRows then
      Left("proxy behavior source row bound exceeded")
    else if sources.exists(_.revision <= 0) then
      Left("invalid proxy behavior source revision")
    else
      val versions = sources
        .groupBy(source => uuid(source.row, "event_id"))
        .values
        .toList
        .flatMap { group =>
          group.groupBy(_.row).values.map(_.maxBy(_.revision))
        }
      val observations = versions
        .flatMap { source =>
          for
            id <- uuid(source.row, "event_id")
            at <- time(source.row)
            eventType <- text(source.row, "type").filter(
              ProxyEvidence.AllowedEvents
            )
          yield
            val normalized = ProxyEvidence.normalized(source.row)
            val host = text(normalized, "host_normalized")
            val endpoint = endpointKey(source.row)
            Observation(
              source,
              id,
              at,
              eventType,
              endpoint,
              host,
              ProxyBehaviorScopes
                .attribution(policy.deploymentScope, source.row, at)
            )
        }
        .sortBy(o => (o.at, o.id))
      val conflicts = sources.filter(_.conflicts.asObject.exists(_.nonEmpty))
      val conflictingIds = versions
        .flatMap(source => uuid(source.row, "event_id"))
        .groupBy(id => id)
        .collect { case (id, instances) if instances.size > 1 => id }
        .toSet ++ conflicts.flatMap(source => uuid(source.row, "event_id"))
      val conflictCorrelations = conflicts.flatMap { source =>
        source.conflicts.asObject.toList
          .flatMap(_.values)
          .flatMap(_.asString)
          .filter(_.matches(Uuid)) ++
          uuid(source.row, "correlation_id").toList
      }.toSet
      val tunnelGroups = versions
        .filter(source => text(source.row, "type").exists(TunnelTypes))
        .flatMap(source => uuid(source.row, "correlation_id").map(_ -> source))
        .groupMap(_._1)(_._2)
      val flows = (tunnelGroups.keySet ++ conflictCorrelations).toList.sorted
        .map { correlation =>
          flow(
            policy,
            correlation,
            tunnelGroups.getOrElse(correlation, Nil),
            conflicts,
            conflictingIds,
            conflictCorrelations
          )
        }
        .filter(flow =>
          !flow.observedAt.isBefore(policy.lookbackStart) && flow.observedAt
            .isBefore(policy.asOf)
        )
      val retainedRows = observations.filter(o =>
        !o.at.isBefore(policy.lookbackStart) && o.at.isBefore(policy.asOf)
      )
      // Overlapping claims for different registrations make the peer ambiguous.
      // Sweep interval boundaries (ends before starts) rather than comparing all pairs.
      val ambiguousPeers = observations
        .filterNot(o => conflictingIds(o.id))
        .map(_.attribution)
        .filter(_.kind == "registration")
        .groupBy(_.peer.get)
        .collect {
          case (peer, claims)
              if claims.distinct
                .flatMap(a =>
                  List(
                    (a.from.get, 1, a.registration.get),
                    (a.until.get, -1, a.registration.get)
                  )
                )
                .sortBy((at, delta, registration) => (at, delta, registration))
                .foldLeft((Map.empty[String, Int], false)) {
                  case ((active, conflict), (_, delta, registration)) =>
                    val count = active.getOrElse(registration, 0) + delta
                    val next = if count == 0 then active - registration
                    else active.updated(registration, count)
                    (next, conflict || next.size > 1)
                }
                ._2 =>
            peer
        }
        .toSet
      val retained = retainedRows.map(o =>
        if o.attribution.peer.exists(ambiguousPeers) then
          o.copy(attribution =
            ProxyBehaviorScopes
              .invalidate(policy.deploymentScope, o.attribution)
          )
        else o
      )
      val supported =
        retained.filter(o => o.endpoint.isDefined && !conflictingIds(o.id))
      val endpointGroups = retained
        .filter(_.endpoint.isDefined)
        .groupBy(_.endpoint.get)
        .toList
        .sortBy(_._1)
      val starts = Iterator
        .iterate(windowStart(policy.lookbackStart, policy.windowSeconds))(
          _.plusSeconds(policy.windowSeconds.toLong)
        )
        .takeWhile(_.isBefore(policy.asOf))
        .take(policy.maxWindows + 1)
        .toList
      val registrationGroups = supported
        .filter(_.attribution.kind == "registration")
        .groupBy(o => (o.attribution.key.get, o.endpoint.get))
        .toList
        .sortBy(_._1)
      val scopes = (List(("deployment", "deployment", supported)) ++
        supported
          .filter(_.attribution.kind == "registration")
          .groupBy(_.attribution.key.get)
          .toList
          .sortBy(_._1)
          .map { case (key, rows) => ("registration", key, rows) } ++
        supported
          .filter(_.attribution.peer.isDefined)
          .groupBy(_.attribution.peer.get)
          .toList
          .sortBy(_._1)
          .map { case (peer, rows) =>
            (
              "peer",
              ProxyBehaviorScopes.peerKey(policy.deploymentScope, peer),
              rows
            )
          })
      val destinationCount =
        scopes.map(_._3.flatMap(_.endpoint).distinct.size).sum
      val registrationWindows = registrationGroups.map { case (scope, rows) =>
        val overlapping = starts.filter(start =>
          rows.head.attribution.from.exists(
            _.isBefore(start.plusSeconds(policy.windowSeconds.toLong))
          ) &&
            rows.head.attribution.until.exists(_.isAfter(start))
        )
        (scope, rows, overlapping)
      }
      val outputWindows =
        BigInt(starts.size) * BigInt(endpointGroups.size + 1) +
          registrationWindows.map(group => BigInt(group._3.size)).sum
      if outputWindows + destinationCount > policy.maxWindows then
        Left("proxy behavior window row bound exceeded")
      else
        val watermark = sources.map(_.revision).maxOption.getOrElse(0L)
        val windows = endpointGroups.flatMap { (endpoint, endpointRows) =>
          starts.map { start =>
            val p = hostWindow(
              policy,
              endpoint,
              start,
              endpointRows,
              flows,
              conflictingIds
            )
            enrichWindow(
              policy,
              p,
              supported.filter(_.endpoint.contains(endpoint)),
              flows,
              watermark,
              supported
            )
          }
        }
        val deviceWindows = registrationWindows.flatMap {
          case ((key, endpoint), rows, overlapping) =>
            val ids = observations
              .filter(o =>
                !conflictingIds(o.id) && o.attribution.kind == "registration" &&
                  o.attribution.key.contains(key)
              )
              .map(_.id)
              .toSet
            val scopedFlows = flows.filter(p =>
              p.evidence.hcursor
                .get[List[String]]("source_event_ids")
                .toOption
                .exists(refs => refs.size == 2 && refs.forall(ids))
            )
            overlapping.map { start =>
              val base = hostWindow(
                policy,
                endpoint,
                start,
                rows,
                scopedFlows,
                Set.empty
              )
              val scopeFull =
                rows.head.attribution.from.exists(at => !at.isAfter(start)) &&
                  rows.head.attribution.until.exists(at =>
                    !at.isBefore(start.plusSeconds(policy.windowSeconds.toLong))
                  )
              val disposition = if scopeFull then base.disposition
              else "attribution_interval_limited"
              val evidence = base.evidence.deepMerge(
                Json.obj(
                  "scope_kind" -> "registration".asJson,
                  "scope_key" -> key.asJson,
                  "ownership" -> "registration_claim_only".asJson,
                  "mapping_revision" -> rows.head.attribution.mapping.asJson,
                  "valid_from" -> rows.head.attribution.from
                    .map(_.toString)
                    .asJson,
                  "valid_until" -> rows.head.attribution.until
                    .map(_.toString)
                    .asJson,
                  "disposition" -> disposition.asJson
                )
              )
              val scoped = projection(
                policy,
                identity(
                  policy.deploymentScope,
                  "registration",
                  key,
                  endpoint,
                  start.toString
                ),
                evidence,
                "proxy registration window",
                disposition,
                watermark,
                start
              )
              enrichWindow(
                policy,
                scoped,
                rows,
                scopedFlows,
                watermark,
                supported.filter(_.attribution.key.contains(key))
              )
            }
        }
        val destinations = scopes.flatMap { (kind, key, rows) =>
          rows.groupBy(_.endpoint.get).toList.sortBy(_._1).map {
            (endpoint, endpointRows) =>
              val evidence = Json
                .obj(
                  "scope_kind" -> kind.asJson,
                  "scope_key" -> key.asJson,
                  "endpoint_key" -> endpoint.asJson,
                  "first_observed_at" -> endpointRows.head.at.toString.asJson,
                  "last_observed_at" -> endpointRows.last.at.toString.asJson,
                  "first_event_id" -> endpointRows.head.id.asJson,
                  "last_event_id" -> endpointRows.last.id.asJson,
                  "observation_count" -> endpointRows.size.asJson,
                  "scope_observation_count" -> rows.size.asJson,
                  "destination_count" -> rows
                    .flatMap(_.endpoint)
                    .distinct
                    .size
                    .asJson,
                  "observed_fraction" -> (endpointRows.size.toDouble / rows.size).asJson,
                  "rarity_unit" -> "destination_observations_per_scope_observations".asJson,
                  "domain_registration_time" -> Json.Null,
                  "domain_registration_disposition" -> "unavailable".asJson
                )
                .deepMerge(history(policy))
              projection(
                policy,
                identity(policy.deploymentScope, kind, key, endpoint),
                evidence,
                "proxy destination retained first observation",
                "retained_history_only",
                watermark,
                endpointRows.head.at
              )
          }
        }
        val attributionWindows = starts.map { start =>
          val end = start.plusSeconds(policy.windowSeconds.toLong)
          val rows =
            retained.filter(o => !o.at.isBefore(start) && o.at.isBefore(end))
          val usable = rows.filterNot(o => conflictingIds(o.id))
          val coverage = ProxyBehaviorScopes.coverage(usable.map(_.attribution))
          val trustedHistory = retained.filterNot(o => conflictingIds(o.id))
          val peerChanges = trustedHistory
            .groupBy(_.attribution.peer)
            .filter(_._1.isDefined)
            .values
            .toList
            .flatMap { peerRows =>
              peerRows.sliding(2).collect {
                case List(a, b)
                    if (a.attribution.mapping != b.attribution.mapping || a.attribution.key != b.attribution.key) &&
                      !b.at.isBefore(start) && b.at.isBefore(end) =>
                  b.id
              }
            }
          val registrationChanges = trustedHistory
            .filter(_.attribution.kind == "registration")
            .groupBy(_.attribution.registration)
            .values
            .toList
            .flatMap { registrationRows =>
              registrationRows.sliding(2).collect {
                case List(a, b)
                    if a.attribution.key != b.attribution.key && !b.at.isBefore(
                      start
                    ) && b.at.isBefore(end) =>
                  b.id
              }
            }
          val changes = (peerChanges ++ registrationChanges).distinct.sorted
          val evidence = coverage
            .deepMerge(
              Json.obj(
                "window_start" -> stamp(start),
                "window_end" -> stamp(end),
                "coverage_scope" -> "deployment".asJson,
                "source_observation_count" -> rows
                  .map(_.id)
                  .distinct
                  .size
                  .asJson,
                "excluded_conflicting_observations" -> rows
                  .filter(o => conflictingIds(o.id))
                  .map(_.id)
                  .distinct
                  .size
                  .asJson,
                "mapping_change_event_ids" -> changes.asJson,
                "mapping_change_count" -> changes.size.asJson,
                "source_event_ids" -> usable.map(_.id).asJson
              )
            )
            .deepMerge(history(policy))
          projection(
            policy,
            identity(policy.deploymentScope, start.toString),
            evidence,
            "proxy attribution coverage",
            "observed_coverage",
            watermark,
            start
          )
        }
        val cohortEvidence = ProxyBehaviorCohorts.evidence(
          deviceWindows,
          ProxyIntelligencePolicy
            .component(policy.intelligencePolicy, "cohorts"),
          policy.conflictsAttested
        )
        val rawSignals = (windows ++ deviceWindows).flatMap(p =>
          p.evidence.hcursor
            .downField("threat_feed")
            .get[List[Json]]("signals")
            .getOrElse(Nil)
        )
        val signalOverflow = rawSignals.size > 256
        def publish(p: Projection, cohorts: Boolean): Projection =
          val report = p.evidence.hcursor
            .downField("threat_feed")
            .focus
            .getOrElse(Json.obj())
            .mapObject(_.remove("state").remove("signals"))
            .deepMerge(
              Json.obj(
                "signal_ids" -> (if signalOverflow then Nil
                                 else
                                   p.evidence.hcursor
                                     .downField("threat_feed")
                                     .get[List[Json]]("signals")
                                     .getOrElse(Nil)
                                     .flatMap(s => text(s, "signal_id"))
                ).asJson,
                "signal_read_surface" -> "atheros_search.proxy_signal".asJson,
                "publication_disposition" -> (if signalOverflow then
                                                "combined_signal_bound_exceeded"
                                              else "expiry_aware_state").asJson
              )
            )
          val evidence = p.evidence
            .mapObject(_.add("threat_feed", report))
            .deepMerge(if cohorts then
              Json.obj(
                "cohort_input_fingerprint" -> p.fingerprint.asJson,
                "behavioral_cohorts" -> cohortEvidence
                  .getOrElse(p.key, Json.obj())
              )
            else Json.obj())
          projection(
            policy,
            p.key,
            evidence,
            p.semanticText,
            p.disposition,
            p.sourceWatermark,
            p.observedAt
          )
        Right(
          Result(
            flows,
            windows.map(publish(_, false)),
            retained
              .map(_.id)
              .distinct
              .size - supported.map(_.id).distinct.size,
            destinations,
            deviceWindows.map(publish(_, true)),
            attributionWindows,
            if signalOverflow then Nil else rawSignals
          )
        )

  private def history(policy: Policy): Json = Json.obj(
    "retained_history_start" -> stamp(policy.lookbackStart),
    "retained_history_end" -> stamp(policy.asOf),
    "left_censored" -> true.asJson,
    "history_complete_within_bounds" -> (policy.coverageAttested && policy.conflictsAttested &&
      !policy.coverageStart.isAfter(policy.lookbackStart) && !policy.coverageEnd
        .isBefore(policy.asOf)).asJson
  )

  private[processor] def collectionComplete(
      p: Projection,
      conflictsAttested: Boolean
  ): Boolean =
    p.disposition == "available" && conflictsAttested

  private def enrichWindow(
      policy: Policy,
      p: Projection,
      rows: List[Observation],
      flows: List[Projection],
      watermark: Long,
      domainRows: List[Observation]
  ): Projection =
    val start = p.observedAt
    val end = start.plusSeconds(policy.windowSeconds.toLong)
    val inWindow = rows.filter(o => !o.at.isBefore(start) && o.at.isBefore(end))
    val endpoint = text(p.evidence, "endpoint_key")
    val matching =
      flows.filter(f => text(f.evidence, "endpoint_key") == endpoint)
    val currentFlows = matching.filter(f =>
      !f.observedAt.isBefore(start) && f.observedAt.isBefore(end)
    )
    val historical = matching.filter(_.observedAt.isBefore(start))
    val first = rows.headOption
    val novel = first.map(o => !o.at.isBefore(start) && o.at.isBefore(end))
    val complete = collectionComplete(p, policy.conflictsAttested)
    val prior = rows.filter(_.at.isBefore(start)).map(_.source.row)
    val current = inWindow.map(_.source.row)
    val historyStart = text(p.evidence, "valid_from")
      .flatMap(ProxyBehaviorScopes.instant)
      .filter(_.isAfter(policy.lookbackStart))
      .getOrElse(policy.lookbackStart)
    val historyComplete = complete && policy.coverageAttested &&
      !policy.coverageStart.isAfter(historyStart) && !policy.coverageEnd
        .isBefore(end)
    val domainCurrent = domainRows
      .filter(o => !o.at.isBefore(start) && o.at.isBefore(end))
      .map(_.source.row)
    val domainHistory =
      domainRows.filter(_.at.isBefore(start)).map(_.source.row)
    val infrastructure =
      ProxyDestinationInfrastructure.evidence(current, prior, historyComplete)
    val addresses = List("current", "baseline")
      .flatMap { slice =>
        List("selected_ips", "resolved_ips").flatMap(field =>
          infrastructure.hcursor
            .downField(slice)
            .get[List[Json]](field)
            .getOrElse(Nil)
            .flatMap(ip => text(ip, "ip"))
        )
      }
      .distinct
      .sorted
    // Flow validation requires pairing context outside the observation window.
    val pairingStart = start.minus(policy.pairingHorizon)
    val pairingEnd = end.plus(policy.pairingHorizon)
    val flowStartComplete =
      complete && !pairingStart.isBefore(policy.coverageStart) &&
        !pairingEnd.isAfter(policy.asOf) &&
        !pairingEnd.isAfter(policy.coverageEnd) &&
        text(p.evidence, "valid_until")
          .flatMap(ProxyBehaviorScopes.instant)
          .forall(until => !pairingEnd.isAfter(until))
    val evidence = p.evidence
      .deepMerge(
        Json.obj(
          "first_observed_at" -> first.map(_.at.toString).asJson,
          "new_to_retained_scope" -> novel.asJson,
          "new_to_scope_ever" -> Json.Null,
          "domain_registration_time" -> Json.Null,
          "intelligence_context" -> Json.obj(
            "history_start" -> stamp(historyStart),
            "history_end" -> stamp(start),
            "history_complete_within_bounds" -> historyComplete.asJson,
            "left_censored" -> true.asJson,
            "calibration_status" -> "unvalidated".asJson,
            "policy_status" -> (if ProxyIntelligencePolicy.valid(
                                    policy.intelligencePolicy
                                  )
                                then "supported_or_disabled"
                                else "invalid_envelope").asJson
          ),
          "clienthello_drift" -> ProxyClientHelloDrift.evidence(
            current,
            prior,
            historyComplete,
            text(p.evidence, "scope_kind").contains("registration")
          ),
          "destination_infrastructure" -> infrastructure.deepMerge(
            Json.obj(
              "asn" ->
                ProxyAsnEnrichment.evidence(
                  addresses,
                  ProxyIntelligencePolicy
                    .component(policy.intelligencePolicy, "asn_cache"),
                  policy.evaluationTime.getOrElse(policy.asOf)
                )
            )
          ),
          "domain_lexical" -> ProxyDomainLexical.evidence(
            current,
            prior,
            historyComplete,
            domainCurrent,
            domainHistory,
            ProxyIntelligencePolicy
              .component(policy.intelligencePolicy, "lexical")
          ),
          "threat_feed" -> ProxyThreatFeed.evidence(
            current,
            p,
            ProxyIntelligencePolicy
              .component(policy.intelligencePolicy, "threat_feed"),
            policy.evaluationTime.getOrElse(policy.asOf)
          ),
          "polling" -> ProxyBehaviorStatistics.polling(
            inWindow.map(_.source.row),
            matching,
            start,
            end,
            complete,
            flowStartComplete
          ),
          "transfer" -> ProxyBehaviorStatistics
            .transfer(currentFlows, historical, novel),
          "attribution_coverage" -> ProxyBehaviorScopes.coverage(
            inWindow.map(_.attribution)
          ),
          "policy_friction" -> ProxyBehaviorScopes
            .friction(inWindow.map(_.source.row), start, end)
        )
      )
      .deepMerge(history(policy))
    projection(
      policy,
      p.key,
      evidence,
      p.semanticText,
      p.disposition,
      watermark,
      start
    )

  private def windowStart(at: Instant, seconds: Int): Instant =
    Instant.ofEpochSecond(
      Math.floorDiv(at.getEpochSecond, seconds.toLong) * seconds.toLong
    )

  private def projection(
      policy: Policy,
      key: String,
      evidence: Json,
      semanticText: String,
      disposition: String,
      watermark: Long,
      at: Instant
  ): Projection =
    val stable = evidence
      .mapObject(_.remove("calculated_at"))
      .deepMerge(
        Json.obj(
          "deployment_scope" -> policy.deploymentScope.asJson,
          "policy_version" -> policy.policyVersion.asJson,
          "calculation_version" -> CalculationVersion.asJson,
          "normalization_version" -> ProxyAuthority.Version.asJson,
          "source_watermark" -> watermark.asJson
        )
      )
    // These bounds describe the evaluation, rather than the observations. Keep
    // source timestamps, registration validity, expiry and completeness in the
    // identity so real changes still invalidate previously published evidence.
    val content = stable
      .mapObject(
        _.remove("retained_history_start").remove("retained_history_end")
      )
      .mapObject { obj =>
        obj("intelligence_context").fold(obj)(context =>
          obj.add(
            "intelligence_context",
            context.mapObject(_.remove("history_start"))
          )
        )
      }
    val fingerprint = Sha256Utils.sha256Hex(
      CanonicalPrinter.print(content).getBytes(StandardCharsets.UTF_8)
    )
    Projection(
      key,
      stable.deepMerge(Json.obj("calculated_at" -> stamp(policy.asOf))),
      semanticText,
      fingerprint,
      disposition,
      watermark,
      at
    )

  private def flow(
      policy: Policy,
      correlation: String,
      sources: List[Source],
      conflicts: List[Source],
      conflictingIds: Set[String],
      conflictCorrelations: Set[String]
  ): Projection =
    val ordered = sources.sortBy(source =>
      (
        time(source.row).getOrElse(Instant.MIN),
        text(source.row, "event_id").getOrElse("")
      )
    )
    val opens =
      ordered.filter(source => text(source.row, "type").contains("tunnel_open"))
    val terminals = ordered.filter(source =>
      text(source.row, "type").contains("tunnel_close")
    )
    val openedAt = opens.headOption.flatMap(source => time(source.row))
    val closedAt = terminals.headOption.flatMap(source => time(source.row))
    val lower = closedAt.map(_.minus(policy.pairingHorizon)).orElse(openedAt)
    val upper = openedAt.map(_.plus(policy.pairingHorizon)).orElse(closedAt)
    val sourceValid = ordered.forall { source =>
      val row = source.row
      val expected = if text(row, "type").contains("tunnel_open") then 1 else 2
      uuid(row, "event_id").isDefined && time(row).isDefined && text(
        row,
        "host"
      ).exists(_.nonEmpty) &&
      row.hcursor.get[Int]("event_sequence").contains(expected) && row.hcursor
        .get[Boolean]("blocked")
        .contains(false)
    }
    val disposition =
      if conflictCorrelations(correlation) || ordered.exists(source =>
          uuid(source.row, "event_id").exists(conflictingIds)
        ) || opens.size > 1 || terminals.size > 1
      then "ambiguous"
      else if !sourceValid then "ineligible"
      else if openedAt
          .zip(closedAt)
          .exists((open, close) => close.isBefore(open)) ||
        opens.headOption
          .zip(terminals.headOption)
          .exists((open, close) =>
            text(open.row, "host") != text(close.row, "host") || endpointKey(
              open.row
            ) != endpointKey(close.row)
          )
      then "inconsistent"
      else if closedAt.zip(upper).exists((close, end) => close.isAfter(end))
      then "outside_horizon"
      else if !policy.conflictsAttested then "conflict_coverage_unavailable"
      else if policy.coverageAttested && lower.exists(
          _.isBefore(policy.coverageStart)
        )
      then "retention_truncated"
      else if !policy.coverageAttested || upper.exists(end =>
          !end.isBefore(policy.coverageEnd)
        )
      then "coverage_limited"
      else if opens.isEmpty then "missing_open"
      else if terminals.isEmpty then "missing_terminal"
      else "validated"
    val terminal = Option.when(terminals.size == 1)(terminals.head.row)
    val measurement = terminal
      .map(row => transfer(row, disposition == "validated"))
      .getOrElse(unavailableTransfer)
    val at = closedAt.orElse(openedAt).getOrElse(policy.lookbackStart)
    val normalized =
      ordered.headOption.map(source => ProxyEvidence.normalized(source.row))
    val host = normalized.flatMap(value => text(value, "host_normalized"))
    val endpoint = ordered.headOption.flatMap(source => endpointKey(source.row))
    val references = ordered.flatMap(source =>
      uuid(source.row, "event_id").map(id =>
        Json.obj("event_id" -> id.asJson, "revision" -> source.revision.asJson)
      )
    )
    val relevantConflicts = conflicts.filter(source =>
      source.conflicts.asObject.exists(
        _.values.exists(_.asString.contains(correlation))
      ) || uuid(source.row, "correlation_id").contains(correlation)
    )
    val watermark =
      (ordered ++ relevantConflicts).map(_.revision).maxOption.getOrElse(0L)
    val evidence = Json
      .obj(
        "correlation_id" -> correlation.asJson,
        "disposition" -> disposition.asJson,
        "source_event_ids" -> ordered
          .flatMap(source => uuid(source.row, "event_id"))
          .distinct
          .sorted
          .asJson,
        "source_references" -> references.asJson,
        "host_normalized" -> host.asJson,
        "endpoint_key" -> endpoint.asJson,
        "opened_at" -> openedAt.map(_.toString).asJson,
        "closed_at" -> closedAt.map(_.toString).asJson,
        "pairing_horizon_ms" -> policy.pairingHorizon.toMillis.asJson,
        "observation_elapsed_ms" -> openedAt
          .zip(closedAt)
          .filter((open, close) => !close.isBefore(open))
          .map((open, close) => Duration.between(open, close).toMillis)
          .asJson
      )
      .deepMerge(measurement)
    val bytesStatus = text(measurement, "bytes_status").getOrElse("unavailable")
    val prose = List(
      "proxy flow",
      host.getOrElse("host unavailable"),
      disposition,
      s"transfer $bytesStatus",
      "duration scope retained separately"
    ).mkString(" ")
    projection(
      policy,
      identity(policy.deploymentScope, correlation),
      evidence,
      prose,
      disposition,
      watermark,
      at
    )

  private val unavailableTransfer = Json.obj(
    "bytes_status" -> "unavailable".asJson,
    "bytes_up" -> Json.Null,
    "bytes_down" -> Json.Null,
    "partial_bytes_up" -> Json.Null,
    "partial_bytes_down" -> Json.Null,
    "duration_status" -> "unavailable".asJson,
    "duration_ms" -> Json.Null
  )

  private def transfer(row: Json, validated: Boolean): Json =
    val measurement = row.hcursor.downField("measurement")
    val versioned =
      text(row, "evidence_version").contains(ProxyEvidence.Version) &&
        measurement.get[String]("version").contains("proxy-measurement-v1")
    val provenance = versioned && measurement
      .get[String]("source")
      .contains("successful_socket_writes") &&
      measurement.get[String]("byte_unit").contains("bytes") &&
      measurement
        .get[String]("up_direction")
        .contains("client_to_destination") &&
      measurement
        .get[String]("down_direction")
        .contains("destination_to_client")
    val counters =
      nonnegative(row, "bytes_up").zip(nonnegative(row, "bytes_down"))
    val declared = measurement.get[String]("bytes_status").toOption
    val destinationKnown = !text(row, "reason").contains("allowed_plaintext")
    val measured =
      validated && provenance && destinationKnown && declared.contains(
        "measured_terminal"
      ) &&
        measurement
          .get[String]("termination")
          .contains("measured_terminal") && counters.isDefined
    val partial = validated && provenance && declared.contains("partial") &&
      measurement
        .get[String]("termination")
        .contains("partial") && counters.isDefined
    val status = if measured then "measured_terminal"
    else if partial then "partial"
    else if !versioned then "legacy_unknown"
    else "unavailable"
    val duration = Option
      .when(
        validated && versioned && measurement
          .get[String]("duration_status")
          .contains("connection_elapsed")
      )(nonnegative(row, "duration_ms"))
      .flatten
    Json.obj(
      "bytes_status" -> status.asJson,
      "bytes_up" -> Option.when(measured)(counters.get._1).asJson,
      "bytes_down" -> Option.when(measured)(counters.get._2).asJson,
      "partial_bytes_up" -> Option.when(partial)(counters.get._1).asJson,
      "partial_bytes_down" -> Option.when(partial)(counters.get._2).asJson,
      "duration_status" -> (if duration.isDefined then "connection_elapsed"
                            else "unavailable").asJson,
      "duration_ms" -> duration.asJson
    )

  private def hostWindow(
      policy: Policy,
      endpoint: String,
      start: Instant,
      sources: List[Observation],
      flows: List[Projection],
      conflictingIds: Set[String]
  ): Projection =
    val end = start.plusSeconds(policy.windowSeconds.toLong)
    val candidates = sources.filter(source =>
      !source.at.isBefore(start) && source.at.isBefore(end)
    )
    val inWindow = candidates.filterNot(source => conflictingIds(source.id))
    val associatedFlows = flows.filter(flow =>
      text(flow.evidence, "endpoint_key").contains(endpoint) &&
        !flow.observedAt.isBefore(start) && flow.observedAt.isBefore(end)
    )
    val validated = associatedFlows.filter(_.disposition == "validated")
    val measured = validated.filter(flow =>
      text(flow.evidence, "bytes_status").contains("measured_terminal")
    )
    val blocked = inWindow.count(source =>
      source.source.row.hcursor
        .get[Boolean]("blocked")
        .getOrElse(BlockedTypes(source.eventType))
    )
    val latest = inWindow.lastOption
    val watermark = (candidates.map(_.source.revision) ++ associatedFlows.map(
      _.sourceWatermark
    )).maxOption.getOrElse(0L)
    val full = policy.coverageAttested && !start.isBefore(
      policy.coverageStart
    ) && !end.isAfter(policy.coverageEnd) && !start.isBefore(
      policy.lookbackStart
    ) && !end.isAfter(policy.asOf)
    val disposition = if candidates.size != inWindow.size then
      "ambiguous_source_evidence"
    else if full then "available"
    else "coverage_limited"
    val totalsStatus = if measured.isEmpty then "unavailable"
    else if measured.size == validated.size then "measured_terminal"
    else "measured_subset"
    val coveredStart = if start.isBefore(policy.coverageStart) then
      policy.coverageStart
    else start
    val coveredEnd =
      if end.isAfter(policy.coverageEnd) then policy.coverageEnd else end
    val overlap = coveredStart.isBefore(coveredEnd)
    val evidence = Json.obj(
      "endpoint_key" -> endpoint.asJson,
      "host_normalized" -> sources.head.host.asJson,
      "window_start" -> stamp(start),
      "window_end" -> stamp(end),
      "disposition" -> disposition.asJson,
      "observed_count" -> inWindow.size.asJson,
      "excluded_observations" -> (candidates.size - inWindow.size).asJson,
      "allowed_observations" -> (inWindow.size - blocked).asJson,
      "blocked_observations" -> blocked.asJson,
      "validated_flows" -> validated.size.asJson,
      "measured_flows" -> measured.size.asJson,
      "unavailable_flows" -> (validated.size - measured.size).asJson,
      "transfer_totals_status" -> totalsStatus.asJson,
      "bytes_up" -> Option
        .when(measured.nonEmpty)(
          measured
            .map(flow =>
              BigInt(flow.evidence.hcursor.get[Long]("bytes_up").toOption.get)
            )
            .sum
        )
        .asJson,
      "bytes_down" -> Option
        .when(measured.nonEmpty)(
          measured
            .map(flow =>
              BigInt(flow.evidence.hcursor.get[Long]("bytes_down").toOption.get)
            )
            .sum
        )
        .asJson,
      "last_event_id" -> latest.map(_.id).asJson,
      "last_event_time" -> latest.map(_.at.toString).asJson,
      "source_event_ids" -> inWindow.map(_.id).distinct.sorted.asJson,
      "flow_keys" -> validated.map(_.key).sorted.asJson,
      "coverage_start" -> Option.when(overlap)(coveredStart.toString).asJson,
      "coverage_end" -> Option.when(overlap)(coveredEnd.toString).asJson,
      "attempt_count" -> Json.Null
    )
    projection(
      policy,
      identity(policy.deploymentScope, endpoint, start.toString),
      evidence,
      "proxy host window " + sources.head.host
        .getOrElse("host unavailable") + " " + disposition,
      disposition,
      watermark,
      start
    )
