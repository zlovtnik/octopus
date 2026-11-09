package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.Instant

/** Observed destination changes, without attributing client addresses or ASN ownership.
  * Callers supply one endpoint/attribution scope and strictly earlier history.
  */
object ProxyDestinationInfrastructure:
  val Version = "proxy-destination-infrastructure-v1"
  private val MaximumRows = 10000
  private val MaximumResolvedPerObservation = 64
  private val MaximumAddresses = 128
  private val MaximumReferences = 32

  private final case class Sample(id: String, at: Instant, selected: Option[String], resolved: List[String])
  private final case class Profile(samples: List[Sample], overflow: Boolean):
    def selected: List[String] = samples.flatMap(_.selected).distinct.sorted
    def resolved: List[String] = samples.flatMap(_.resolved).distinct.sorted

  private def text(row: Json, key: String): Option[String] =
    row.hcursor.get[String](key).toOption

  private def address(value: String): Option[String] =
    // Reuse the authority parser only after excluding names, ports, zones and
    // brackets. Its IPv6 path accepts numeric characters only and cannot resolve DNS.
    val numeric = value.nonEmpty && value.length <= 45 &&
      value.forall(c => "0123456789abcdefABCDEF:.".contains(c))
    Option.when(numeric)(ProxyAuthority.normalize(Some(value), "original_destination"))
      .filter(p => Set("valid_ipv4", "valid_ipv6")(p.parse_status))
      .filter(_.port.isEmpty)
      .flatMap(_.host_normalized)

  private def profile(rows: List[Json]): Profile =
    val unique = rows.flatMap(row => text(row, "event_id").filter(_.nonEmpty).map(_ -> row))
      .groupMap(_._1)(_._2).toList.sortBy(_._1)
      .flatMap { case (_, versions) => Option.when(versions.distinct.size == 1)(versions.head) }
    val eligible = unique.filter(row =>
      text(row, "evidence_version").contains(ProxyEvidence.Version) &&
        text(row, "type").contains("tunnel_open") &&
        row.hcursor.get[Boolean]("blocked").contains(false)
    )
    val oversized = eligible.exists(row => row.hcursor.get[List[Json]]("resolved_ips").toOption
      .exists(_.size > MaximumResolvedPerObservation))
    val samples = eligible.flatMap { row =>
      for
        id <- text(row, "event_id")
        at <- text(row, "time").flatMap(ProxyBehaviorScopes.instant)
      yield Sample(id, at, text(row, "selected_ip").flatMap(address),
        row.hcursor.get[List[Json]]("resolved_ips").toOption.toList.flatten
          .take(MaximumResolvedPerObservation).flatMap(_.asString.flatMap(address)).distinct.sorted)
    }.sortBy(s => (s.at, s.id))
    val selected = samples.flatMap(_.selected).distinct.size
    val resolved = samples.flatMap(_.resolved).distinct.size
    Profile(samples, oversized || selected > MaximumAddresses || resolved > MaximumAddresses)

  private def references(samples: List[Sample]): Json = Json.obj(
    "source_event_ids" -> samples.map(_.id).sorted.take(MaximumReferences).asJson,
    "omitted_reference_count" -> (samples.size - MaximumReferences).max(0).asJson,
    "first_observed_at" -> samples.headOption.map(_.at.toString).asJson,
    "last_observed_at" -> samples.lastOption.map(_.at.toString).asJson
  )

  private def published(p: Profile): Json =
    def counts(selected: Boolean): Json =
      val values = if selected then p.selected else p.resolved
      values.map { ip =>
        val supporting = p.samples.filter(s => if selected then s.selected.contains(ip) else s.resolved.contains(ip))
        Json.obj("ip" -> ip.asJson, "observation_count" -> supporting.size.asJson)
          .deepMerge(references(supporting))
      }.asJson
    Json.obj(
      "eligible_open_observations" -> p.samples.size.asJson,
      "selected_ip_observations" -> p.samples.count(_.selected.isDefined).asJson,
      "resolved_ip_observations" -> p.samples.count(_.resolved.nonEmpty).asJson,
      "selected_ips" -> counts(true),
      "resolved_ips" -> counts(false)
    ).deepMerge(references(p.samples))

  private def change(current: List[String], baseline: List[String], complete: Boolean): Json =
    val disposition = if current.isEmpty then "current_unavailable"
      else if baseline.isEmpty then "history_unavailable"
      else if !complete then "collection_censored"
      else if current == baseline then "unchanged_observed_set"
      else "changed_observed_set"
    val comparable = current.nonEmpty && baseline.nonEmpty && complete
    Json.obj(
      "disposition" -> disposition.asJson,
      "observed_change" -> Option.when(comparable)(current != baseline).asJson,
      "newly_observed_ips" -> (if comparable then current.diff(baseline) else Nil).asJson,
      "not_observed_in_current_window_ips" -> (if comparable then baseline.diff(current) else Nil).asJson
    )

  def evidence(current: List[Json], history: List[Json], collectionComplete: Boolean): Json =
    val rowOverflow = current.size.toLong + history.size.toLong > MaximumRows
    val now = if rowOverflow then Profile(Nil, true) else profile(current)
    val prior = if rowOverflow then Profile(Nil, true) else profile(history)
    val overflow = now.overflow || prior.overflow
    val usableNow = if overflow then Profile(Nil, false) else now
    val usablePrior = if overflow then Profile(Nil, false) else prior
    val disposition = if overflow then "input_or_address_bound_exceeded"
      else if now.samples.isEmpty then "no_supported_open_observations"
      else if now.selected.isEmpty && now.resolved.isEmpty then "destination_ips_unavailable"
      else if !collectionComplete then "collection_censored"
      else "descriptive_evidence"
    Json.obj(
      "calculation_version" -> Version.asJson,
      "disposition" -> disposition.asJson,
      "interpretation" -> "observed_infrastructure_change_only".asJson,
      "sample_unit" -> "unique_unblocked_tunnel_open_observations".asJson,
      "selected_ip_source" -> "connected_upstream_socket_peer".asJson,
      "resolved_ip_source" -> "resolver_candidates_not_connection_evidence".asJson,
      "collection_complete" -> collectionComplete.asJson,
      "history_scope" -> "earlier_retained_observations_same_endpoint_and_attribution_scope".asJson,
      "history_left_censored" -> true.asJson,
      "current" -> published(usableNow),
      "baseline" -> published(usablePrior),
      "selected_ip_change" -> change(usableNow.selected, usablePrior.selected, collectionComplete),
      "resolved_ip_change" -> change(usableNow.resolved, usablePrior.resolved, collectionComplete),
      "asn" -> Json.obj("disposition" -> "unavailable_no_versioned_enrichment_source".asJson),
      "caveat" -> "CDN rotation, load balancing and shared infrastructure can change observed addresses".asJson
    )
