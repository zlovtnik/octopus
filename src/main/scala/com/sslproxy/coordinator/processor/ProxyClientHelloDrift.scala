package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import scala.util.Try

/** Scoped software-stack changes in retained ClientHello offers, not identity
  * or threat attribution. Callers supply one endpoint and registration epoch.
  */
object ProxyClientHelloDrift:
  val Version = "proxy-clienthello-drift-v1"
  val MinimumSamples = 6
  private val MaximumRows = 10000
  private val MaximumValues = 128
  private val MaximumReferences = 64
  private val Parsers = Set("proxy-clienthello-v1", "proxy-clienthello-v2")
  private val EventId = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"

  private final case class Observation(id: String, at: Instant, row: Json)
  private final case class Sample(observation: Observation, parser: String, fingerprint: String, alpn: Option[String])

  private def text(row: Json, key: String): Option[String] = row.hcursor.get[String](key).toOption

  private def observations(rows: List[Json]): List[Observation] =
    rows.flatMap { row =>
      for
        id <- text(row, "event_id").filter(_.matches(EventId))
        at <- text(row, "time").flatMap(value => Try(Instant.parse(value)).toOption)
        _ <- text(row, "type").filter(kind => ProxyEvidence.AllowedEvents(kind) && kind != "tunnel_close")
      yield Observation(id, at, row)
    }.groupBy(_.id).values.toList.flatMap { duplicates =>
      // Contradictory versions cannot create a profile, even in direct callers.
      Option.when(duplicates.map(_.row).distinct.size == 1)(duplicates.head)
    }.sortBy(o => (o.at, o.id))

  private def fingerprint(value: String): Boolean =
    value.length <= 4096 && value.split(",", -1).length == 5 &&
      value.split(",", -1).zipWithIndex.forall { (field, index) =>
        (field.nonEmpty || index > 0) && (field.isEmpty || field.split("-", -1).forall(number =>
          number.matches("[0-9]{1,5}") && Try(number.toInt).toOption.exists(_ <= (if index == 4 then 255 else 65535))))
      }

  private def sample(o: Observation): Option[Sample] =
    val c = o.row.hcursor.downField("tls_evidence")
    for
      _ <- Option.when(text(o.row, "evidence_version").contains(ProxyEvidence.Version))(())
      _ <- Option.when(c.get[String]("source").contains("client_hello") &&
        c.get[String]("parse_status").contains("parsed") &&
        c.get[String]("semantics").contains("offered_not_negotiated"))(())
      parser <- c.get[String]("parser_version").toOption.filter(Parsers)
      value <- c.get[String]("ja3_lite").toOption.filter(fingerprint)
    yield Sample(o, parser, value, c.get[String]("offered_alpn").toOption.filter(_.matches("[A-Za-z0-9./_-]{1,64}")))

  private def counts(values: List[String]): Json = Json.obj(
    values.groupBy(identity).toList.sortBy(_._1).take(MaximumValues).map((value, rows) => value -> rows.size.asJson)*
  )

  private def profile(rows: List[Sample]): Json = Json.obj(
    "sample_size" -> rows.size.asJson,
    "ja3_lite_counts" -> counts(rows.map(_.fingerprint)),
    "omitted_ja3_lite_value_count" -> math.max(0, rows.map(_.fingerprint).distinct.size - MaximumValues).asJson,
    "first_offered_alpn_counts" -> counts(rows.flatMap(_.alpn)),
    "omitted_alpn_value_count" -> math.max(0, rows.flatMap(_.alpn).distinct.size - MaximumValues).asJson,
    "alpn_unavailable_count" -> rows.count(_.alpn.isEmpty).asJson,
    "first_observed_at" -> rows.headOption.map(_.observation.at.toString).asJson,
    "last_observed_at" -> rows.lastOption.map(_.observation.at.toString).asJson,
    "source_event_ids" -> rows.map(_.observation.id).sorted.take(MaximumReferences).asJson,
    "omitted_source_event_count" -> math.max(0, rows.size - MaximumReferences).asJson
  )

  /** History is retained and earlier than the window supplied by the caller.
    * A second event-time check prevents accidental future-sample leakage.
    */
  def evidence(current: List[Json], history: List[Json], collectionComplete: Boolean, registrationScope: Boolean): Json =
    val overflow = current.size.toLong + history.size.toLong > MaximumRows
    val now = if overflow then Nil else observations(current)
    val nowIds = now.map(_.id).toSet
    val before = if overflow then Nil else observations(history).filter(o =>
      now.headOption.exists(_.at.isAfter(o.at)) && !nowIds(o.id))
    val nowSamples = now.flatMap(sample)
    val beforeSamples = before.flatMap(sample)
    val versions = (nowSamples.map(_.parser) ++ beforeSamples.map(_.parser)).distinct.sorted
    val comparisons = versions.map { parser =>
      val a = nowSamples.filter(_.parser == parser)
      val b = beforeSamples.filter(_.parser == parser)
      val newFingerprints = a.map(_.fingerprint).distinct.sorted.diff(b.map(_.fingerprint).distinct)
      val newAlpn = a.flatMap(_.alpn).distinct.sorted.diff(b.flatMap(_.alpn).distinct)
      val enough = a.size >= MinimumSamples && b.size >= MinimumSamples
      // Missing ALPN is availability, not a change; its own comparison needs
      // the minimum sample count on both sides.
      val alpnEnough = a.count(_.alpn.isDefined) >= MinimumSamples && b.count(_.alpn.isDefined) >= MinimumSamples
      val changed = newFingerprints.nonEmpty || (alpnEnough && newAlpn.nonEmpty)
      val disposition =
        if !registrationScope then "unsupported_attribution_scope"
        else if !collectionComplete then "collection_censored"
        else if !enough then "insufficient_samples"
        else if changed then "drift_candidate"
        else "no_new_offer_values"
      Json.obj(
        "parser_version" -> parser.asJson,
        "current" -> profile(a), "baseline" -> profile(b),
        "new_ja3_lite_values" -> newFingerprints.take(MaximumValues).asJson,
        "new_ja3_lite_value_count" -> newFingerprints.size.asJson,
        "new_first_offered_alpn_values" -> newAlpn.take(MaximumValues).asJson,
        "new_first_offered_alpn_value_count" -> newAlpn.size.asJson,
        "alpn_comparison_available" -> alpnEnough.asJson,
        "candidate" -> (disposition == "drift_candidate").asJson,
        "disposition" -> disposition.asJson
      )
    }
    Json.obj(
      "calculation_version" -> Version.asJson,
      "interpretation" -> "software_stack_drift_candidate_only".asJson,
      "calibration_status" -> "provisional_unvalidated".asJson,
      "ownership" -> "unavailable".asJson,
      "benign_explanations" -> List("software_update", "application_mix_change").asJson,
      "alpn_semantics" -> "first_offered_protocol_only_not_complete_list_or_negotiated".asJson,
      "sample_unit" -> "unique_observations_excluding_tunnel_close".asJson,
      "baseline_provenance" -> "strictly_earlier_retained_same_endpoint_and_scope".asJson,
      "minimum_samples_per_parser_per_slice" -> MinimumSamples.asJson,
      "collection_complete" -> collectionComplete.asJson,
      "registration_scope" -> registrationScope.asJson,
      "current_denominator" -> now.size.asJson,
      "current_available_count" -> nowSamples.size.asJson,
      "current_unavailable_count" -> (now.size - nowSamples.size).asJson,
      "baseline_denominator" -> before.size.asJson,
      "baseline_available_count" -> beforeSamples.size.asJson,
      "baseline_unavailable_count" -> (before.size - beforeSamples.size).asJson,
      "candidate" -> comparisons.exists(_.hcursor.get[Boolean]("candidate").contains(true)).asJson,
      "disposition" -> (if overflow then "source_row_bound_exceeded" else if versions.isEmpty then "unavailable" else "profiles_available").asJson,
      "parser_profiles" -> comparisons.asJson
    )
