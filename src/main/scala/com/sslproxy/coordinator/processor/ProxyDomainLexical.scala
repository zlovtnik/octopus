package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.{Instant, OffsetDateTime}
import scala.util.Try

/** DNS features and opt-in candidates using pinned authority normalization.
  * Supplied calibration attestations never establish a confirmed threat.
  */
object ProxyDomainLexical:
  val Version = "proxy-domain-lexical-v2"
  private val MaximumRows = 10000
  private val MaximumReferences = 64
  private val MaximumHosts = 128
  private val PolicyFields = Set("version", "calibration_reference", "heldout_dataset", "trained_until",
    "min_label_length", "min_entropy", "min_digit_fraction", "max_new_subdomains", "minimum_samples")

  private final case class DetectorPolicy(version: String, calibration: String, dataset: String,
      trainedUntil: Instant, length: Int, entropy: Double, digits: Double, maxNew: Int, samples: Int)

  final case class EvaluationCase(id: String, observations: List[Json], history: List[Json],
      positive: Boolean, controlGroup: String)

  private def detectorPolicy(value: Json): Option[DetectorPolicy] =
    val c = value.hcursor
    def token(key: String, limit: Int): Option[String] = c.get[String](key).toOption.filter(s =>
      s.nonEmpty && s.length <= limit && s.forall(ch => ch >= '!' && ch <= '~'))
    def integer(key: String, lower: Int, upper: Int): Option[Int] =
      c.downField(key).focus.flatMap(_.asNumber).flatMap(_.toInt).filter(n => n >= lower && n <= upper)
    def decimal(key: String, upper: Double): Option[Double] =
      c.downField(key).focus.flatMap(_.asNumber).map(_.toDouble)
        .filter(n => n.isFinite && n >= 0.0 && n <= upper)
    for
      _ <- value.asObject.filter(_.keys.toSet == PolicyFields)
      _ <- Option.when(value.noSpaces.length <= 4096)(())
      version <- token("version", 64)
      calibration <- token("calibration_reference", 256)
      dataset <- token("heldout_dataset", 128)
      trained <- c.get[String]("trained_until").toOption.flatMap(s => Try(OffsetDateTime.parse(s).toInstant).toOption)
      length <- integer("min_label_length", 1, 63)
      labelEntropy <- decimal("min_entropy", 6.0)
      digits <- decimal("min_digit_fraction", 1.0)
      maxNew <- integer("max_new_subdomains", 1, MaximumRows)
      samples <- integer("minimum_samples", 1, MaximumRows)
    yield DetectorPolicy(version, calibration, dataset, trained, length, labelEntropy, digits, maxNew, samples)

  private final case class Observation(id: String, at: Instant, host: String, domain: Option[String])

  private def text(row: Json, key: String): Option[String] =
    row.hcursor.get[String](key).toOption

  private def observations(rows: List[Json]): List[Observation] =
    rows.groupBy(row => text(row, "event_id")).toList.flatMap { case (id, versions) =>
      val distinct = versions.distinct
      if distinct.size != 1 then Nil
      else
        val row = distinct.head
        val normalized = ProxyEvidence.normalized(row)
        (for
          eventId <- id.filter(value => value.nonEmpty && value.length <= 128)
          at <- text(row, "time").flatMap(value => Try(OffsetDateTime.parse(value).toInstant).toOption)
          _ <- text(row, "type").filter(ProxyEvidence.AllowedEvents)
          _ <- text(normalized, "parse_status").filter(_ == "valid_dns")
          host <- text(normalized, "host_normalized")
        yield Observation(eventId, at, host, text(normalized, "registrable_domain"))).toList
    }.sortBy(row => (row.at, row.id))

  private def entropy(value: String): Double =
    value.groupBy(identity).toList.sortBy(_._1).map { (_, occurrences) =>
      val probability = occurrences.length.toDouble / value.length
      -probability * (math.log(probability) / math.log(2.0))
    }.sum

  private def features(host: String): Json =
    val labels = host.split("\\.").toList
    Json.obj(
      "host_normalized" -> host.asJson,
      "host_length" -> host.length.asJson,
      "label_count" -> labels.size.asJson,
      "maximum_label_length" -> labels.map(_.length).max.asJson,
      "labels" -> labels.map { label =>
        Json.obj(
          "length" -> label.length.asJson,
          "shannon_entropy_bits_per_character" -> entropy(label).asJson,
          "digit_fraction" -> (label.count(c => c >= '0' && c <= '9').toDouble / label.length).asJson
        )
      }.asJson
    )

  private def detector(now: List[Observation], churn: List[Json], complete: Boolean, policy: Json,
      evaluationStart: Option[Instant]): Json =
    if policy.asObject.exists(_.isEmpty) then
      Json.obj("detector" -> Json.obj("disposition" -> "disabled_no_policy".asJson))
    else detectorPolicy(policy) match
      case None => Json.obj(
        "candidate" -> Json.Null,
        "calibration_status" -> "invalid_detector_policy".asJson,
        "detector" -> Json.obj("disposition" -> "invalid_policy".asJson))
      case Some(p) =>
        val labelMatches = now.map(_.host).distinct.sorted.flatMap { host =>
          host.split("\\.").zipWithIndex.toList.collect {
            case (label, index) if label.length >= p.length && entropy(label) >= p.entropy &&
              label.count(c => c >= '0' && c <= '9').toDouble / label.length >= p.digits =>
              Json.obj("host_normalized" -> host.asJson, "label_index" -> index.asJson)
          }
        }
        val churnMatches = churn.filter { value =>
          val c = value.hcursor
          c.get[String]("disposition").contains("retained_observations_only") &&
            c.get[Int]("current_observation_count").exists(_ >= p.samples) &&
            c.get[Int]("prior_observation_count").exists(_ >= p.samples) &&
            c.get[Int]("new_to_retained_history_subdomain_count").exists(_ > p.maxNew)
        }.flatMap(_.hcursor.get[String]("registrable_domain").toOption)
        val disposition =
          if now.isEmpty then "dns_input_unavailable"
          else if !evaluationStart.exists(_.isAfter(p.trainedUntil)) then "training_cutoff_overlaps_evaluation"
          else if !complete then "collection_censored"
          else if now.size < p.samples then "insufficient_samples"
          else "evaluated_candidate_only"
        val evaluated = disposition == "evaluated_candidate_only"
        val matched = labelMatches.nonEmpty || churnMatches.nonEmpty
        Json.obj(
          "candidate" -> Option.when(evaluated)(matched).asJson,
          "interpretation" -> "lexical_anomaly_candidate_only_not_confirmed_threat".asJson,
          "calibration_status" -> "supplied_policy_attestation_not_independently_verified".asJson,
          "false_positive_disposition" -> "offline_labeled_evaluation_required".asJson,
          "detector" -> Json.obj(
            "calculation_version" -> Version.asJson,
            "disposition" -> disposition.asJson,
            "policy_version" -> p.version.asJson,
            "calibration_reference" -> p.calibration.asJson,
            "heldout_dataset" -> p.dataset.asJson,
            "trained_until" -> p.trainedUntil.toString.asJson,
            "thresholds" -> policy.mapObject(_.filter { case (key, _) =>
              !Set("version", "calibration_reference", "heldout_dataset", "trained_until")(key)
            }),
            "sample_size" -> now.size.asJson,
            "sample_unit" -> "unique_supported_dns_observations".asJson,
            "matched_label_count" -> labelMatches.size.asJson,
            "matched_labels" -> labelMatches.take(MaximumHosts).asJson,
            "matched_labels_omitted" -> (labelMatches.size - MaximumHosts).max(0).asJson,
            "churn_match_count" -> churnMatches.size.asJson,
            "churn_domains" -> churnMatches.take(MaximumHosts).asJson,
            "churn_domains_omitted" -> (churnMatches.size - MaximumHosts).max(0).asJson,
            "rule_semantics" -> "all_label_minima_or_new_subdomains_exceed_maximum_with_both_slice_samples".asJson
          )
        )

  /** Reports actual fixture outcomes without treating supplied labels as a
    * production calibration. Each evaluated case is one classification unit.
    */
  def evaluate(policy: Json, cases: List[EvaluationCase]): Json =
    val p = detectorPolicy(policy)
    val bounded = cases.size <= 512 && cases.map(c => c.observations.size.toLong + c.history.size.toLong).sum <= MaximumRows
    val valid = cases.map(_.id).distinct.size == cases.size && cases.forall(c =>
      c.id.nonEmpty && c.id.length <= 128 && c.controlGroup.nonEmpty && c.controlGroup.length <= 64 &&
        (c.id + c.controlGroup).forall(ch => ch >= '!' && ch <= '~'))
    if p.isEmpty || !bounded || !valid then
      Json.obj("calculation_version" -> Version.asJson, "disposition" -> "invalid_evaluation_input".asJson)
    else
      val outcomes = cases.sortBy(_.id).map { c =>
        val result = evidence(c.observations, c.history, true, c.observations, c.history, policy)
        (c, result.hcursor.get[Boolean]("candidate").toOption)
      }
      def metrics(rows: List[(EvaluationCase, Option[Boolean])]): Json =
        val scored = rows.collect { case (c, Some(candidate)) => (c, candidate) }
        val negative = scored.count { case (c, _) => !c.positive }
        val positive = scored.count(_._1.positive)
        val falsePositive = scored.count((c, candidate) => !c.positive && candidate)
        Json.obj(
          "case_count" -> rows.size.asJson, "evaluated_count" -> scored.size.asJson,
          "excluded_count" -> (rows.size - scored.size).asJson,
          "positive_count" -> positive.asJson, "negative_count" -> negative.asJson,
          "true_positive" -> scored.count((c, candidate) => c.positive && candidate).asJson,
          "false_negative" -> scored.count((c, candidate) => c.positive && !candidate).asJson,
          "false_positive" -> falsePositive.asJson,
          "true_negative" -> scored.count((c, candidate) => !c.positive && !candidate).asJson,
          "false_positive_rate" -> Option.when(negative > 0)(falsePositive.toDouble / negative).asJson
        )
      val excluded = outcomes.collect { case (c, None) => c.id }
      Json.obj(
        "calculation_version" -> Version.asJson,
        "disposition" -> "labeled_fixture_evaluation_only".asJson,
        "policy_version" -> p.map(_.version).asJson,
        "heldout_dataset" -> p.map(_.dataset).asJson,
        "label_provenance" -> "caller_supplied_not_independently_reviewed".asJson,
        "classification_unit" -> "evaluation_case".asJson,
        "metrics" -> metrics(outcomes),
        "groups" -> Json.obj(outcomes.groupBy(_._1.controlGroup).toList.sortBy(_._1)
          .map((group, rows) => group -> metrics(rows))*),
        "excluded_case_ids" -> excluded.take(MaximumReferences).asJson,
        "excluded_case_ids_omitted" -> (excluded.size - MaximumReferences).max(0).asJson
      )

  /** The optional domain rows must have the same deployment/registration scope
    * as endpoint rows. The caller supplies half-open current and earlier-only
    * history windows from the same bounded retained snapshot.
    */
  def evidence(
      current: List[Json],
      history: List[Json],
      collectionComplete: Boolean,
      domainCurrent: List[Json] = Nil,
      domainHistory: List[Json] = Nil,
      policy: Json = Json.obj()
  ): Json =
    val bounded = List(current, history, domainCurrent, domainHistory).forall(_.size <= MaximumRows)
    if !bounded then
      Json.obj("calculation_version" -> Version.asJson, "disposition" -> "input_bound_exceeded".asJson)
    else
      val now = observations(current)
      val cutoff = now.headOption.map(_.at)
      val prior = observations(history).filter(row => cutoff.exists(row.at.isBefore))
      val domains = now.flatMap(_.domain).distinct.sorted
      val allCurrent = observations(domainCurrent).groupBy(_.domain)
      val allPrior = observations(domainHistory).filter(row => cutoff.exists(row.at.isBefore)).groupBy(_.domain)
      val churn = domains.map { domain =>
        val currentRows = allCurrent.getOrElse(Some(domain), Nil)
        val priorRows = allPrior.getOrElse(Some(domain), Nil)
        val hosts = currentRows.map(_.host).distinct.sorted
        val previous = priorRows.map(_.host).distinct.sorted
        val subdomains = hosts.filter(_ != domain)
        val priorSubdomains = previous.filter(_ != domain)
        val priorSet = priorSubdomains.toSet
        val added = subdomains.filterNot(priorSet)
        val available = domainCurrent.nonEmpty && currentRows.nonEmpty
        Json.obj(
          "registrable_domain" -> domain.asJson,
          "disposition" -> (if !available then "domain_scope_unavailable"
            else if priorRows.isEmpty then "insufficient_retained_history"
            else if !collectionComplete then "collection_censored"
            else "retained_observations_only").asJson,
          "current_observation_count" -> Option.when(available)(currentRows.size).asJson,
          "prior_observation_count" -> Option.when(available)(priorRows.size).asJson,
          "current_distinct_hosts" -> Option.when(available)(hosts.take(MaximumHosts)).asJson,
          "current_distinct_host_count" -> Option.when(available)(hosts.size).asJson,
          "current_distinct_hosts_omitted" -> Option.when(available)((hosts.size - MaximumHosts).max(0)).asJson,
          "prior_distinct_hosts" -> Option.when(available)(previous.take(MaximumHosts)).asJson,
          "prior_distinct_host_count" -> Option.when(available)(previous.size).asJson,
          "prior_distinct_hosts_omitted" -> Option.when(available)((previous.size - MaximumHosts).max(0)).asJson,
          "current_subdomain_count" -> Option.when(available)(subdomains.size).asJson,
          "prior_subdomain_count" -> Option.when(available)(priorSubdomains.size).asJson,
          "new_to_retained_history_subdomains" -> Option.when(available && priorRows.nonEmpty)(added.take(MaximumHosts)).asJson,
          "new_to_retained_history_subdomain_count" -> Option.when(available && priorRows.nonEmpty)(added.size).asJson,
          "new_to_retained_history_subdomains_omitted" -> Option.when(available && priorRows.nonEmpty)((added.size - MaximumHosts).max(0)).asJson,
          "new_to_scope_ever" -> Json.Null,
          "source_event_ids" -> currentRows.take(MaximumReferences).map(_.id).asJson,
          "source_event_ids_omitted" -> (currentRows.size - MaximumReferences).max(0).asJson,
          "baseline_event_ids" -> priorRows.take(MaximumReferences).map(_.id).asJson,
          "baseline_event_ids_omitted" -> (priorRows.size - MaximumReferences).max(0).asJson,
          "baseline_start" -> priorRows.headOption.map(_.at.toString).asJson,
          "baseline_end" -> priorRows.lastOption.map(_.at.toString).asJson
        )
      }
      Json.obj(
        "calculation_version" -> Version.asJson,
        "normalization_version" -> ProxyAuthority.Version.asJson,
        "disposition" -> (if now.isEmpty then "dns_input_unavailable" else "descriptive_features_only").asJson,
        "representation" -> "normalized_ascii_idna_alabels".asJson,
        "candidate" -> Json.Null,
        "interpretation" -> "lexical_features_do_not_establish_anomaly_or_threat".asJson,
        "calibration_status" -> "no_reviewed_detector_threshold".asJson,
        "false_positive_rate" -> Json.Null,
        "false_positive_disposition" -> "unavailable_without_detector_and_reviewed_labels".asJson,
        "protected_domain_similarity" -> Json.Null,
        "protected_domain_similarity_disposition" -> "protected_domain_list_not_supplied".asJson,
        "collection_complete" -> collectionComplete.asJson,
        "left_censored" -> true.asJson,
        "observation_count" -> current.map(row => text(row, "event_id")).distinct.size.asJson,
        "eligible_dns_observation_count" -> now.size.asJson,
        "baseline_dns_observation_count" -> prior.size.asJson,
        "source_event_ids" -> now.take(MaximumReferences).map(_.id).asJson,
        "source_event_ids_omitted" -> (now.size - MaximumReferences).max(0).asJson,
        "baseline_event_ids" -> prior.take(MaximumReferences).map(_.id).asJson,
        "baseline_event_ids_omitted" -> (prior.size - MaximumReferences).max(0).asJson,
        "distinct_host_count" -> now.map(_.host).distinct.size.asJson,
        "hosts" -> now.map(_.host).distinct.sorted.take(MaximumHosts).map(features).asJson,
        "hosts_omitted" -> (now.map(_.host).distinct.size - MaximumHosts).max(0).asJson,
        "subdomain_churn" -> churn.asJson
      ).deepMerge(detector(now, churn, collectionComplete, policy,
        (now.map(_.at) ++ allCurrent.values.toList.flatten.map(_.at)).minOption))
