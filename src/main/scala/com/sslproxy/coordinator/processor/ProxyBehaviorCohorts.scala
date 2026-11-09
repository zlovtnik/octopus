package com.sslproxy.coordinator.processor

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import scala.util.Try

/** Direct, scoped destination-set similarities. A matching registration epoch
  * establishes neither a physical device relationship nor transitive identity.
  */
object ProxyBehaviorCohorts:
  val Version = "proxy-behavior-cohorts-v1"
  private val MaximumWindows = 168
  private val MaximumReferences = 32
  private val Fields = Set("version", "calibration_reference", "b11_acceptance_reference",
    "minimum_destinations", "minimum_observations", "minimum_similarity", "max_pairs")

  private final case class Policy(version: String, calibration: String, acceptance: String,
      destinations: Int, observations: Int, similarity: BigDecimal, pairs: Int)
  private final case class Compatibility(deployment: String, start: Instant, end: Instant,
      normalization: String, calculation: String, policy: String):
    def json: Json = Json.arr(deployment.asJson, start.toString.asJson, end.toString.asJson,
      normalization.asJson, calculation.asJson, policy.asJson)
  private final case class Window(projection: ProxyBehavior.Projection, compatible: Compatibility,
      scope: String, endpoint: String, observations: Int, complete: Boolean)
  private final case class Group(compatible: Compatibility, scope: String, windows: List[Window]):
    def destinations: Set[String] = windows.filter(_.observations > 0).map(_.endpoint).toSet
    def observations: BigInt = windows.map(w => BigInt(w.observations)).sum
    def complete: Boolean = windows.forall(_.complete)

  private def text(row: Json, key: String): Option[String] = row.hcursor.get[String](key).toOption
  private def bounded(value: String, maximum: Int): Boolean =
    value.trim.nonEmpty && value.length <= maximum && value.forall(c => c >= ' ' && c <= '~')
  private def time(row: Json, key: String): Option[Instant] =
    text(row, key).flatMap(value => Try(Instant.parse(value)).toOption)
  private def config(value: Json): Option[Policy] =
    for
      obj <- value.asObject.filter(_.keys.toSet == Fields)
      version <- obj("version").flatMap(_.asString).filter(bounded(_, 64))
      calibration <- obj("calibration_reference").flatMap(_.asString).filter(bounded(_, 256))
      acceptance <- obj("b11_acceptance_reference").flatMap(_.asString).filter(bounded(_, 256))
      destinations <- value.hcursor.get[Int]("minimum_destinations").toOption.filter(n => n >= 1 && n <= 128)
      observations <- value.hcursor.get[Int]("minimum_observations").toOption.filter(n => n >= 1 && n <= 10000)
      similarity <- value.hcursor.get[BigDecimal]("minimum_similarity").toOption.filter(n => n > 0 && n <= 1)
      pairs <- value.hcursor.get[Int]("max_pairs").toOption.filter(n => n >= 1 && n <= 256)
    yield Policy(version, calibration, acceptance, destinations, observations, similarity, pairs)

  private def window(p: ProxyBehavior.Projection): Option[Window] =
    val e = p.evidence
    for
      deployment <- text(e, "deployment_scope").filter(bounded(_, 128))
      scope <- text(e, "scope_key").filter(_.matches("[0-9a-f]{64}"))
      endpoint <- text(e, "endpoint_key").filter(bounded(_, 1024))
      start <- time(e, "window_start")
      end <- time(e, "window_end").filter(start.isBefore)
      normalization <- text(e, "normalization_version").filter(bounded(_, 64))
      calculation <- text(e, "calculation_version").filter(bounded(_, 64))
      policy <- text(e, "policy_version").filter(bounded(_, 64))
      count <- e.hcursor.get[Int]("observed_count").toOption.filter(n => n >= 0 && n <= 10000)
      _ <- Option.when(bounded(p.key, 4096) && p.fingerprint.matches("[0-9a-f]{64}") && p.sourceWatermark >= 0)(())
    yield
      val complete = p.disposition == "available" && text(e, "disposition").contains("available") &&
        text(e, "ownership").contains("registration_claim_only") &&
        time(e, "valid_from").exists(at => !at.isAfter(start)) &&
        time(e, "valid_until").exists(at => !at.isBefore(end)) &&
        time(e, "coverage_start").exists(at => !at.isAfter(start)) &&
        time(e, "coverage_end").exists(at => !at.isBefore(end))
      Window(p, Compatibility(deployment, start, end, normalization, calculation, policy), scope, endpoint, count, complete)

  private def references(group: Group): Json =
    val ordered = group.windows.sortBy(_.projection.key)
    Json.obj(
      "scope_key" -> group.scope.asJson,
      "destination_count" -> group.destinations.size.asJson,
      "observation_count" -> group.observations.asJson,
      "source_projection_count" -> ordered.size.asJson,
      "source_projections" -> ordered.take(MaximumReferences).map { row =>
        Json.obj("key" -> row.projection.key.asJson,
          "input_fingerprint" -> row.projection.fingerprint.asJson,
          "source_watermark" -> row.projection.sourceWatermark.asJson)
      }.asJson,
      "omitted_projection_reference_count" -> (ordered.size - MaximumReferences).max(0).asJson
    )

  private def base(disposition: String, policy: Option[Policy]): Json = Json.obj(
    "calculation_version" -> Version.asJson,
    "disposition" -> disposition.asJson,
    "method" -> "exact_destination_set_jaccard".asJson,
    "sample_unit" -> "distinct_observed_endpoints_per_registration_epoch_and_window".asJson,
    "interpretation" -> "direct_behavioral_similarity_only_no_transitive_identity".asJson,
    "ownership" -> "unavailable".asJson,
    "embedding_model" -> Json.Null,
    "embedding_model_disposition" -> "not_used_exact_structured_evidence".asJson,
    "policy_version" -> policy.map(_.version).asJson,
    "calibration_reference" -> policy.map(_.calibration).asJson,
    "b11_acceptance_reference" -> policy.map(_.acceptance).asJson,
    "minimum_destinations" -> policy.map(_.destinations).asJson,
    "minimum_observations" -> policy.map(_.observations).asJson,
    "maximum_pairs" -> policy.map(_.pairs).asJson,
    "matches" -> List.empty[Json].asJson,
    "match_count" -> 0.asJson
  )

  /** All registration windows must come from one bounded, fenced snapshot.
    * Corrections, expiry and source removal recompute the complete direct-pair
    * set; output never merges previous memberships into a new generation.
    */
  def evidence(windows: List[ProxyBehavior.Projection], policy: Json): Map[String, Json] =
    val settings = config(policy)
    def unavailable(reason: String): Map[String, Json] =
      windows.take(MaximumWindows).map(p => p.key -> base(reason, settings)).toMap
    if windows.size > MaximumWindows then unavailable("window_bound_exceeded")
    else if policy.asObject.exists(_.isEmpty) then unavailable("disabled_no_reviewed_policy")
    else if settings.isEmpty then unavailable("invalid_policy")
    else
      val unique = windows.groupBy(_.key).toList.sortBy(_._1)
      val registration = unique.map(_._2.head).filter(p => text(p.evidence, "scope_kind").contains("registration"))
      val parsed = registration.flatMap(window)
      val duplicate = unique.exists(_._2.distinct.size != 1)
      val duplicateEndpoint = parsed.groupBy(w => (w.compatible, w.scope, w.endpoint)).exists(_._2.size > 1)
      val mixedVersions = parsed.groupBy(w => (w.compatible.deployment, w.scope,
        w.compatible.start, w.compatible.end)).exists(_._2.map(_.compatible).distinct.size > 1)
      if duplicate || duplicateEndpoint then unavailable("conflicting_projection_versions")
      else if mixedVersions then unavailable("incompatible_scope_preparation_versions")
      else if parsed.size != registration.size then unavailable("invalid_registration_projection")
      else
        val configured = settings.get
        val groups = parsed.groupBy(w => (w.compatible, w.scope)).toList.map {
          case ((compatible, scope), rows) => Group(compatible, scope, rows.sortBy(_.projection.key))
        }.sortBy(g => (g.compatible.json.noSpaces, g.scope))
        val eligible = groups.filter(g => g.complete && g.destinations.size >= configured.destinations &&
          g.observations >= configured.observations && g.observations <= 10000)
        val pairs = eligible.indices.iterator.flatMap { index =>
          val left = eligible(index)
          eligible.iterator.drop(index + 1).filter(right => right.compatible == left.compatible && right.scope != left.scope).flatMap { right =>
            val shared = left.destinations.intersect(right.destinations).size
            val union = left.destinations.union(right.destinations).size
            val similarity = BigDecimal(shared) / BigDecimal(union)
            Option.when(similarity >= configured.similarity) {
              val id = Sha256Utils.sha256Hex(Json.arr(Version.asJson, configured.version.asJson,
                left.compatible.json, left.scope.asJson, right.scope.asJson).noSpaces)
              val pair = Json.obj("pair_id" -> id.asJson,
                "left" -> references(left), "right" -> references(right),
                "shared_destination_count" -> shared.asJson, "union_destination_count" -> union.asJson,
                "jaccard_similarity" -> similarity.asJson,
                "window_start" -> left.compatible.start.toString.asJson,
                "window_end" -> left.compatible.end.toString.asJson,
                "normalization_version" -> left.compatible.normalization.asJson,
                "source_calculation_version" -> left.compatible.calculation.asJson,
                "source_policy_version" -> left.compatible.policy.asJson)
              (left, right, pair)
            }
          }
        }.take(configured.pairs + 1).toList
        if pairs.size > configured.pairs then unavailable("pair_bound_exceeded")
        else
          val initial = unavailable("ineligible_or_insufficient_samples")
          groups.foldLeft(initial) { (result, group) =>
            val supported = eligible.contains(group)
            val matches = pairs.collect { case (left, right, pair) if left == group || right == group => pair }
            val disposition = if !supported then "ineligible_or_insufficient_samples"
              else if matches.isEmpty then "no_matching_cohorts" else "direct_matches_available"
            val output = base(disposition, settings).deepMerge(Json.obj(
              "matches" -> matches.asJson, "match_count" -> matches.size.asJson,
              "scope_destination_count" -> group.destinations.size.asJson,
              "scope_observation_count" -> group.observations.asJson,
              "minimum_similarity" -> configured.similarity.asJson))
            group.windows.foldLeft(result)((acc, row) => acc.updated(row.projection.key, output))
          }
