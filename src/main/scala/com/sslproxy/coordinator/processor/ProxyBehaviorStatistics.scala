package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.{Duration, Instant, OffsetDateTime}
import scala.util.Try

/** Descriptive candidates, never threat labels. Thresholds are provisional
  * until evaluated against reviewed deployment-specific controls (the G8 gate).
  */
object ProxyBehaviorStatistics:
  val Version = "proxy-behavior-statistics-v1"
  val MinimumSamples = 6
  val ActiveBins = 12
  private val MaximumRelativeMad = BigDecimal("0.1")
  private val DeviationThreshold = BigDecimal(6)

  private def text(row: Json, key: String): Option[String] =
    row.hcursor.get[String](key).toOption
  private def instant(value: String): Option[Instant] = Try(
    OffsetDateTime.parse(value).toInstant
  ).toOption
  private def time(row: Json, key: String): Option[Instant] =
    text(row, key).flatMap(instant)
  private def nonnegative(row: Json, key: String): Option[Long] =
    row.hcursor.get[Long](key).toOption.filter(_ >= 0)
  private def median(values: List[BigDecimal]): Option[BigDecimal] =
    val sorted = values.sorted
    Option.when(sorted.nonEmpty) {
      val middle = sorted.size / 2
      if sorted.size % 2 == 0 then (sorted(middle - 1) + sorted(middle)) / 2
      else sorted(middle)
    }

  /** Observation and flow-start series are evaluated separately: mixing their
    * collection units can manufacture regular inter-arrivals.
    */
  def polling(
      rows: List[Json],
      flows: List[ProxyBehavior.Projection],
      windowStart: Instant,
      windowEnd: Instant,
      collectionComplete: Boolean,
      flowStartCollectionComplete: Boolean
  ): Json =
    val observations = rows.filter(row =>
      text(row, "type").exists(kind =>
        ProxyEvidence.AllowedEvents(
          kind
        ) && kind != "tunnel_open" && kind != "tunnel_close"
      )
    )
    val observationTimes = observations.flatMap(row => time(row, "time"))
    val pollingFlows = uniqueFlows(flows).filter(flow =>
      flow.disposition == "validated" &&
        time(flow.evidence, "opened_at").exists(at =>
          !at.isBefore(windowStart) && at.isBefore(windowEnd)
        )
    )
    val flowTimes = pollingFlows.flatMap(flow => time(flow.evidence, "opened_at"))
    val coalescingUnknown = observations.count(row =>
      !row.hcursor
        .downField("coalescing")
        .get[String]("status")
        .contains("not_applied")
    )
    val suppressed = observations
      .flatMap(row =>
        row.hcursor
          .downField("coalescing")
          .get[Long]("suppressed_previous_window")
          .toOption
          .filter(_ >= 0)
      )
      .map(BigInt(_))
      .sum
    val observationSeries = pollingSeries(
      observationTimes,
      windowStart,
      windowEnd,
      collectionComplete,
      coalescingUnknown > 0 || suppressed > 0
    )
    val flowSeries = pollingSeries(
      flowTimes,
      windowStart,
      windowEnd,
      flowStartCollectionComplete,
      false
    )
    val candidate = List(observationSeries, flowSeries).exists(
      _.hcursor.get[Boolean]("candidate").contains(true)
    )
    Json.obj(
      "calculation_version" -> Version.asJson,
      "calibration_status" -> "provisional_unvalidated".asJson,
      "candidate" -> candidate.asJson,
      "interpretation" -> "periodicity_candidate_only".asJson,
      "window_start" -> windowStart.toString.asJson,
      "window_end" -> windowEnd.toString.asJson,
      "collection_censored" -> (!collectionComplete).asJson,
      "coalescing_or_unknown_observations" -> coalescingUnknown.asJson,
      "suppressed_previous_window" -> suppressed.asJson,
      "suppression_interval" -> "previous_collection_window_not_current_attempts".asJson,
      "observations" -> observationSeries.deepMerge(
        Json.obj("sample_unit" -> "unique_observation_times".asJson)
      ),
      "validated_flow_starts" -> flowSeries.deepMerge(
        Json.obj("sample_unit" -> "unique_validated_flow_start_times".asJson)
      ),
      "source_event_ids" -> observations
        .flatMap(row => text(row, "event_id"))
        .distinct
        .sorted
        .asJson,
      "flow_keys" -> pollingFlows
        .map(_.key)
        .sorted
        .asJson
    )

  private def pollingSeries(
      times: List[Instant],
      start: Instant,
      end: Instant,
      complete: Boolean,
      coalesced: Boolean
  ): Json =
    val ordered = times
      .filter(at => !at.isBefore(start) && at.isBefore(end))
      .distinct
      .sorted
    val intervals = ordered
      .zip(ordered.drop(1))
      .map((a, b) => BigDecimal(Duration.between(a, b).toMillis))
    val midpoint = median(intervals)
    val mad = midpoint.flatMap(value =>
      median(intervals.map(interval => (interval - value).abs))
    )
    val relativeMad =
      midpoint.filter(_ > 0).flatMap(value => mad.map(_ / value))
    val spanMillis =
      Try(Duration.between(start, end).toMillis).toOption.filter(_ > 0)
    val bins = spanMillis
      .map { span =>
        val indices = ordered.map(at =>
          ((BigInt(
            Duration.between(start, at).toMillis
          ) * ActiveBins) / span).toInt
        )
        (0 until ActiveBins).map(bin => indices.count(_ == bin)).toList
      }
      .getOrElse(List.fill(ActiveBins)(0))
    val active = bins.count(_ > 0)
    val enough = ordered.size >= MinimumSamples
    val periodic =
      enough && active >= 3 && relativeMad.exists(_ <= MaximumRelativeMad)
    val disposition =
      if spanMillis.isEmpty then "invalid_window"
      else if !enough then "insufficient_samples"
      else if !complete then "collection_censored"
      else if coalesced then "coalescing_censored"
      else if active < 3 then "insufficient_active_bins"
      else if periodic then "candidate"
      else "irregular"
    Json.obj(
      "sample_size" -> ordered.size.asJson,
      "minimum_samples" -> MinimumSamples.asJson,
      "unique_times" -> ordered.map(_.toString).asJson,
      "interval_count" -> intervals.size.asJson,
      "median_interarrival_ms" -> midpoint.asJson,
      "mad_interarrival_ms" -> mad.asJson,
      "relative_mad" -> relativeMad.asJson,
      "maximum_relative_mad" -> MaximumRelativeMad.asJson,
      "active_bin_count" -> active.asJson,
      "time_bin_counts" -> bins.asJson,
      "bin_count" -> ActiveBins.asJson,
      "collection_censored" -> (!complete).asJson,
      "periodic_pattern" -> periodic.asJson,
      "candidate" -> (disposition == "candidate").asJson,
      "disposition" -> disposition.asJson
    )

  private def uniqueFlows(
      flows: List[ProxyBehavior.Projection]
  ): List[ProxyBehavior.Projection] =
    // A conflicting projection cannot safely contribute confirmed measurements.
    flows.groupBy(_.key).toList.sortBy(_._1).flatMap { (_, copies) =>
      Option.when(copies.map(_.evidence).distinct.size == 1)(
        copies.maxBy(_.sourceWatermark)
      )
    }

  private def measured(
      flows: List[ProxyBehavior.Projection]
  ): List[ProxyBehavior.Projection] =
    uniqueFlows(flows).filter(flow =>
      flow.disposition == "validated" &&
        text(flow.evidence, "bytes_status").contains("measured_terminal") &&
        nonnegative(flow.evidence, "bytes_up").isDefined && nonnegative(
          flow.evidence,
          "bytes_down"
        ).isDefined
    )

  private def ratio(flow: ProxyBehavior.Projection): Option[BigDecimal] =
    for
      up <- nonnegative(flow.evidence, "bytes_up")
      down <- nonnegative(flow.evidence, "bytes_down").filter(_ > 0)
    yield BigDecimal(up) / BigDecimal(down)

  private def duration(flow: ProxyBehavior.Projection): Option[BigDecimal] =
    Option
      .when(
        text(flow.evidence, "duration_status").contains("connection_elapsed")
      )(nonnegative(flow.evidence, "duration_ms").map(BigDecimal(_)))
      .flatten

  private def profile(
      current: List[BigDecimal],
      historical: List[BigDecimal],
      minimumScale: BigDecimal = BigDecimal(1)
  ): Json =
    val center = median(historical)
    val mad = center.flatMap(value =>
      median(historical.map(sample => (sample - value).abs))
    )
    // A relative 10% floor avoids division by zero for a constant history.
    val scale = center
      .zip(mad)
      .map((value, deviation) =>
        (deviation * BigDecimal("1.4826"))
          .max(value.abs * BigDecimal("0.1"))
          .max(minimumScale)
      )
    val score = Option
      .when(historical.size >= MinimumSamples)(
        center
          .zip(scale)
          .flatMap((value, spread) =>
            current.maxOption
              .map(sample => ((sample - value) / spread).max(BigDecimal(0)))
          )
      )
      .flatten
    Json.obj(
      "historical_sample_size" -> historical.size.asJson,
      "minimum_samples" -> MinimumSamples.asJson,
      "current_sample_size" -> current.size.asJson,
      "current_median" -> median(current).asJson,
      "historical_median" -> center.asJson,
      "historical_mad" -> mad.asJson,
      "robust_scale" -> scale.asJson,
      "maximum_upward_robust_deviation" -> score.asJson,
      "candidate" -> score.exists(_ >= DeviationThreshold).asJson,
      "disposition" -> (if current.isEmpty then "unavailable"
                        else if historical.size < MinimumSamples then
                          "insufficient_history"
                        else "available") .asJson
    )

  /** Caller supplies a matching endpoint/scope and prior-window history. The
    * additional strict event-time check prevents accidental future leakage.
    */
  def transfer(
      flows: List[ProxyBehavior.Projection],
      historical: List[ProxyBehavior.Projection],
      novel: Option[Boolean]
  ): Json =
    val current = measured(flows)
    val cutoff = flows.map(_.observedAt).minOption
    val currentKeys = flows.map(_.key).toSet
    val prior = measured(historical).filter(flow =>
      cutoff.exists(flow.observedAt.isBefore) && !currentKeys(flow.key)
    )
    val upload = current
      .map(flow => BigInt(nonnegative(flow.evidence, "bytes_up").get))
      .sum
    val download = current
      .map(flow => BigInt(nonnegative(flow.evidence, "bytes_down").get))
      .sum
    val uploadProfile = profile(
      current.flatMap(flow =>
        nonnegative(flow.evidence, "bytes_up").map(BigDecimal(_))
      ),
      prior.flatMap(flow =>
        nonnegative(flow.evidence, "bytes_up").map(BigDecimal(_))
      )
    )
    val ratioProfile = profile(
      current.flatMap(ratio),
      prior.flatMap(ratio),
      BigDecimal("0.000001")
    )
    val durationProfile =
      profile(current.flatMap(duration), prior.flatMap(duration))
    val candidate = List(uploadProfile, ratioProfile, durationProfile).exists(
      _.hcursor.get[Boolean]("candidate").contains(true)
    )
    Json.obj(
      "calculation_version" -> Version.asJson,
      "calibration_status" -> "provisional_unvalidated".asJson,
      "interpretation" -> "transfer_anomaly_candidate_only".asJson,
      "candidate" -> candidate.asJson,
      "deviation_threshold" -> DeviationThreshold.asJson,
      "measured_terminal_flows" -> current.size.asJson,
      "excluded_flows" -> (flows
        .map(_.key)
        .distinct
        .size - current.size).asJson,
      "bytes_status" -> (if current.isEmpty then "unavailable"
                         else if current.size == flows.map(_.key).distinct.size
                         then "measured_terminal"
                         else "measured_terminal_subset") .asJson,
      "bytes_up" -> Option.when(current.nonEmpty)(upload).asJson,
      "bytes_down" -> Option.when(current.nonEmpty)(download).asJson,
      "upload_download_ratio" -> Option
        .when(current.nonEmpty && download > 0)(
          BigDecimal(upload) / BigDecimal(download)
        )
        .asJson,
      "ratio_disposition" -> (if current.isEmpty then "unavailable"
                              else if download == 0 then "zero_download"
                              else "available") .asJson,
      "zero_download_flows" -> current
        .count(flow => nonnegative(flow.evidence, "bytes_down").contains(0L))
        .asJson,
      "duration_scope" -> "connection_elapsed".asJson,
      "new_to_retained_scope" -> novel.asJson,
      "upload_profile" -> uploadProfile,
      "ratio_profile" -> ratioProfile,
      "duration_profile" -> durationProfile,
      "baseline_sample_size" -> prior.size.asJson,
      "baseline_flow_keys" -> prior.map(_.key).sorted.asJson,
      "baseline_start" -> prior
        .map(_.observedAt)
        .minOption
        .map(_.toString)
        .asJson,
      "baseline_end" -> prior
        .map(_.observedAt)
        .maxOption
        .map(_.toString)
        .asJson,
      "baseline_cutoff_exclusive" -> cutoff.map(_.toString).asJson,
      "baseline_disposition" -> (if prior.size < MinimumSamples then
                                   "insufficient_history"
                                 else "available").asJson,
      "flow_keys" -> current.map(_.key).sorted.asJson
    )
