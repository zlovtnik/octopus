package com.sslproxy.coordinator.metrics

import java.time.Instant

final case class DayPeak(records: Long, date: String)
final case class WeekPeak(records: Long, start: String, end: String)

/** Reads authoritative ingestion evidence. Errors remain in F so callers can
  * distinguish an unavailable measurement from a successful, measured zero.
  */
trait MetricsRepository[F[_]]:
  def peakDay: F[Option[DayPeak]]
  def peakWeek: F[Option[WeekPeak]]
  def lifetimeTotals(computedAt: Instant): F[LifetimeTotals]
  def hourlyBuckets(from: Instant, until: Instant): F[List[ThroughputPoint]]
