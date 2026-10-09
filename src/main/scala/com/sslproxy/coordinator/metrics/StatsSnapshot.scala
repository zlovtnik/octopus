package com.sslproxy.coordinator.metrics

import io.circe.{Decoder, Encoder, Json}
import io.circe.syntax.*

import java.time.Instant
import java.time.format.DateTimeFormatter

final case class ThroughputPoint(bucketStart: String, records: Long)

final case class ThroughputSeries(bucket: String, series: List[ThroughputPoint])

final case class LifetimeTotals(recordsTotal: Long, daysCounted: Long, computedAt: String)

final case class LiveStripSnapshot(
  ingestProcessedRatePerSec: Double,
  pendingLedgerCount: Long,
  lastIngestSuccessAt: Option[String],
  backpressureActive: Boolean
)

final case class PeaksSnapshot(
  peaksComputedAt: Option[String],
  peakRecordsDay: Option[Long],
  peakRecordsDayDate: Option[String],
  peakRecordsWeek: Option[Long],
  peakRecordsWeekStart: Option[String],
  peakRecordsWeekEnd: Option[String]
)

final case class StatsSnapshot(
  asOf: String,
  peaks: Option[PeaksSnapshot],
  liveStrip: Option[LiveStripSnapshot],
  lifetimeTotals: Option[LifetimeTotals],
  throughput24h: Option[ThroughputSeries],
  throughput7d: Option[ThroughputSeries]
)

object StatsSnapshot:
  private val isoFormatter = DateTimeFormatter.ISO_INSTANT

  def toIso(instant: Instant): String = isoFormatter.format(instant)

  def empty(at: Instant): StatsSnapshot =
    StatsSnapshot(
      asOf = toIso(at),
      peaks = None,
      liveStrip = None,
      lifetimeTotals = None,
      throughput24h = None,
      throughput7d = None
    )

  given Encoder[ThroughputPoint] = Encoder.forProduct2("bucketStart", "records")(p =>
    (p.bucketStart, p.records)
  )
  given Decoder[ThroughputPoint] = Decoder.forProduct2("bucketStart", "records")(ThroughputPoint.apply)

  given Encoder[ThroughputSeries] = Encoder.forProduct2("bucket", "series")(s => (s.bucket, s.series))
  given Decoder[ThroughputSeries] = Decoder.forProduct2("bucket", "series")(ThroughputSeries.apply)

  given Encoder[LifetimeTotals] = Encoder.forProduct3("recordsTotal", "daysCounted", "computedAt")(t =>
    (t.recordsTotal, t.daysCounted, t.computedAt)
  )
  given Decoder[LifetimeTotals] = Decoder.forProduct3("recordsTotal", "daysCounted", "computedAt")(
    LifetimeTotals.apply
  )

  given Encoder[LiveStripSnapshot] = Encoder.forProduct4(
    "ingestProcessedRatePerSec",
    "pendingLedgerCount",
    "lastIngestSuccessAt",
    "backpressureActive"
  )(l =>
    (l.ingestProcessedRatePerSec, l.pendingLedgerCount, l.lastIngestSuccessAt, l.backpressureActive)
  )
  given Decoder[LiveStripSnapshot] = Decoder.forProduct4(
    "ingestProcessedRatePerSec",
    "pendingLedgerCount",
    "lastIngestSuccessAt",
    "backpressureActive"
  )(LiveStripSnapshot.apply)

  /** Public JSON always includes peak keys; missing stays null (never zero). */
  def toJson(snapshot: StatsSnapshot): Json =
    val peaks = snapshot.peaks
    val live = snapshot.liveStrip.map { ls =>
      Json.obj(
        "ingestProcessedRatePerSec" -> Json.fromDoubleOrNull(ls.ingestProcessedRatePerSec),
        "pendingLedgerCount" -> Json.fromLong(ls.pendingLedgerCount),
        "lastIngestSuccessAt" -> ls.lastIngestSuccessAt.fold(Json.Null)(Json.fromString),
        "backpressureActive" -> Json.fromBoolean(ls.backpressureActive)
      )
    }.getOrElse(Json.Null)
    Json.obj(
      "asOf" -> Json.fromString(snapshot.asOf),
      "peaksComputedAt" -> peaks.flatMap(_.peaksComputedAt).fold(Json.Null)(Json.fromString),
      "peakRecordsDay" -> peaks.flatMap(_.peakRecordsDay).fold(Json.Null)(Json.fromLong),
      "peakRecordsDayDate" -> peaks.flatMap(_.peakRecordsDayDate).fold(Json.Null)(Json.fromString),
      "peakRecordsWeek" -> peaks.flatMap(_.peakRecordsWeek).fold(Json.Null)(Json.fromLong),
      "peakRecordsWeekStart" -> peaks.flatMap(_.peakRecordsWeekStart).fold(Json.Null)(Json.fromString),
      "peakRecordsWeekEnd" -> peaks.flatMap(_.peakRecordsWeekEnd).fold(Json.Null)(Json.fromString),
      "liveStrip" -> live,
      "lifetimeTotals" -> snapshot.lifetimeTotals.fold(Json.Null)(_.asJson),
      "throughput24h" -> snapshot.throughput24h.fold(Json.Null)(_.asJson),
      "throughput7d" -> snapshot.throughput7d.fold(Json.Null)(_.asJson)
    )

  def parse(json: Json): Either[String, StatsSnapshot] =
    val c = json.hcursor
    for
      asOf <- c.get[String]("asOf").left.map(_.getMessage)
      peaksComputedAt <- c.get[Option[String]]("peaksComputedAt").left.map(_.getMessage)
      peakRecordsDay <- c.get[Option[Long]]("peakRecordsDay").left.map(_.getMessage)
      peakRecordsDayDate <- c.get[Option[String]]("peakRecordsDayDate").left.map(_.getMessage)
      peakRecordsWeek <- c.get[Option[Long]]("peakRecordsWeek").left.map(_.getMessage)
      peakRecordsWeekStart <- c.get[Option[String]]("peakRecordsWeekStart").left.map(_.getMessage)
      peakRecordsWeekEnd <- c.get[Option[String]]("peakRecordsWeekEnd").left.map(_.getMessage)
      liveStrip <- c.get[Option[LiveStripSnapshot]]("liveStrip").left.map(_.getMessage)
      lifetimeTotals <- c.get[Option[LifetimeTotals]]("lifetimeTotals").left.map(_.getMessage)
      throughput24h <- c.get[Option[ThroughputSeries]]("throughput24h").left.map(_.getMessage)
      throughput7d <- c.get[Option[ThroughputSeries]]("throughput7d").left.map(_.getMessage)
    yield
      val hasPeaks =
        peaksComputedAt.nonEmpty || peakRecordsDay.nonEmpty || peakRecordsWeek.nonEmpty
      StatsSnapshot(
        asOf = asOf,
        peaks =
          if hasPeaks then
            Some(
              PeaksSnapshot(
                peaksComputedAt = peaksComputedAt,
                peakRecordsDay = peakRecordsDay,
                peakRecordsDayDate = peakRecordsDayDate,
                peakRecordsWeek = peakRecordsWeek,
                peakRecordsWeekStart = peakRecordsWeekStart,
                peakRecordsWeekEnd = peakRecordsWeekEnd
              )
            )
          else None,
        liveStrip = liveStrip,
        lifetimeTotals = lifetimeTotals,
        throughput24h = throughput24h,
        throughput7d = throughput7d
      )
