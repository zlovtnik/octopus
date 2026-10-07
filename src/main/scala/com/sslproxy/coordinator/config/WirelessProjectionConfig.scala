package com.sslproxy.coordinator.config

import io.circe.{Decoder, parser}
import io.circe.generic.semiauto.deriveDecoder
import pureconfig.ConfigReader
import java.time.Instant

final case class SensorCalibration(
  sensorId: String,
  locationId: String,
  version: String,
  referenceRssi: Double,
  referenceMeters: Double,
  pathLossExponent: Double,
  uncertaintyDb: Double,
  frequenciesMhz: List[Int],
  channels: List[Int],
  minimumSamples: Int,
  maximumStddevDb: Double,
  validFrom: Instant,
  validUntil: Instant,
  accessPoints: List[String] = Nil
)

object SensorCalibration:
  given Decoder[SensorCalibration] = deriveDecoder

final case class WirelessProjectionConfig(
  mode: String = "legacy",
  rangesEnabled: Boolean = false,
  calibrationsJson: String = "[]"
) derives ConfigReader:
  def enabled: Boolean = mode != "legacy"
  def projectionOnly: Boolean = mode == "projected"
  def calibrations: Either[String, List[SensorCalibration]] =
    parser.decode[List[SensorCalibration]](calibrationsJson).left.map(_ => "invalid sensor calibration JSON").flatMap { values =>
      val valid = values.forall { c =>
        c.sensorId.nonEmpty && c.sensorId.length <= 64 && c.locationId.nonEmpty && c.locationId.length <= 128 &&
        c.version.nonEmpty && c.version.length <= 64 &&
        List(c.referenceRssi, c.referenceMeters, c.pathLossExponent, c.uncertaintyDb, c.maximumStddevDb).forall(_.isFinite) &&
        c.referenceRssi >= -120 && c.referenceRssi < 0 && c.referenceMeters > 0 && c.referenceMeters <= 100 &&
        c.pathLossExponent >= 1 && c.pathLossExponent <= 6 && c.uncertaintyDb > 0 && c.uncertaintyDb <= 30 &&
        c.minimumSamples >= 2 && c.maximumStddevDb > 0 && c.maximumStddevDb <= 20 &&
        c.frequenciesMhz.nonEmpty && c.frequenciesMhz.forall(f => f >= 2300 && f <= 7200) &&
        c.channels.nonEmpty && c.channels.forall(ch => ch > 0 && ch <= 233) && c.validFrom.isBefore(c.validUntil) &&
        c.accessPoints.forall(_.matches("^[0-9a-f]{2}(:[0-9a-f]{2}){5}$"))
      }
      // A sensor can have historical calibration versions, but no exact
      // version/date record may be declared twice.
      if !valid || values.map(c => (c.sensorId, c.version, c.validFrom)).distinct.size != values.size then Left("invalid or duplicate sensor calibration")
      else Right(values)
    }
  def errors: List[String] =
    List(
      Option.when(!Set("legacy", "shadow", "projected").contains(mode))("wireless projection mode must be legacy, shadow or projected"),
      calibrations.left.toOption,
      Option.when(rangesEnabled && calibrations.toOption.forall(_.isEmpty))("ranges require reviewed sensor calibration")
    ).flatten
