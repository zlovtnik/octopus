package com.sslproxy.coordinator.processor

import cats.syntax.all.*
import com.sslproxy.coordinator.config.SensorCalibration
import io.circe.{Json, parser}
import java.time.Instant

final case class WirelessObservation(
  sensorId: String, observedAt: Instant, sourceMac: Option[String], bssid: Option[String],
  transmitterMac: Option[String], ssid: Option[String], signalDbm: Option[Int],
  frequencyMhz: Option[Int], channel: Option[Int], frameType: String, protocol: String,
  retry: Boolean, protectedFrame: Boolean, handshake: Boolean
)

object WirelessObservation:
  private val Mac = "^[0-9a-f]{2}(:[0-9a-f]{2}){5}$".r
  def decode(raw: String): Either[Throwable, WirelessObservation] =
    for
      json <- parser.parse(raw).leftMap(identity[Throwable])
      sensor <- json.hcursor.get[String]("sensor_id").leftMap(identity[Throwable])
      time <- json.hcursor.get[String]("observed_at").leftMap(identity[Throwable])
      observed <- Either.catchNonFatal(Instant.parse(time))
      _ <- Either.cond(sensor.nonEmpty && sensor.length <= 64, (), IllegalArgumentException("invalid sensor_id"))
      _ <- Either.cond(json.hcursor.get[Int]("schema_version").toOption.forall(v => v >= 1 && v <= 2), (),
        IllegalArgumentException("unsupported wireless schema version"))
    yield
      def field(name: String, parent: String): Option[Json] =
        json.hcursor.downField(name).focus.orElse(json.hcursor.downField(parent).downField(name).focus)
      def string(name: String, parent: String = ""): Option[String] = field(name, parent).flatMap(_.asString)
      def integer(name: String, parent: String): Option[Int] = field(name, parent).flatMap(_.asNumber.flatMap(_.toInt))
      def mac(name: String): Option[String] = string(name, "mac").map(_.toLowerCase(java.util.Locale.ROOT))
        .filter(m => Mac.matches(m) && (Integer.parseInt(m.take(2), 16) & 1) == 0 && m != "00:00:00:00:00:00")
      def bool(name: String, parent: String = "mac"): Boolean = field(name, parent).flatMap(_.asBoolean).getOrElse(false)
      WirelessObservation(sensor, observed, mac("source_mac"), mac("bssid"), mac("transmitter_mac"),
        string("ssid").filter(_.length <= 256), integer("signal_dbm", "rf").filter(v => v >= -127 && v < 0),
        integer("frequency_mhz", "rf"), integer("channel_number", "rf").orElse(integer("channel", "")),
        string("frame_type", "mac").filter(_.length <= 32).getOrElse("unknown"),
        string("app_protocol").orElse(string("protocol", "application")).filter(_.length <= 64).getOrElse("unknown"),
        bool("retry"), bool("protected"), bool("handshake_captured", ""))

final case class RadioSamples(count: Long = 0, sum: Double = 0, squares: Double = 0):
  def add(rssi: Int): RadioSamples = RadioSamples(count + 1, sum + rssi, squares + rssi.toDouble * rssi)
  def mean: Double = if count == 0 then 0 else sum / count
  def stddev: Double = if count < 2 then 0 else math.sqrt(((squares - sum * sum / count) / (count - 1)).max(0))

object CalibratedRange:
  def estimate(samples: RadioSamples, calibration: SensorCalibration, frequency: Int, channel: Int,
    firstSeen: Instant, lastSeen: Instant, now: Instant): Option[Json] =
    Option.when(samples.count >= calibration.minimumSamples && samples.stddev <= calibration.maximumStddevDb &&
      calibration.frequenciesMhz.contains(frequency) && calibration.channels.contains(channel) &&
      !firstSeen.isBefore(calibration.validFrom) && lastSeen.isBefore(calibration.validUntil) &&
      !now.isBefore(calibration.validFrom) && now.isBefore(calibration.validUntil) &&
      !lastSeen.isBefore(now.minusSeconds(300)) && !lastSeen.isAfter(now)) {
      val meters = calibration.referenceMeters * math.pow(10, (calibration.referenceRssi - samples.mean) / (10 * calibration.pathLossExponent))
      val errorDb = calibration.uncertaintyDb + 1.96 * samples.stddev / math.sqrt(samples.count.toDouble)
      val factor = math.pow(10, errorDb / (10 * calibration.pathLossExponent))
      Json.obj("meters" -> Json.fromDoubleOrNull(meters), "error_meters" -> Json.fromDoubleOrNull(meters * (factor - 1)),
        "lower_meters" -> Json.fromDoubleOrNull(meters / factor), "upper_meters" -> Json.fromDoubleOrNull(meters * factor),
        "confidence" -> Json.fromString("model_estimate"), "calibration_version" -> Json.fromString(calibration.version),
        "sample_count" -> Json.fromLong(samples.count), "observed_at" -> Json.fromString(lastSeen.toString),
        "valid_until" -> Json.fromString((if calibration.validUntil.isBefore(lastSeen.plusSeconds(300)) then
          calibration.validUntil else lastSeen.plusSeconds(300)).toString))
    }
