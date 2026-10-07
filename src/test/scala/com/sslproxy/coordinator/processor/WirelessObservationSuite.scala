package com.sslproxy.coordinator.processor

import com.sslproxy.coordinator.config.{SensorCalibration, WirelessProjectionConfig}
import munit.FunSuite
import java.time.Instant

class WirelessObservationSuite extends FunSuite:
  private val now = Instant.parse("2026-10-07T16:00:00Z")
  private val calibration = SensorCalibration("sensor-1", "lab", "review-1", -40, 1, 2, 3,
    List(2412), List(1), 3, 4, now.minusSeconds(3600), now.plusSeconds(3600))
  private val samples = List(-59, -60, -61).foldLeft(RadioSamples())(_.add(_))

  test("stable calibrated transmitter samples produce a labeled estimate with uncertainty") {
    val result = CalibratedRange.estimate(samples, calibration, 2412, 1, now.minusSeconds(20), now, now)
    assertEquals(result.flatMap(_.hcursor.get[Double]("meters").toOption), Some(10d))
    assert(result.exists(_.hcursor.get[Double]("error_meters").toOption.exists(_ > 0)))
    assertEquals(result.flatMap(_.hcursor.get[String]("calibration_version").toOption), Some("review-1"))
  }

  test("expired, future, wrong-band, insufficient, stale and noisy radio samples cannot claim meters") {
    val variants = List(
      (samples, calibration.copy(validUntil = now), 2412, now),
      (samples, calibration.copy(validFrom = now.plusSeconds(1)), 2412, now),
      (samples, calibration, 5180, now),
      (RadioSamples().add(-60), calibration, 2412, now),
      (samples, calibration, 2412, now.minusSeconds(301)),
      (List(-30, -90, -50).foldLeft(RadioSamples())(_.add(_)), calibration, 2412, now)
    )
    variants.foreach { case (radio, c, freq, last) =>
      assertEquals(CalibratedRange.estimate(radio, c, freq, 1, last.minusSeconds(20), last, now), None)
    }
  }

  test("invalid calibration and uncalibrated range enablement fail closed") {
    assert(WirelessProjectionConfig(rangesEnabled = true).errors.nonEmpty)
    assert(WirelessProjectionConfig(mode = "typo").errors.nonEmpty)
    assert(WirelessProjectionConfig(calibrationsJson = "{}").errors.nonEmpty)
    assertEquals(WirelessProjectionConfig(mode = "shadow").errors, Nil)
  }

  test("wire decoder handles nested radio and omits multicast device identities") {
    val decoded = WirelessObservation.decode("""{"schema_version":2,"sensor_id":"s","observed_at":"2026-10-07T16:00:00Z",
      "mac":{"source_mac":"FF:FF:FF:FF:FF:FF","bssid":"02:00:00:00:00:01","transmitter_mac":"02:00:00:00:00:02"},
      "rf":{"signal_dbm":-60,"frequency_mhz":2412,"channel_number":1},"payload":"must not persist"}""").toOption.get
    assertEquals(decoded.sourceMac, None)
    assertEquals(decoded.signalDbm, Some(-60))
    assertEquals(decoded.bssid, Some("02:00:00:00:00:01"))
  }
