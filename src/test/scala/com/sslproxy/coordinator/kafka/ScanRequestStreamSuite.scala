package com.sslproxy.coordinator.kafka

import com.sslproxy.coordinator.postgres.PostgresPayloadReadException
import munit.FunSuite

import java.nio.file.{AccessDeniedException, NoSuchFileException}

class ScanRequestStreamSuite extends FunSuite:
  private val streamNames = Set(
    "proxy.events",
    "wireless.audit",
    "wireless.alert.rogue_ap",
    "proxy.payload_audit"
  )

  test("known and unknown event streams are accepted independently of typed load configuration"):
    (streamNames + "wireless.unknown" + "custom.new-event").foreach { streamName =>
      assert(ScanRequestStream.shouldIngest(streamName, wirelessProjectionOnly = false), streamName)
    }

  test("projection-only mode suppresses only the companion wireless audit discovery record"):
    assert(!ScanRequestStream.shouldIngest("wireless.audit", wirelessProjectionOnly = true))
    (streamNames - "wireless.audit" + "wireless.unknown").foreach { streamName =>
      assert(ScanRequestStream.shouldIngest(streamName, wirelessProjectionOnly = true), streamName)
    }

  test("missing or malformed payload references are non-retriable poison records"):
    assert(ScanRequestStream.isNonRetriableResolutionError(IllegalArgumentException("bad payload")))
    assert(
      ScanRequestStream.isNonRetriableResolutionError(
        PostgresPayloadReadException(NoSuchFileException("missing.json"))
      )
    )

  test("transient payload I/O failures remain retriable"):
    assert(
      !ScanRequestStream.isNonRetriableResolutionError(
        PostgresPayloadReadException(AccessDeniedException("event.json"))
      )
    )
    assert(!ScanRequestStream.isNonRetriableResolutionError(RuntimeException("temporary failure")))
