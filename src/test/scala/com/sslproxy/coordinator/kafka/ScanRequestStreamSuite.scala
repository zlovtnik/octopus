package com.sslproxy.coordinator.kafka

import com.sslproxy.coordinator.postgres.PostgresPayloadReadException
import munit.FunSuite

import java.nio.file.{AccessDeniedException, NoSuchFileException}

class ScanRequestStreamSuite extends FunSuite:
  private val configured = Set(
    "proxy.events",
    "wireless.audit",
    "wireless.alert.rogue_ap",
    "proxy.payload_audit"
  )

  test("every configured proxy and wireless stream is accepted for durable ingestion"):
    configured.foreach { streamName =>
      assert(ScanRequestStream.isConfiguredStream(streamName, configured), streamName)
    }

  test("unknown streams are rejected instead of silently skipped"):
    assert(!ScanRequestStream.isConfiguredStream("wireless.unknown", configured))

  test("configured stream names use the same trimming behavior as the ingest ledger"):
    assertEquals(
      ScanRequestStream.configuredStreamNames(
        List(" proxy.events ", "", "wireless.audit", "proxy.events")
      ),
      Set("proxy.events", "wireless.audit")
    )

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
