package com.sslproxy.coordinator.observability

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import munit.FunSuite

class CoordinatorMetricsSuite extends FunSuite:
  test("scrape renders Prometheus-compatible metric names and values") {
    val metrics = new CoordinatorMetrics(SimpleMeterRegistry())
    metrics.recordIngestProcessed(3)
    val output = metrics.scrape
    assert(output.contains("coordinator_ingest_processed_total_count 3.0"), output)
    assert(!output.contains("coordinator.ingest"), output)
  }
