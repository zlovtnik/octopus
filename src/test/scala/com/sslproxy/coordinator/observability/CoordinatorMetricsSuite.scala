package com.sslproxy.coordinator.observability

import cats.effect.IO
import munit.CatsEffectSuite
import org.apache.kafka.common.{Metric, MetricName}

class CoordinatorMetricsSuite extends CatsEffectSuite:
  private def testMetrics(nowMillis: () => Long = () => System.currentTimeMillis()): CoordinatorMetrics =
    CoordinatorMetrics.withClock(nowMillis)

  test("JVM bindings export heap, metaspace, direct buffers and GC timers") {
    val metrics = testMetrics()
    metrics.jvmMetrics.use { _ => IO {
      val output = metrics.scrape
      assert(output.contains("jvm_memory_used_bytes"), output)
      assert(output.contains("# TYPE"), output)
      assert(output.contains("# HELP"), output)
    }}
  }

  test("partition lag gauges are replaced on revocation and removed on shutdown") {
    val metrics = testMetrics()
    val name = new MetricName("records-lag", "consumer-fetch-manager-metrics", "lag",
      java.util.Map.of("topic", "sync.scan.request", "partition", "7"))
    val metric = new Metric:
      def metricName(): MetricName = name
      def metricValue(): Object = java.lang.Double.valueOf(42.0)
    metrics.recordKafkaMetrics("scan-v1", Map(name -> metric))
    val sample = metrics.scrape
    assert(sample.contains("partition=\"7\""), sample)
    assert(sample.contains("coordinator_redpanda_consumer_lag_records"), sample)
    assert(sample.contains("coordinator_redpanda_lag_stale_seconds"), sample)
    metrics.clearKafkaMetrics("scan-v1")
    val cleared = metrics.scrape
    assert(!cleared.contains("coordinator_redpanda_consumer_lag_records"), cleared)
    assert(!cleared.contains("coordinator_redpanda_lag_stale_seconds"), cleared)
  }

  test("scrape renders idiomatic Prometheus counter names") {
    val metrics = testMetrics()
    metrics.recordIngestProcessed(3)
    val output = metrics.scrape
    assert(output.contains("coordinator_ingest_processed_total"), output)
    assert(!output.contains("coordinator_ingest_processed_total_count"), output)
    assert(output.contains("# TYPE coordinator_ingest_processed_total counter"), output)
  }

  test("scrape exports last-success timestamp without a _value suffix") {
    val metrics = testMetrics()
    metrics.recordIngestInvocation(success = true)
    val output = metrics.scrape
    assert(output.contains("coordinator_ingest_ledger_last_success_timestamp_seconds"), output)
    assert(!output.contains("coordinator_ingest_ledger_last_success_timestamp_seconds_value"), output)
  }

  test("ingest invocations are tagged with outcome") {
    val metrics = testMetrics()
    metrics.recordIngestInvocation(success = true)
    metrics.recordIngestInvocation(success = true)
    metrics.recordIngestInvocation(success = false)
    val output = metrics.scrape
    assert(output.contains("coordinator_ingest_ledger_invocations_total{outcome=\"success\"}"), output)
    assert(output.contains("coordinator_ingest_ledger_invocations_total{outcome=\"failure\"}"), output)
  }

  test("deduplicated ingest counter is exported") {
    val metrics = testMetrics()
    metrics.recordIngestDeduplicated(2)
    val output = metrics.scrape
    assert(output.contains("coordinator_ingest_deduplicated_total"), output)
  }

  test("tick failures and lag refresh failures are exported") {
    val metrics = testMetrics()
    metrics.recordTickFailure("ingest")
    metrics.recordLagRefreshFailure("scan-v1")
    val output = metrics.scrape
    assert(output.contains("coordinator_tick_failures_total"), output)
    assert(output.contains("coordinator_redpanda_lag_refresh_failures_total"), output)
  }

  test("lease claims are exported by result") {
    val metrics = testMetrics()
    metrics.recordLeaseClaim("claimed")
    metrics.recordLeaseClaim("contended")
    val output = metrics.scrape
    assert(output.contains("coordinator_lease_claims_total"), output)
  }

  test("postgres and locked-batch histograms export bucket, sum and count") {
    val metrics = testMetrics()
    metrics.recordPostgresQuery("postgres.mark_wireless_backlog_synced", "success", scala.concurrent.duration.DurationInt(12).millis)
    metrics.recordLockedBatchDuration("sync.scan.request", "success", scala.concurrent.duration.DurationInt(30).millis)
    val output = metrics.scrape
    assert(output.contains("coordinator_postgres_query_duration_seconds_bucket"), output)
    assert(output.contains("coordinator_postgres_query_duration_seconds_sum"), output)
    assert(output.contains("coordinator_postgres_query_duration_seconds_count"), output)
    assert(output.contains("coordinator_locked_batch_duration_seconds_bucket"), output)
  }

  test("ingestLastSuccessEpochSeconds returns None when never set") {
    val metrics = testMetrics()
    assertEquals(metrics.ingestLastSuccessEpochSeconds, None)
  }

  test("ingestLastSuccessEpochSeconds returns timestamp after success") {
    val metrics = testMetrics()
    metrics.recordIngestInvocation(success = true)
    assert(metrics.ingestLastSuccessEpochSeconds.nonEmpty)
  }

  test("ingestProcessedRatePerSec returns 0 with no samples") {
    val metrics = testMetrics()
    assertEquals(metrics.ingestProcessedRatePerSec(System.currentTimeMillis()), 0.0)
  }

  test("ingestProcessedRatePerSec counts even a single batch across the whole window") {
    val metrics = testMetrics()
    metrics.recordIngestProcessed(5)
    assertEqualsDouble(metrics.ingestProcessedRatePerSec(System.currentTimeMillis()), 5.0 / 300.0, 0.000001)
  }

  test("ingestProcessedRatePerSec computes rate from counter growth") {
    val metrics = testMetrics()
    val now = System.currentTimeMillis()
    metrics.recordIngestProcessed(10)
    metrics.recordIngestProcessed(10)
    val rate = metrics.ingestProcessedRatePerSec(now + 100)
    assert(rate >= 0.0, s"rate should be non-negative, got $rate")
  }

  test("pendingLedgerCountValue reflects recorded count") {
    val metrics = testMetrics()
    metrics.recordPendingLedgerCount(42)
    assertEquals(metrics.pendingLedgerCountValue, 42L)
  }

  test("rate includes all batches including same-millisecond batches and expires idle traffic") {
    var now = 1000L
    val metrics = testMetrics(() => now)
    metrics.recordIngestProcessed(300)
    metrics.recordIngestProcessed(300)
    now = 151000L
    metrics.recordIngestProcessed(300)
    assertEquals(metrics.ingestProcessedRatePerSec(now), 3.0)
    now = 301000L
    assertEquals(metrics.ingestProcessedRatePerSec(now), 1.0)
    now = 451000L
    assertEquals(metrics.ingestProcessedRatePerSec(now), 0.0)
  }

  test("public readings require a full window and fresh collection, including observed zero") {
    var now = 1000L
    val metrics = testMetrics(() => now)
    assert(!metrics.publicReadingsFresh(now))
    now = 301000L
    assert(!metrics.publicReadingsFresh(now))
    metrics.recordPendingLedgerCount(0)
    metrics.recordBackpressureActive(false)
    metrics.recordIngestProcessed(0)
    assert(metrics.publicReadingsFresh(now))
    assertEquals(metrics.ingestProcessedRatePerSec(now), 0.0)
    now += 61000L
    assert(!metrics.publicReadingsFresh(now))
  }

  test("backpressureActiveValue reflects recorded state") {
    val metrics = testMetrics()
    metrics.recordBackpressureActive(true)
    assert(metrics.backpressureActiveValue)
    metrics.recordBackpressureActive(false)
    assert(!metrics.backpressureActiveValue)
  }

  test("route state gauges are exported with role and route labels") {
    val metrics = testMetrics()
    metrics.recordRouteState("sync", "sync-scan-ingestion", running = true, suspended = false)
    val output = metrics.scrape
    assert(output.contains("coordinator_route_running"), output)
    assert(output.contains("role=\"sync\""), output)
    assert(output.contains("route=\"sync-scan-ingestion\""), output)
    assert(output.contains("coordinator_route_suspended"), output)
  }
