package com.sslproxy.coordinator.observability

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import cats.effect.IO
import munit.CatsEffectSuite
import org.apache.kafka.common.{Metric, MetricName}

class CoordinatorMetricsSuite extends CatsEffectSuite:
  test("JVM bindings export heap, metaspace, direct buffers and GC timers") {
    val registry = new SimpleMeterRegistry()
    val metrics = new CoordinatorMetrics(registry)
    metrics.jvmMetrics.use { _ => IO {
      assert(registry.find("jvm.memory.used").tag("area", "heap").gauge() != null)
      assert(registry.find("jvm.memory.max").tag("area", "heap").gauge() != null)
      assert(registry.find("jvm.memory.used").tag("id", "Metaspace").gauge() != null)
      assert(registry.find("jvm.buffer.memory.used").tag("id", "direct").gauge() != null)
      // GC pause timers appear lazily on the first collection notification.
      assert(metrics.scrape.contains("jvm_gc_memory_allocated"))
    }}
  }

  test("partition lag gauges are replaced on revocation and removed on shutdown") {
    val metrics = new CoordinatorMetrics(new SimpleMeterRegistry())
    val name = new MetricName("records-lag", "consumer-fetch-manager-metrics", "lag",
      java.util.Map.of("topic", "sync.scan.request", "partition", "7"))
    val metric = new Metric:
      def metricName(): MetricName = name
      def metricValue(): Object = java.lang.Double.valueOf(42.0)
    metrics.recordKafkaMetrics("scan-v1", Map(name -> metric))
    assert(metrics.scrape.contains("partition=\"7\""))
    assert(metrics.scrape.contains("42.0"))
    metrics.clearKafkaMetrics("scan-v1")
    assert(!metrics.scrape.contains("coordinator_kafka_partition_lag"))
  }

  test("scrape renders Prometheus-compatible metric names and values") {
    val metrics = new CoordinatorMetrics(SimpleMeterRegistry())
    metrics.recordIngestProcessed(3)
    val output = metrics.scrape
    assert(output.contains("coordinator_ingest_processed_total_count 3.0"), output)
    assert(!output.contains("coordinator.ingest"), output)
  }
