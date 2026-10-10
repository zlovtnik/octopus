package com.sslproxy.coordinator.observability

import munit.FunSuite

import java.nio.file.{Files, Path}
import scala.util.matching.Regex

/** Locks Prometheus metric names used by alert rules and dashboards to the
  * names actually produced by `CoordinatorMetrics.scrape`.
  */
class ObservabilityContractSuite extends FunSuite:
  private val MetricName: Regex = """coordinator_[a-z0-9_]+""".r
  private val ForbiddenSuffixes = List("_value", "_total_count")

  private val Allowlist: Set[String] = Set(
    // recording-rule outputs, not scrape series
    "octopus:ingest_processed:rate5m",
    "octopus:pending_ledger:current",
    // emitted lazily after first Kafka lag sample
    "coordinator_redpanda_lag_stale_seconds",
    "coordinator_redpanda_consumer_lag_records",
    "coordinator_redpanda_lag_refresh_failures_total"
  )

  private def repoRoot: Path =
    val userDir = Path.of(sys.props("user.dir")).toAbsolutePath
    Iterator
      .iterate(userDir)(_.getParent)
      .takeWhile(_ != null)
      .find(p => Files.isDirectory(p.resolve("cyber-stack")) && Files.isDirectory(p.resolve("docker")))
      .getOrElse(fail(s"repository root not found from $userDir"))

  private def scrapeNames: Set[String] =
    val metrics = CoordinatorMetrics()
    metrics.recordIngestProcessed(1)
    metrics.recordIngestDeduplicated(1)
    metrics.recordIngestInvocation(success = true)
    metrics.recordTickFailure("ingest")
    metrics.recordLagRefreshFailure("scan-v1")
    metrics.recordLeaseClaim("claimed")
    metrics.recordPostgresQuery("postgres.check_connectivity", "success", scala.concurrent.duration.DurationInt(1).millis)
    metrics.recordLockedBatchDuration("sync.scan.request", "success", scala.concurrent.duration.DurationInt(1).millis)
    metrics.recordRouteState("sync", "sync-scan-ingestion", running = true, suspended = false)
    metrics.recordProcessorState("sync-scan-ingestion", "ready", 0)
    metrics
      .scrape
      .linesIterator
      .map(_.trim)
      .filter(line => line.nonEmpty && !line.startsWith("#"))
      .flatMap { line =>
        val space = line.indexOf(' ')
        if space > 0 then Some(line.substring(0, space).replaceAll("\\{.*", "")) else None
      }
      .toSet

  private def consumerMetricNames(root: Path): Set[String] =
    val files = List(
      "cyber-stack/base/telemetry/config/prometheus/rules/recording.rules.yml",
      "cyber-stack/base/telemetry/config/prometheus/rules/availability.rules.yml",
      "cyber-stack/base/telemetry/config/prometheus/rules/capacity.alerts.yml",
      "cyber-stack/base/telemetry/config/grafana/dashboards/service-red-sync.json",
      "docker/observability/alerts.yml",
      "docker/observability/grafana/dashboards/sync-pipeline-latency.json"
    )
    files.flatMap { relative =>
      val path = root.resolve(relative)
      assert(Files.exists(path), s"missing consumer file $relative")
      val text = Files.readString(path)
      MetricName.findAllIn(text).toSet
    }.toSet

  test("alert and dashboard coordinator metric names exist in the scrape") {
    val root = repoRoot
    val scrape = scrapeNames
    val referenced = consumerMetricNames(root)
    val missing = referenced -- scrape -- Allowlist
    assertEquals(missing, Set.empty[String], s"referenced metrics missing from scrape: $missing")
  }

  test("scrape metric names never use hand-rolled statistic suffixes") {
    val names = scrapeNames
    val bad = names.filter(name => ForbiddenSuffixes.exists(name.endsWith))
    assertEquals(bad, Set.empty[String], s"forbidden suffixes present: $bad")
  }

  test("production scrape emits the canonical ingest and backlog series") {
    val names = scrapeNames
    assert(names.contains("coordinator_ingest_processed_total"), names.mkString(","))
    assert(names.contains("coordinator_pending_ledger_count"), names.mkString(","))
    assert(names.contains("coordinator_ingest_deduplicated_total"), names.mkString(","))
    assert(names.contains("coordinator_processor_lifecycle"), names.mkString(","))
  }
