package com.sslproxy.coordinator.config

import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
import munit.FunSuite

import scala.jdk.CollectionConverters.*

class AppConfigSuite extends FunSuite:
  test("application defaults keep local runtime lanes disabled"):
    val config = defaults

    assertEquals(config.postgres.enabled, false)
    assertEquals(config.postgres.poolSize, 20)
    assertEquals(config.postgres.healthcheckReserve, 2)
    assertEquals(config.kafka.lockedBatchSize, 500)
    assertEquals(config.kafka.lockedBatchWindowMs, 250L)
    assertEquals(config.kafka.topicPartitions, 24)
    assertEquals(config.kafka.topicReplicationFactor, 3)
    assertEquals(config.runtime, RuntimeConfig(processorsEnabled = false, consumersEnabled = false))
    assertEquals(config.processors.enabled, List.empty)

  test("active runtime uses Kafka offsets and accepts the deployed single-broker replication"):
    val baseline = defaults
    val active = baseline.copy(
      postgres = enabledPostgres(baseline.postgres),
      kafka = baseline.kafka.copy(topicReplicationFactor = 1),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true)
    )

    assertEquals(AppConfig.validate(active), Right(active))

  test("active runtime still requires PostgreSQL"):
    val baseline = defaults
    val invalid = baseline.copy(
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true)
    )

    assert(validationMessages(invalid).exists(_.contains("requires postgres.enabled=true")))

  test("all Kafka consumer groups remain explicitly versioned"):
    val baseline = defaults
    val invalid = baseline.copy(
      postgres = enabledPostgres(baseline.postgres),
      kafka = baseline.kafka.copy(scanConsumer = "octopus-scan"),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true)
    )

    assert(validationMessages(invalid).exists(_.contains("version suffix")))

  test("locked consumer processors require the consumer lane"):
    val baseline = defaults
    val invalid = baseline.copy(
      postgres = enabledPostgres(baseline.postgres),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = false),
      processors = baseline.processors.copy(enabled = List("sync-scan-ingestion"))
    )

    assert(validationMessages(invalid).exists(_.contains("require runtime.consumers-enabled=true")))

  test("an explicit processor catalog includes every enabled locked consumer"):
    val baseline = defaults
    val invalid = baseline.copy(
      postgres = enabledPostgres(baseline.postgres),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      processors = baseline.processors.copy(enabled = List("sync-job-planner"))
    )

    val messages = validationMessages(invalid)
    assert(messages.exists(_.contains("requires locked-consumer processors")))
    assert(messages.exists(_.contains("sync-scan-ingestion")))
    assert(messages.exists(_.contains("sync-load-consumer")))
    assert(messages.exists(_.contains("sync-result-consumer")))

  test("deployment SYNC variables configure locked topics and consumer groups"):
    val environment = ConfigFactory.parseMap(Map(
      "SYNC_SCAN_TOPIC" -> "sync.scan.request.cluster",
      "SYNC_PAYLOAD_AUDIT_TOPIC" -> "proxy.payload_audit.cluster",
      "SYNC_SCAN_CONSUMER" -> "octopus-scan-v7",
      "SYNC_LOAD_CONSUMER" -> "octopus-load-v7",
      "SYNC_RESULT_CONSUMER" -> "octopus-result-v7",
      "SYNC_PAYLOAD_AUDIT_CONSUMER" -> "octopus-payload-audit-v7",
      "SYNC_STREAM_NAMES" -> "proxy.events,proxy.payload_audit",
      "COORDINATOR_LOAD_STREAM_NAMES" -> "proxy.events,proxy.payload_audit"
    ).asJava)
    val loaded = AppConfig.load(
      ConfigFactory.parseResources("application.conf").withFallback(environment).resolve()
    )

    assertEquals(loaded.kafka.scanTopic, "sync.scan.request.cluster")
    assertEquals(loaded.kafka.payloadAuditTopic, "proxy.payload_audit.cluster")
    assertEquals(loaded.kafka.scanConsumer, "octopus-scan-v7")
    assertEquals(loaded.kafka.loadConsumer, "octopus-load-v7")
    assertEquals(loaded.kafka.resultConsumer, "octopus-result-v7")
    assertEquals(loaded.kafka.payloadAuditConsumer, "octopus-payload-audit-v7")
    assertEquals(loaded.ingest.streamNames, List("proxy.events", "proxy.payload_audit"))

  test("processor supervision rejects blanks duplicates unknown IDs and invalid delays"):
    val baseline = defaults
    val invalid = baseline.copy(
      processors = ProcessorConfig(
        enabled = List("sync-job-planner", "", "sync-job-planner", "missing-processor"),
        restartBaseDelayMs = 0L,
        restartMaxDelayMs = -1L
      )
    )
    val messages = validationMessages(invalid)

    assert(messages.exists(_.contains("blank processor IDs")))
    assert(messages.exists(_.contains("duplicate processor IDs")))
    assert(messages.exists(_.contains("unknown processor id")))
    assert(messages.exists(_.contains("restart-base-delay-ms")))
    assert(messages.exists(_.contains("restart-max-delay-ms")))

  test("event retention requires archival"):
    val baseline = defaults
    val invalid = baseline.copy(
      processors = baseline.processors.copy(enabled = List("event-retention")),
      runtime = baseline.runtime.copy(processorsEnabled = true)
    )

    assert(validationMessages(invalid).contains("event-retention requires archive.enabled=true"))

  test("topic replication factor must be a positive Kafka value"):
    val baseline = defaults
    val invalid = baseline.copy(kafka = baseline.kafka.copy(topicReplicationFactor = 0))

    assert(validationMessages(invalid).exists(_.contains("topic-replication-factor")))

  test("public key retrieval is restricted to explicit development mode"):
    val baseline = defaults
    val production = baseline.copy(
      postgres = enabledPostgres(baseline.postgres).copy(localDevAllowPublicKeyRetrieval = true)
    )
    val development = production.copy(runtime = production.runtime.copy(environment = "development"))

    assert(validationMessages(production).exists(_.contains("OCTOPUS_ENVIRONMENT=development")))
    assertEquals(AppConfig.validate(development), Right(development))

  test("HTTP and locked consumer batch bounds are validated"):
    val baseline = defaults
    val invalid = baseline.copy(
      http = baseline.http.copy(port = 65536),
      kafka = baseline.kafka.copy(
        maxPollRecords = 0,
        lockedBatchSize = baseline.kafka.maxPollRecords + 1,
        lockedBatchWindowMs = 0L
      )
    )
    val messages = validationMessages(invalid)

    assert(messages.exists(_.contains("http.port")))
    assert(messages.exists(_.contains("max-poll-records")))
    assert(messages.exists(_.contains("locked-batch-size")))
    assert(messages.exists(_.contains("locked-batch-window-ms")))

  test("all concurrency polling batch and cron bounds are validated"):
    val baseline = defaults
    val invalid = baseline.copy(
      cron = baseline.cron.copy(
        idleSleepMs = 0,
        dispatchBatchSize = 0,
        ingestBatchSize = 0,
        scanMaxAttempts = 0,
        heartbeatLogIntervalMs = 0,
        schemaRefreshIntervalSeconds = 0,
        scanFetchCount = 0,
        resultFetchCount = 0
      ),
      backpressure = baseline.backpressure.copy(
        budgetMultiplier = 0,
        adaptivePullChangeThreshold = 0,
        adaptivePullMinRestartIntervalMs = 0
      ),
      wireless = baseline.wireless.copy(consumersCount = 0, maxPollRecords = 0)
    )
    val messages = validationMessages(invalid)

    List(
      "idle-sleep-ms",
      "dispatch-batch-size",
      "ingest-batch-size",
      "scan-max-attempts",
      "heartbeat-log-interval-ms",
      "schema-refresh-interval-seconds",
      "scan-fetch-count",
      "result-fetch-count",
      "budget-multiplier",
      "adaptive-pull-change-threshold",
      "wireless.consumers-count",
      "wireless.max-poll-records"
    ).foreach(expected => assert(messages.exists(_.contains(expected)), expected))

  test("enabled PostgreSQL requires an exact canonical manifest checksum"):
    val baseline = defaults
    val invalid = baseline.copy(
      postgres = enabledPostgres(baseline.postgres).copy(manifestSha256 = "not-a-checksum")
    )

    assert(validationMessages(invalid).exists(_.contains("manifest-sha-256")))

  test("proxy.events remains in both ingest and PostgreSQL load streams"):
    val baseline = defaults
    val invalid = baseline.copy(
      ingest = IngestConfig(
        streamNames = baseline.ingest.streamNames.filterNot(_ == "proxy.events"),
        loadStreamNames = baseline.ingest.loadStreamNames.filterNot(_ == "proxy.events")
      )
    )

    val messages = validationMessages(invalid)
    assert(messages.exists(_.contains("ingest.stream-names must contain proxy.events")))
    assert(messages.exists(_.contains("persisted to PostgreSQL")))

  private def defaults: AppConfig =
    AppConfig.load(
      ConfigFactory.parseResources("application.conf")
        .withFallback(ConfigFactory.empty())
        .resolve(ConfigResolveOptions.defaults().setUseSystemEnvironment(false))
    )

  private def validationMessages(config: AppConfig): List[String] =
    AppConfig.validate(config).left.toOption.map(_.errors.toList).getOrElse(
      fail("expected invalid configuration")
    )

  private def enabledPostgres(config: PostgresConfig): PostgresConfig =
    config.copy(
      host = "postgres.example.internal",
      database = "sync",
      user = "octopus_runtime",
      password = "not-a-real-secret",
      enabled = true,
      warnOnly = false,
      sslMode = "verify-full",
      sslCaPath = "/etc/postgres-tls/ca.crt",
      sslServerName = "postgres.example.internal"
    )
