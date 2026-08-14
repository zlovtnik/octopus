package com.sslproxy.coordinator.cutover

import com.sslproxy.coordinator.config.{AppConfig, CutoverConfig, IngestConfig, ProcessorConfig, RuntimeConfig, TiDbConfig}
import com.typesafe.config.{ConfigFactory, ConfigResolveOptions}
import munit.FunSuite

import scala.jdk.CollectionConverters.*

class AppConfigCutoverSuite extends FunSuite:
  test("application defaults disable TiDB, consumers, processors, and the processor catalog"):
    val config = AppConfig.load(
      ConfigFactory.parseResources("application.conf")
        .withFallback(ConfigFactory.empty())
        .resolve(ConfigResolveOptions.defaults().setUseSystemEnvironment(false))
    )

    assertEquals(config.tidb.enabled, false)
    assertEquals(config.tidb.poolSize, 20)
    assertEquals(config.tidb.healthcheckReserve, 2)
    assertEquals(config.kafka.lockedBatchSize, 500)
    assertEquals(config.kafka.lockedBatchWindowMs, 250L)
    assertEquals(config.kafka.topicPartitions, 24)
    assertEquals(config.kafka.topicReplicationFactor, 3)
    assertEquals(config.cron.batchDispatchRetryMaxSeconds, 300)
    assertEquals(config.runtime, RuntimeConfig(processorsEnabled = false, consumersEnabled = false))
    assertEquals(config.processors.enabled, List.empty)
    assertEquals(config.processors.restartBaseDelayMs, 1000L)
    assertEquals(config.processors.restartMaxDelayMs, 30000L)
    assertEquals(config.cutover.devBypass, false)

  test("comma-separated processor configuration preserves blank entries for validation"):
    val config = ConfigFactory.parseString(
      """processors.enabled = "sync-job-planner,,sync-load-dispatch""""
    ).withFallback(ConfigFactory.parseResources("application.conf"))
      .resolve(ConfigResolveOptions.defaults().setUseSystemEnvironment(false))

    val error = intercept[com.sslproxy.coordinator.config.AppConfigValidation] {
      AppConfig.load(config)
    }
    assert(error.getMessage.contains("blank processor IDs"))

  test("deployment SYNC variables configure locked topics and consumer groups"):
    val environment = ConfigFactory.parseMap(Map(
      "SYNC_SCAN_TOPIC" -> "sync.scan.request.cluster",
      "SYNC_PAYLOAD_AUDIT_TOPIC" -> "proxy.payload_audit.cluster",
      "SYNC_SCAN_CONSUMER" -> "octopus-scan-v7",
      "SYNC_LOAD_CONSUMER" -> "octopus-load-v7",
      "SYNC_RESULT_CONSUMER" -> "octopus-result-v7",
      "SYNC_PAYLOAD_AUDIT_CONSUMER" -> "octopus-payload-audit-v7",
      "COORDINATOR_SCAN_TOPIC" -> "coordinator.scan.request",
      "COORDINATOR_PAYLOAD_AUDIT_TOPIC" -> "coordinator.payload.audit",
      "COORDINATOR_SCAN_CONSUMER" -> "coordinator-scan-v6",
      "COORDINATOR_LOAD_CONSUMER" -> "coordinator-load-v6",
      "COORDINATOR_RESULT_CONSUMER" -> "coordinator-result-v6",
      "COORDINATOR_PAYLOAD_AUDIT_CONSUMER" -> "coordinator-payload-audit-v6",
      "SYNC_STREAM_NAMES" -> "proxy.events,proxy.payload_audit",
      "COORDINATOR_LOAD_STREAM_NAMES" -> "proxy.events,proxy.payload_audit"
    ).asJava)
    val config = ConfigFactory.parseResources("application.conf")
      .withFallback(environment)
      .resolve()
    val loaded = AppConfig.load(config)

    assertEquals(loaded.kafka.scanTopic, "sync.scan.request.cluster")
    assertEquals(loaded.kafka.payloadAuditTopic, "proxy.payload_audit.cluster")
    assertEquals(loaded.kafka.scanConsumer, "octopus-scan-v7")
    assertEquals(loaded.kafka.loadConsumer, "octopus-load-v7")
    assertEquals(loaded.kafka.resultConsumer, "octopus-result-v7")
    assertEquals(loaded.kafka.payloadAuditConsumer, "octopus-payload-audit-v7")
    assertEquals(loaded.ingest.streamNames, List("proxy.events", "proxy.payload_audit"))
    assertEquals(loaded.ingest.loadStreamNames, List("proxy.events", "proxy.payload_audit"))

  test("enabled runtime fails closed without artifact, signature, pinned key, cluster, and groups"):
    val baseline = AppConfig.load
    val enabled = baseline.copy(
      tidb = enabledTiDb(baseline.tidb),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = baseline.cutover.copy(devBypass = false)
    )

    AppConfig.validate(enabled) match
      case Left(error) =>
        val messages = error.errors.toList
        assert(messages.exists(_.contains("cutover.artifact-path")))
        assert(messages.exists(_.contains("cutover.signature-path")))
        assert(messages.exists(_.contains("public-key")))
        assert(messages.exists(_.contains("public-key-sha-256")))
        assert(messages.exists(_.contains("expected-cluster-id")))
        assert(messages.exists(_.contains("required-consumer-groups")))
      case Right(_) => fail("expected fail-closed configuration rejection")

  test("enabled runtime accepts complete cutover configuration and versioned groups"):
    val baseline = AppConfig.load
    val groups = configuredGroups(baseline)
    val enabled = baseline.copy(
      tidb = enabledTiDb(baseline.tidb),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = completeCutover(groups)
    )

    assertEquals(AppConfig.validate(enabled), Right(enabled))

  test("enabled runtime rejects unversioned consumer groups"):
    val baseline = AppConfig.load
    val kafka = baseline.kafka.copy(scanConsumer = "octopus-scan")
    val configured = baseline.copy(kafka = kafka)
    val groups = configuredGroups(configured)
    val enabled = configured.copy(
      tidb = enabledTiDb(configured.tidb),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = completeCutover(groups)
    )

    AppConfig.validate(enabled) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("every configured consumer group")))
      case Right(_) => fail("expected unversioned group rejection")

  test("enabled runtime rejects consumer groups and key pins with trailing newlines"):
    val baseline = AppConfig.load
    val configured = baseline.copy(
      kafka = baseline.kafka.copy(scanConsumer = "octopus-scan-v1\n")
    )
    val enabled = configured.copy(
      tidb = enabledTiDb(configured.tidb),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = completeCutover(configuredGroups(configured)).copy(publicKeySha256 = ("0" * 64) + "\n")
    )

    AppConfig.validate(enabled) match
      case Left(error) =>
        val messages = error.errors.toList
        assert(messages.exists(_.contains("every configured consumer group")))
        assert(messages.exists(_.contains("64 lowercase hexadecimal")))
      case Right(_) => fail("expected full-string validation rejection")

  test("required cutover groups must exactly match configured groups"):
    val baseline = AppConfig.load
    val groups = configuredGroups(baseline).drop(1)
    val enabled = baseline.copy(
      tidb = enabledTiDb(baseline.tidb),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = completeCutover(groups)
    )

    AppConfig.validate(enabled) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("must exactly match")))
      case Right(_) => fail("expected required group mismatch rejection")

  test("processor supervision settings reject duplicates and invalid restart delays"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(
      processors = ProcessorConfig(
        enabled = List("outbox-relay", "outbox-relay"),
        restartBaseDelayMs = 0L,
        restartMaxDelayMs = -1L
      )
    )

    AppConfig.validate(invalid) match
      case Left(error) =>
        val messages = error.errors.toList
        assert(messages.exists(_.contains("duplicate processor IDs")))
        assert(messages.exists(_.contains("restart-base-delay-ms")))
        assert(messages.exists(_.contains("restart-max-delay-ms")))
      case Right(_) => fail("expected invalid processor configuration rejection")

  test("processor configuration rejects unknown processor IDs"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(
      processors = baseline.processors.copy(enabled = List("sync-job-planer"))
    )

    AppConfig.validate(invalid) match
      case Left(error) =>
        assert(error.errors.toList.contains("unknown processor id: sync-job-planer"))
      case Right(_) => fail("expected unknown processor rejection")

  test("event retention fails closed unless archival is enabled"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(
      processors = baseline.processors.copy(enabled = List("event-retention")),
      runtime = baseline.runtime.copy(processorsEnabled = true)
    )

    AppConfig.validate(invalid) match
      case Left(error) =>
        assert(error.errors.toList.contains("event-retention requires archive.enabled=true"))
      case Right(_) => fail("expected event retention without archival to be rejected")

  test("topic auto-provisioning rejects an unsafe replication factor"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(
      kafka = baseline.kafka.copy(topicReplicationFactor = 1),
      tidb = enabledTiDb(baseline.tidb),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = completeCutover(configuredGroups(baseline))
    )

    AppConfig.validate(invalid) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("topic-replication-factor")))
      case Right(_) => fail("expected unsafe topic replication rejection")

  test("active runtime rejects replication factor below the minimum"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(
      kafka = baseline.kafka.copy(topicReplicationFactor = 2),
      tidb = enabledTiDb(baseline.tidb),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = completeCutover(configuredGroups(baseline))
    )

    AppConfig.validate(invalid) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("topic-replication-factor")))
      case Right(_) => fail("expected replication factor below minimum to be rejected")

  test("development cutover bypass permits a single broker replica"):
    val baseline = AppConfig.load
    val development = baseline.copy(
      kafka = baseline.kafka.copy(topicReplicationFactor = 1),
      runtime = baseline.runtime.copy(environment = "development"),
      cutover = baseline.cutover.copy(devBypass = true)
    )

    assertEquals(AppConfig.validate(development), Right(development))

  test("HTTP port must be in the TCP port range"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(http = baseline.http.copy(port = 65536))

    AppConfig.validate(invalid) match
      case Left(error) =>
        assert(error.errors.toList.contains("http.port must be between 1 and 65535"))
      case Right(_) => fail("expected invalid HTTP port rejection")

  test("locked consumer batching must fit within a poll and use a positive window"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(
      kafka = baseline.kafka.copy(
        maxPollRecords = 0,
        lockedBatchSize = baseline.kafka.maxPollRecords + 1,
        lockedBatchWindowMs = 0L
      )
    )

    AppConfig.validate(invalid) match
      case Left(error) =>
        val messages = error.errors.toList
        assert(messages.exists(_.contains("max-poll-records")))
        assert(messages.exists(_.contains("locked-batch-size")))
        assert(messages.exists(_.contains("locked-batch-window-ms")))
      case Right(_) => fail("expected invalid locked consumer batch configuration")

  test("TiDB worker reserve must leave at least one pooled connection"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(
      tidb = enabledTiDb(baseline.tidb).copy(healthcheckReserve = baseline.tidb.poolSize)
    )

    AppConfig.validate(invalid) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("healthcheck-reserve")))
      case Right(_) => fail("expected invalid TiDB connection reserve")

  test("batch dispatch retry maximum must cover its base backoff"):
    val baseline = AppConfig.load
    val invalid = baseline.copy(
      cron = baseline.cron.copy(batchDispatchRetryMaxSeconds = 1)
    )

    AppConfig.validate(invalid) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("batch-dispatch-retry-max-seconds")))
      case Right(_) => fail("expected invalid batch dispatch retry maximum")

  test("stage mode permits TiDB readiness with all runtime work disabled and no cutover artifact"):
    val baseline = AppConfig.load
    val staged = baseline.copy(
      kafka = baseline.kafka.copy(topicReplicationFactor = 1),
      tidb = enabledTiDb(baseline.tidb)
    )

    assertEquals(AppConfig.validate(staged), Right(staged))

  test("stage mode still rejects an invalid topic replication factor"):
    val baseline = AppConfig.load
    val staged = baseline.copy(
      kafka = baseline.kafka.copy(topicReplicationFactor = 0),
      tidb = enabledTiDb(baseline.tidb)
    )

    AppConfig.validate(staged) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("topic-replication-factor")))
      case Right(_) => fail("expected invalid staged replication factor rejection")

  test("cutover dev bypass still requires TiDB for an enabled runtime"):
    val baseline = AppConfig.load
    val activeWithoutTiDb = baseline.copy(
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = baseline.cutover.copy(devBypass = true)
    )

    AppConfig.validate(activeWithoutTiDb) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("requires tidb.enabled=true")))
      case Right(_) => fail("expected an enabled runtime without TiDB to fail")

  test("cutover dev bypass is restricted to the explicit development environment"):
    val baseline = AppConfig.load
    val production = baseline.copy(
      tidb = enabledTiDb(baseline.tidb),
      runtime = RuntimeConfig(processorsEnabled = true, consumersEnabled = true),
      cutover = baseline.cutover.copy(devBypass = true)
    )
    val development = production.copy(
      runtime = production.runtime.copy(environment = "development")
    )

    AppConfig.validate(production) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("OCTOPUS_ENVIRONMENT=development")))
      case Right(_) => fail("expected production dev bypass rejection")
    assertEquals(AppConfig.validate(development), Right(development))

  test("disabled production stage rejects devBypass without an active runtime"):
    val baseline = AppConfig.load
    val disabledProduction = baseline.copy(
      runtime = RuntimeConfig(processorsEnabled = false, consumersEnabled = false),
      cutover = baseline.cutover.copy(devBypass = true)
    )

    AppConfig.validate(disabledProduction) match
      case Left(error) =>
        assert(error.errors.toList.exists(_.contains("OCTOPUS_ENVIRONMENT=development")))
      case Right(_) => fail("expected disabled production stage to reject devBypass")

  test("stage mode rejects loopback root blank-password or downgraded TLS TiDB"):
    val baseline = AppConfig.load
    val staged = baseline.copy(tidb = baseline.tidb.copy(enabled = true, sslMode = "REQUIRED"))

    AppConfig.validate(staged) match
      case Left(error) =>
        val messages = error.errors.toList
        assert(messages.exists(_.contains("external TiDB cluster")))
        assert(messages.exists(_.contains("non-root")))
        assert(messages.exists(_.contains("tidb.password")))
        assert(messages.exists(_.contains("VERIFY_IDENTITY")))
        assert(messages.exists(_.contains("tidb.ssl-ca-path")))
        assert(messages.exists(_.contains("tidb.ssl-server-name")))
        assert(!messages.exists(_.contains("cutover.artifact-path")))
      case Right(_) => fail("expected invalid staged TiDB configuration rejection")

  test("enabled TiDB restricts public key retrieval to development bypass"):
    val baseline = AppConfig.load
    val staged = baseline.copy(
      tidb = enabledTiDb(baseline.tidb).copy(localDevAllowPublicKeyRetrieval = true)
    )
    val development = staged.copy(
      runtime = staged.runtime.copy(environment = "development"),
      cutover = staged.cutover.copy(devBypass = true)
    )

    AppConfig.validate(staged) match
      case Left(error) =>
        assert(error.errors.toList.contains(
          "tidb.local-dev-allow-public-key-retrieval requires development cutover bypass"
        ))
      case Right(_) => fail("expected public key retrieval rejection")
    assertEquals(AppConfig.validate(development), Right(development))

  test("all concurrency polling batch and cron bounds are validated"):
    val baseline = AppConfig.load
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

    val messages = AppConfig
      .validate(invalid)
      .left
      .toOption
      .map(_.errors.toList)
      .getOrElse(fail("expected invalid operational bounds"))
    assert(messages.exists(_.contains("idle-sleep-ms")))
    assert(messages.exists(_.contains("dispatch-batch-size")))
    assert(messages.exists(_.contains("ingest-batch-size")))
    assert(messages.exists(_.contains("scan-max-attempts")))
    assert(messages.exists(_.contains("heartbeat-log-interval-ms")))
    assert(messages.exists(_.contains("schema-refresh-interval-seconds")))
    assert(messages.exists(_.contains("scan-fetch-count")))
    assert(messages.exists(_.contains("result-fetch-count")))
    assert(messages.exists(_.contains("budget-multiplier")))
    assert(messages.exists(_.contains("adaptive-pull-change-threshold")))
    assert(messages.exists(_.contains("adaptive-pull-min-restart-interval-ms")))
    assert(messages.exists(_.contains("wireless.consumers-count")))
    assert(messages.exists(_.contains("wireless.max-poll-records")))

  test("enabled TiDB requires an exact canonical manifest checksum"):
    val baseline = AppConfig.load
    val staged = baseline.copy(
      tidb = enabledTiDb(baseline.tidb).copy(manifestSha256 = "not-a-checksum")
    )

    AppConfig.validate(staged) match
      case Left(error) =>
        assert(error.errors.toList.contains(
          "tidb.manifest-sha-256 must be 64 lowercase hexadecimal characters"
        ))
      case Right(_) => fail("expected manifest checksum rejection")

  test("proxy.events must remain in both ingest and TiDB load streams"):
    val baseline = AppConfig.load
    val missing = baseline.copy(
      ingest = IngestConfig(
        streamNames = baseline.ingest.streamNames.filterNot(_ == "proxy.events"),
        loadStreamNames = baseline.ingest.loadStreamNames.filterNot(_ == "proxy.events")
      )
    )

    AppConfig.validate(missing) match
      case Left(error) =>
        val messages = error.errors.toList
        assert(messages.exists(_.contains("ingest.stream-names must contain proxy.events")))
        assert(messages.exists(_.contains("persisted to TiDB")))
      case Right(_) => fail("expected proxy.events routing rejection")

  private def completeCutover(groups: List[String]): CutoverConfig =
    CutoverConfig(
      artifactPath = "/run/octopus/cutover.json",
      signaturePath = "/run/octopus/cutover.json.sig",
      publicKeyPath = "/run/octopus/cutover-public.pem",
      publicKeyBase64 = "",
      publicKeySha256 = "0" * 64,
      expectedSchemaVersion = 1,
      expectedClusterId = "redpanda-prod-1",
      requiredConsumerGroups = groups
    )

  private def configuredGroups(config: AppConfig): List[String] =
    List(
      config.kafka.scanConsumer,
      config.kafka.resultConsumer,
      config.kafka.payloadAuditConsumer,
      config.kafka.loadConsumer,
      config.wireless.backlogSaveConsumer,
      config.wireless.backlogListConsumer,
      config.wireless.backlogSyncedConsumer,
      config.wireless.backlogPruneConsumer,
      config.wireless.macLookupConsumer,
      config.wireless.networksAuthorizedConsumer,
      config.wireless.probeFlushConsumer
    )

  private def enabledTiDb(config: TiDbConfig): TiDbConfig =
    config.copy(
      host = "tidb.example.internal",
      database = "octopus_core",
      user = "octopus_runtime",
      password = "not-a-real-secret",
      enabled = true,
      warnOnly = false,
      sslMode = "VERIFY_IDENTITY",
      sslCaPath = "/etc/tidb-tls/ca.crt",
      sslServerName = "tidb.example.internal"
    )
