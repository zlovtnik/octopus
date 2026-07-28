package com.sslproxy.coordinator.cutover

import com.sslproxy.coordinator.config.{AppConfig, CutoverConfig, IngestConfig, ProcessorConfig, RuntimeConfig, TiDbConfig}
import com.typesafe.config.ConfigFactory
import munit.FunSuite

import scala.jdk.CollectionConverters.*

class AppConfigCutoverSuite extends FunSuite:
  test("application defaults disable TiDB, consumers, processors, and the processor catalog"):
    val config = AppConfig.load

    assertEquals(config.tidb.enabled, false)
    assertEquals(config.tidb.poolSize, 4)
    assertEquals(config.runtime, RuntimeConfig(processorsEnabled = false, consumersEnabled = false))
    assertEquals(config.processors.enabled, List.empty)
    assertEquals(config.processors.restartBaseDelayMs, 1000L)
    assertEquals(config.processors.restartMaxDelayMs, 30000L)

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

  test("stage mode permits TiDB readiness with all runtime work disabled and no cutover artifact"):
    val baseline = AppConfig.load
    val staged = baseline.copy(tidb = enabledTiDb(baseline.tidb))

    assertEquals(AppConfig.validate(staged), Right(staged))

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
