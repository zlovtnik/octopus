package com.sslproxy.coordinator.config

import cats.data.NonEmptyList
import com.typesafe.config.Config
import pureconfig.{ConfigReader, ConfigCursor}
import pureconfig.error.ConfigReaderFailures
import com.sslproxy.coordinator.config.StringListConfigReader.given
import com.sslproxy.coordinator.processor.ProcessorId

object StringListConfigReader:
  given ConfigReader[List[String]] with
    def from(cursor: ConfigCursor): Either[ConfigReaderFailures, List[String]] =
      cursor.asList match
        case Right(cursors) =>
          cursors.foldRight(Right(Nil): Either[ConfigReaderFailures, List[String]]) { (c, acc) =>
            for
              s   <- c.asString.map(_.trim)
              tail <- acc
            yield if s.nonEmpty then s :: tail else tail
          }
        case Left(_) =>
          cursor.asString.map(s => s.split(",").map(_.trim).filter(_.nonEmpty).toList)

final case class AppConfig(
    tidb: TiDbConfig,
    kafka: KafkaCfg,
    cron: CronConfig,
    ingest: IngestConfig,
    backpressure: BackpressureConfig,
    http: HttpConfig,
    sync: SyncConfig,
    wireless: WirelessConfig,
    runtime: RuntimeConfig,
    processors: ProcessorConfig,
    archive: ArchiveConfig,
    cutover: CutoverConfig
) derives ConfigReader

final case class TiDbConfig(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: String,
    poolSize: Int,
    healthcheckReserve: Int,
    connectionTimeoutMs: Long,
    statementTimeoutSecs: Int,
    enabled: Boolean,
    warnOnly: Boolean,
    sslMode: String = "VERIFY_IDENTITY",
    sslCaPath: String = "",
    sslServerName: String = "",
    sslClientKeyStorePath: String = "",
    sslClientKeyStorePassword: String = "",
    sslClientKeyStoreType: String = "PKCS12",
    localDevAllowPublicKeyRetrieval: Boolean = false,
    manifestSha256: String = ""
) derives ConfigReader

final case class KafkaCfg(
    bootstrapServers: String,
    loadTopic: String,
    resultTopic: String,
    scanTopic: String,
    payloadAuditTopic: String,
    dlqSuffix: String,
    scanConsumer: String,
    resultConsumer: String,
    payloadAuditConsumer: String,
    loadConsumer: String,
    maxPollRecords: Int,
    pollTimeoutMs: Long,
    lockedBatchSize: Int,
    lockedBatchWindowMs: Long,
    topicPartitions: Int,
    topicReplicationFactor: Int
) derives ConfigReader

final case class CronConfig(
    idleSleepMs: Int,
    idleSleepBackoffMs: Int,
    dispatchBatchSize: Int,
    ingestBatchSize: Int,
    scanMaxAttempts: Int,
    scanRetryBackoffSeconds: Int,
    batchDispatchLeaseSeconds: Int,
    batchDispatchRetryMaxSeconds: Int,
    batchMaxAttempts: Int,
    heartbeatLogIntervalMs: Int,
    schemaRefreshIntervalSeconds: Int,
    scanFetchCount: Int,
    resultFetchCount: Int
) derives ConfigReader

final case class IngestConfig(
    streamNames: List[String],
    loadStreamNames: List[String]
) derives ConfigReader

final case class BackpressureConfig(
    budgetMultiplier: Int,
    adaptivePullChangeThreshold: Int,
    adaptivePullMinRestartIntervalMs: Int
) derives ConfigReader

final case class HttpConfig(
    port: Int
) derives ConfigReader

final case class SyncConfig(
    outboxDir: String
) derives ConfigReader

final case class RuntimeConfig(
    processorsEnabled: Boolean,
    consumersEnabled: Boolean,
    environment: String = "production"
) derives ConfigReader:
  def anyEnabled: Boolean = processorsEnabled || consumersEnabled

final case class ProcessorConfig(
    enabled: List[String],
    restartBaseDelayMs: Long,
    restartMaxDelayMs: Long,
    batchSize: Int = 250,
    intervalSeconds: Int = 10,
    embeddingModel: String = "sentence-transformers/all-MiniLM-L6-v2",
    eventDuplicateDistance: Double = 0.05d,
    behaviorSimilarityThreshold: Double = 0.88d,
    sequenceDistanceThreshold: Double = 0.10d
) derives ConfigReader

final case class ArchiveConfig(
    enabled: Boolean = false,
    endpoint: String = "http://minio:9000",
    accessKey: String = "",
    secretKey: String = "",
    bucket: String = "ssl-proxy-wireless-raw-archive",
    region: String = "us-east-1",
    hotDays: Int = 7,
    eventRetentionDays: Int = 30,
    searchRetentionDays: Int = 30,
    tombstoneRetentionDays: Int = 45,
    batchSize: Int = 100,
    intervalMs: Long = 300000L,
    maintenanceIntervalMs: Long = 3600000L
) derives ConfigReader

final case class CutoverConfig(
    artifactPath: String,
    signaturePath: String,
    publicKeyPath: String,
    publicKeyBase64: String,
    publicKeySha256: String,
    expectedSchemaVersion: Int,
    expectedClusterId: String,
    requiredConsumerGroups: List[String],
    devBypass: Boolean = false
) derives ConfigReader

final case class AppConfigValidation(errors: NonEmptyList[String])
    extends IllegalArgumentException(
      errors.toList.mkString("Invalid Octopus configuration: ", "; ", "")
    )

object AppConfig:
  private val VersionedConsumerGroup = "^[A-Za-z0-9._-]+[-_.]v[1-9][0-9]*$".r
  private val Sha256Hex = "^[0-9a-f]{64}$".r
  private val ProxyEventsStream = "proxy.events"

  def load: AppConfig =
    val config = pureconfig.ConfigSource.default.loadOrThrow[AppConfig]
    validate(config).fold(error => throw error, identity)

  private[coordinator] def load(config: Config): AppConfig =
    val loaded = pureconfig.ConfigSource.fromConfig(config).loadOrThrow[AppConfig]
    validate(loaded).fold(error => throw error, identity)

  def validate(config: AppConfig): Either[AppConfigValidation, AppConfig] =
    val isDevelopment = config.cutover.devBypass && config.runtime.environment == "development"
    val stagedTiDbErrors =
      if config.tidb.enabled then enabledTiDbErrors(config.tidb, isDevelopment)
      else List.empty
    val runtimeErrors =
      if config.runtime.anyEnabled || config.processors.enabled.nonEmpty then activeRuntimeErrors(config)
      else List.empty
    val errors =
      processorErrors(config.processors) ++
        List(
          Option.when(config.processors.enabled.nonEmpty && !config.runtime.processorsEnabled)(
            "processors.enabled requires runtime.processors-enabled=true"
          )
        ).flatten ++
        ingestErrors(config.ingest) ++
        archiveErrors(config.archive, config.processors) ++
        kafkaErrors(
          config.kafka,
          isDevelopment) ++
        wirelessErrors( config.wireless) ++
        backpressureErrors( config.backpressure
        ) ++
        cronErrors(config.cron) ++
        httpErrors(config.http) ++
        tidbBoundErrors(config.tidb) ++
        stagedTiDbErrors ++
        runtimeErrors

    NonEmptyList.fromList(errors) match
      case Some(values) => Left(AppConfigValidation(values))
      case None         => Right(config)

  private def ingestErrors(config: IngestConfig): List[String] =
    List(
      Option.when(!config.streamNames.contains(ProxyEventsStream))(
        s"ingest.stream-names must contain $ProxyEventsStream"
      ),
      Option.when(!config.loadStreamNames.contains(ProxyEventsStream))(
        s"ingest.load-stream-names must contain $ProxyEventsStream so proxy events are persisted to TiDB"
      ),
      Option.when(config.streamNames.distinct.size != config.streamNames.size)(
        "ingest.stream-names must not contain duplicates"
      ),
      Option.when(config.loadStreamNames.distinct.size != config.loadStreamNames.size)(
        "ingest.load-stream-names must not contain duplicates"
      ),
      Option.when(config.loadStreamNames.exists(name => !config.streamNames.contains(name)))(
        "ingest.load-stream-names must be a subset of ingest.stream-names"
      )
    ).flatten

  private def processorErrors(config: ProcessorConfig): List[String] =
    val idErrors = config.enabled.flatMap { id =>
      ProcessorId.fromString(id).fold(
        error => List(error),
        processor => Option.when(processor.owner != com.sslproxy.coordinator.processor.ProcessorOwner.Octopus)(
          s"processor $id is owned by ${processor.owner.value}, not octopus"
        ).toList
      )
    }

    List(
      Option.when(config.enabled.exists(_.trim.isEmpty))(
        "processors.enabled must not contain blank processor IDs"
      ),
      Option.when(config.enabled.distinct.size != config.enabled.size)(
        "processors.enabled must not contain duplicate processor IDs"
      ),
      Option.when(config.restartBaseDelayMs <= 0L)(
        "processors.restart-base-delay-ms must be positive"
      ),
      Option.when(config.restartMaxDelayMs < config.restartBaseDelayMs)(
        "processors.restart-max-delay-ms must be at least processors.restart-base-delay-ms"
      ),
      Option.when(config.batchSize <= 0)(
        "processors.batch-size must be positive"
      ),
      Option.when(config.intervalSeconds <= 0)(
        "processors.interval-seconds must be positive"
      ),
      Option.when(config.embeddingModel.trim.isEmpty)(
        "processors.embedding-model must not be blank"
      ),
      Option.when(
        !config.eventDuplicateDistance.isFinite ||config.eventDuplicateDistance < 0.0d || config.eventDuplicateDistance > 2.0d)(
        "processors.event-duplicate-distance must be between 0 and 2"
      ),
      Option.when(
        !config.behaviorSimilarityThreshold.isFinite ||config.behaviorSimilarityThreshold < -1.0d || config.behaviorSimilarityThreshold > 1.0d)(
        "processors.behavior-similarity-threshold must be between -1 and 1"
      ),
      Option.when(
        !config.sequenceDistanceThreshold.isFinite ||config.sequenceDistanceThreshold < 0.0d || config.sequenceDistanceThreshold > 2.0d)(
        "processors.sequence-distance-threshold must be between 0 and 2"
      )
    ).flatten ++ idErrors

  private def archiveErrors(config: ArchiveConfig, processors: ProcessorConfig): List[String] =
    val eventRetentionEnabled = processors.enabled.contains(ProcessorId.EventRetention.value)
    List(
      Option.when(eventRetentionEnabled && !config.enabled)(
        "event-retention requires archive.enabled=true"
      ),
      Option.when(config.enabled && config.endpoint.trim.isEmpty)("archive.endpoint must not be blank"),
      Option.when(config.enabled && config.accessKey.trim.isEmpty)("archive.access-key must not be blank"),
      Option.when(config.enabled && config.secretKey.trim.isEmpty)("archive.secret-key must not be blank"),
      Option.when(config.enabled && config.bucket.trim.isEmpty)("archive.bucket must not be blank"),
      Option.when(config.hotDays <= 0)("archive.hot-days must be positive"),
      Option.when(config.eventRetentionDays < config.hotDays)(
        "archive.event-retention-days must be at least archive.hot-days"
      ),
      Option.when(config.searchRetentionDays <= 0)(
        "archive.search-retention-days must be positive"
      ),
      Option.when(config.tombstoneRetentionDays < config.eventRetentionDays)(
        "archive.tombstone-retention-days must be at least archive.event-retention-days"
      ),
      Option.when(config.batchSize <= 0)("archive.batch-size must be positive"),
      Option.when(config.intervalMs <= 0L)("archive.interval-ms must be positive"),
      Option.when(config.maintenanceIntervalMs <= 0L)("archive.maintenance-interval-ms must be positive")
    ).flatten

  private def kafkaErrors(config: KafkaCfg, isDevelopment: Boolean): List[String] =
    List(
      Option.when(config.dlqSuffix.isEmpty)(
        "kafka.dlq-suffix must not be empty"
      ),
      Option.when(config.maxPollRecords <= 0)(
        "kafka.max-poll-records must be positive"
      ),
      Option.when(config.pollTimeoutMs <= 0L)(
        "kafka.poll-timeout-ms must be positive"
      ),
      Option.when(config.topicPartitions <= 0)(
        "kafka.topic-partitions must be positive"
      ),
      Option.when(config.lockedBatchSize <= 0)(
        "kafka.locked-batch-size must be positive"
      ),
      Option.when(config.lockedBatchSize > config.maxPollRecords)(
        "kafka.locked-batch-size must not exceed kafka.max-poll-records"
      ),
      Option.when(config.lockedBatchWindowMs <= 0L)(
        "kafka.locked-batch-window-ms must be positive"
      ),
      Option.when(
        isDevelopment &&
          (config.topicReplicationFactor < 1 || config.topicReplicationFactor > Short.MaxValue)
      )(
        "kafka.topic-replication-factor must be between 1 and 32767 in development mode"
      ),
      Option.when(
        !isDevelopment &&
          (config.topicReplicationFactor < 3 || config.topicReplicationFactor > Short.MaxValue)
      )(
        "kafka.topic-replication-factor must be between 3 and 32767"
      )
    ).flatten

  private def httpErrors(config: HttpConfig): List[String] =
    List(
      Option.when(config.port <= 0 || config.port > 65535)(
        "http.port must be between 1 and 65535"
      )
    ).flatten

  private def wirelessErrors(config: WirelessConfig): List[String] =
    List(
      Option.when(config.consumersCount <= 0)(
        "wireless.consumers-count must be positive"
      ),
      Option.when(config.maxPollRecords <= 0)(
        "wireless.max-poll-records must be positive"
      )
    ).flatten

  private def backpressureErrors(config: BackpressureConfig): List[String] =
    List(
      Option.when(config.budgetMultiplier <= 0)(
        "backpressure.budget-multiplier must be positive"
      ),
      Option.when(config.adaptivePullChangeThreshold <= 0)(
        "backpressure.adaptive-pull-change-threshold must be positive"
      ),
      Option.when(config.adaptivePullMinRestartIntervalMs <= 0)(
        "backpressure.adaptive-pull-min-restart-interval-ms must be positive"
      )
    ).flatten

  private def cronErrors(config: CronConfig): List[String] =
    List(
      Option.when(config.idleSleepMs <= 0)("cron.idle-sleep-ms must be positive"),
      Option.when(config.idleSleepBackoffMs <= 0)("cron.idle-sleep-backoff-ms must be positive"),
      Option.when(config.dispatchBatchSize <= 0)("cron.dispatch-batch-size must be positive"),
      Option.when(config.ingestBatchSize <= 0)("cron.ingest-batch-size must be positive"),
      Option.when(config.scanMaxAttempts <= 0)("cron.scan-max-attempts must be positive"),
      Option.when(config.scanRetryBackoffSeconds <= 0)("cron.scan-retry-backoff-seconds must be positive"),
      Option.when(config.batchDispatchLeaseSeconds <= 0)("cron.batch-dispatch-lease-seconds must be positive"),
      Option.when(config.batchDispatchRetryMaxSeconds <= 0)(
        "cron.batch-dispatch-retry-max-seconds must be positive"
      ),
      Option.when(config.batchDispatchRetryMaxSeconds < config.scanRetryBackoffSeconds)(
        "cron.batch-dispatch-retry-max-seconds must be at least cron.scan-retry-backoff-seconds"
      ),
      Option.when(config.batchMaxAttempts <= 0)("cron.batch-max-attempts must be positive"),
      Option.when(config.heartbeatLogIntervalMs <= 0)("cron.heartbeat-log-interval-ms must be positive"),
      Option.when(config.schemaRefreshIntervalSeconds <= 0)("cron.schema-refresh-interval-seconds must be positive"),
      Option.when(config.scanFetchCount <= 0)("cron.scan-fetch-count must be positive"),
      Option.when(config.resultFetchCount <= 0)("cron.result-fetch-count must be positive")
    ).flatten

  private def tidbBoundErrors(config: TiDbConfig): List[String] =

    List(
      Option.when(config.port <= 0 || config.port > 65535)(
        "tidb.port must be between 1 and 65535"),
      Option.when(config.poolSize <= 0)("tidb.pool-size must be positive"),
      Option.when(config.healthcheckReserve < 0)(
        "tidb.healthcheck-reserve must not be negative"
      ),
      Option.when(config.healthcheckReserve >= config.poolSize)(
        "tidb.healthcheck-reserve must be smaller than tidb.pool-size"
      ),
      Option.when(config.connectionTimeoutMs <= 0L)(
        "tidb.connection-timeout-ms must be positive"
      ),
      Option.when(config.statementTimeoutSecs <= 0)(
        "tidb.statement-timeout-secs must be positive"
      )
    ).flatten

  private def enabledTiDbErrors(config: TiDbConfig, isDevelopment: Boolean): List[String] =
    val normalizedHost = config.host.trim.toLowerCase(java.util.Locale.ROOT)
    val loopbackHosts = Set("localhost", "127.0.0.1", "::1", "[::1]")

    List(
      required(config.host, "tidb.host"),
      Option.when(loopbackHosts.contains(normalizedHost))(
        "tidb.host must reference the external TiDB cluster, not loopback"
      ),
      required(config.database, "tidb.database"),
      required(config.user, "tidb.user"),
      Option.when(config.user.trim.equalsIgnoreCase("root"))(
        "tidb.user must be a least-privilege non-root account"
      ),
      required(config.password, "tidb.password"
      ),
      Option.when(config.sslMode != "VERIFY_IDENTITY")(
        "tidb.ssl-mode must be VERIFY_IDENTITY"
      ),
      required(config.sslCaPath, "tidb.ssl-ca-path"),
      required(config.sslServerName, "tidb.ssl-server-name"),
      Option.when(
        config.sslServerName.trim.nonEmpty &&
          !config.sslServerName.trim.equalsIgnoreCase(config.host.trim)
      )(
        "tidb.ssl-server-name must equal tidb.host because Connector/J verifies the JDBC host identity"
      ),
      Option.when(
        config.sslClientKeyStorePath.trim.nonEmpty != config.sslClientKeyStorePassword.trim.nonEmpty
      )(
        "tidb.ssl-client-key-store-path and tidb.ssl-client-key-store-password must be configured together"
      ),
      Option.when(
        !Set("JKS", "PKCS12").contains(config.sslClientKeyStoreType.trim.toUpperCase(java.util.Locale.ROOT))
      )(
        "tidb.ssl-client-key-store-type must be JKS or PKCS12"
      ),
      Option.when(config.warnOnly)(
        "tidb.warn-only must be false when TiDB readiness is enabled"
      ),
      Option.when(config.localDevAllowPublicKeyRetrieval && !isDevelopment)(
        "tidb.local-dev-allow-public-key-retrieval requires development cutover bypass"
      ),
      Option.when(!Sha256Hex.matches(config.manifestSha256))(
        "tidb.manifest-sha-256 must be 64 lowercase hexadecimal characters"
      )
    ).flatten

  private def activeRuntimeErrors(config: AppConfig): List[String] =
    val cutover = config.cutover
    val configuredGroups = List(
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
    val lockedConsumerProcessorIds = Set(
      ProcessorId.SyncScanIngestion,
      ProcessorId.SyncLoadConsumer,
      ProcessorId.SyncResultConsumer
    )
    val enabledLockedConsumers =
      config.processors.enabled.flatMap(ProcessorId.fromString(_).toOption)
        .filter(lockedConsumerProcessorIds.contains)
    val runtimeInvariantErrors = List(
      Option.when(!config.tidb.enabled)(
        "an enabled runtime requires tidb.enabled=true"
      ),
      Option.when(enabledLockedConsumers.nonEmpty && !config.runtime.consumersEnabled)(
        s"locked-consumer processors ${enabledLockedConsumers.map(_.value).mkString(", ")} require runtime.consumers-enabled=true"
      )
    ).flatten

    if cutover.devBypass then
      runtimeInvariantErrors ++ List(
        Option.when(config.runtime.environment != "development")(
          "cutover.dev-bypass requires OCTOPUS_ENVIRONMENT=development"
        )
      ).flatten
    else
      val requiredGroups = cutover.requiredConsumerGroups
      val keySources = List(cutover.publicKeyPath, cutover.publicKeyBase64).count(_.trim.nonEmpty)

      runtimeInvariantErrors ++ List(
        required(cutover.artifactPath, "cutover.artifact-path"),
        required(cutover.signaturePath, "cutover.signature-path"),
        Option.when(keySources != 1)(
          "exactly one of cutover.public-key-path or cutover.public-key-base-64 is required"
        ),
        Option.when(!Sha256Hex.matches(cutover.publicKeySha256))(
          "cutover.public-key-sha-256 must be 64 lowercase hexadecimal characters"
        ),
        Option.when(cutover.expectedSchemaVersion <= 0)(
          "cutover.expected-schema-version must be positive"
        ),
        required(cutover.expectedClusterId, "cutover.expected-cluster-id"),
        Option.when(requiredGroups.isEmpty)(
          "cutover.required-consumer-groups must not be empty"
        ),
        Option.when(requiredGroups.distinct.size != requiredGroups.size)(
          "cutover.required-consumer-groups must not contain duplicates"
        ),
        Option.when(configuredGroups.exists(group => !isVersionedConsumerGroup(group)))(
          "every configured consumer group must end in a non-zero version suffix such as -v1"
        ),
        Option.when(requiredGroups.exists(group => !isVersionedConsumerGroup(group)))(
          "every cutover.required-consumer-groups entry must end in a non-zero version suffix such as -v1"
        ),
        Option.when(configuredGroups.toSet != requiredGroups.toSet)(
          "cutover.required-consumer-groups must exactly match the configured consumer groups"
        )
      ).flatten

  private def required(value: String, path: String): Option[String] =
    Option.when(value.trim.isEmpty)(s"$path must not be blank")

  private def isVersionedConsumerGroup(group: String): Boolean =
    VersionedConsumerGroup.matches(group)
