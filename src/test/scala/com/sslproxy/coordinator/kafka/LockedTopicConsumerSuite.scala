package com.sslproxy.coordinator.kafka

import com.sslproxy.coordinator.config.KafkaCfg
import com.sslproxy.coordinator.domain.BrokerConsumerContract
import com.sslproxy.coordinator.tidb.TidbResult
import munit.FunSuite
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{InvalidReplicationFactorException, TopicExistsException}

import java.util.concurrent.ExecutionException

class LockedTopicConsumerSuite extends FunSuite:
  private val ScanGroup = "octopus-scan-v1"
  private val ScanTopic = "sync.scan.request"

  test("consumer contract is deterministic and derived from the versioned Kafka group"):
    val first = BrokerConsumerContract.from(ScanGroup, ScanTopic)
    val second = BrokerConsumerContract.from(ScanGroup, ScanTopic)

    assertEquals(first, second)
    assertEquals(first.toOption.map(_.groupVersion), Some(1))
    assertEquals(first.toOption.map(_.contractSha256.length), Some(64))

  test("consumer contract rejects unversioned groups"):
    val result = BrokerConsumerContract.from("octopus-scan", ScanTopic)
    assert(result.left.toOption.exists(_.getMessage.contains("version suffix")))

  test("consumer contract changes with group or topic"):
    val baseline = BrokerConsumerContract.from(ScanGroup, ScanTopic).toOption.get
    val nextGroup = BrokerConsumerContract.from("octopus-scan-v2", ScanTopic).toOption.get
    val nextTopic = BrokerConsumerContract.from(ScanGroup, "sync.scan.other").toOption.get

    assertNotEquals(baseline.contractSha256, nextGroup.contractSha256)
    assertNotEquals(baseline.contractSha256, nextTopic.contractSha256)

  test("locked consumer settings replay from earliest and commit manually"):
    val settings = KafkaComponents.consumerSettings(kafkaConfig, kafkaConfig.loadConsumer)

    assertEquals(settings.properties(ConsumerConfig.GROUP_ID_CONFIG), kafkaConfig.loadConsumer)
    assertEquals(settings.properties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")
    assertEquals(settings.properties(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "false")
    assertEquals(settings.properties(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG), "false")
    assertEquals(settings.properties(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed")

  test("assignment validation rejects an unexpected topic"):
    val expected = List(new TopicPartition(ScanTopic, 0), new TopicPartition(ScanTopic, 1))
    val unexpected = expected :+ new TopicPartition("sync.scan.other", 0)

    assertEquals(LockedTopicConsumer.validateAssignments(ScanTopic, List.empty), Right(()))
    assertEquals(LockedTopicConsumer.validateAssignments(ScanTopic, expected), Right(()))
    assert(LockedTopicConsumer.validateAssignments(ScanTopic, unexpected).isLeft)

  test("topic provisioning accepts an already-existing topic race only"):
    assert(
      KafkaComponents.isTopicAlreadyExists(
        new ExecutionException(new TopicExistsException("topic already exists"))
      )
    )
    assert(
      !KafkaComponents.isTopicAlreadyExists(
        new ExecutionException(new IllegalStateException("topic creation failed"))
      )
    )

  test("topic provisioning detects invalid replication factor"):
    assert(
      KafkaComponents.isInvalidReplicationFactor(
        new ExecutionException(
          new InvalidReplicationFactorException("Unable to allocate topic with given replication factor")
        )
      )
    )
    assert(
      !KafkaComponents.isInvalidReplicationFactor(
        new ExecutionException(new IllegalStateException("replication failed"))
      )
    )

  test("topic provisioning reserves the expanded count for locked sync topics"):
    val cfg = kafkaConfig

    assertEquals(KafkaComponents.provisionedTopicPartitions(cfg, cfg.scanTopic), 24)
    assertEquals(KafkaComponents.provisionedTopicPartitions(cfg, cfg.loadTopic + cfg.dlqSuffix), 24)
    assertEquals(KafkaComponents.provisionedTopicPartitions(cfg, cfg.payloadAuditTopic), 3)
    assertEquals(KafkaComponents.provisionedTopicPartitions(cfg, "wireless.mac.lookup"), 3)

  test("result codec preserves the locked result payload"):
    val expected = TidbResult(
      jobId = "job-1",
      batchId = "batch-1",
      status = "success",
      rowCount = 7,
      checksum = "abc123",
      retryable = false,
      errorClass = "",
      errorText = "",
      finishedAt = "2026-07-21T21:00:00Z"
    )

    assertEquals(
      KafkaComponents.deserializeResult(KafkaComponents.serializeResult(expected)),
      Right(expected)
    )

  private def kafkaConfig: KafkaCfg =
    KafkaCfg(
      bootstrapServers = "redpanda:9092",
      loadTopic = "sync.oracle.load",
      resultTopic = "sync.oracle.result",
      scanTopic = ScanTopic,
      payloadAuditTopic = "proxy.payload_audit",
      dlqSuffix = ".dlq",
      scanConsumer = ScanGroup,
      resultConsumer = "octopus-result-v1",
      payloadAuditConsumer = "octopus-payload-audit-v1",
      loadConsumer = "octopus-load-v1",
      maxPollRecords = 100,
      pollTimeoutMs = 1000L,
      lockedBatchSize = 100,
      lockedBatchWindowMs = 250L,
      topicPartitions = 24,
      topicReplicationFactor = 1
    )
