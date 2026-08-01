package com.sslproxy.coordinator.kafka

import com.sslproxy.coordinator.config.{CutoverConfig, KafkaCfg}
import com.sslproxy.coordinator.cutover.*
import com.sslproxy.coordinator.tidb.TidbResult
import munit.FunSuite
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException

import java.nio.charset.StandardCharsets
import java.security.{KeyPairGenerator, MessageDigest, Signature}
import java.time.Instant
import java.util.{Base64, HexFormat}
import java.util.concurrent.ExecutionException

class LockedTopicConsumerSuite extends FunSuite:
  private val ScanGroup = "octopus-scan-v1"
  private val ScanTopic = "sync.scan.request"
  private val Cutoff = 100L

  test("first observed partition offset must exactly equal the signed cutoff"):
    val artifact = verifiedArtifact(ScanGroup, ScanTopic, Cutoff)

    val accepted = CutoverOffsetGuard.authorize(
      CutoverOffsetGuardState.empty,
      artifact,
      ScanGroup,
      ScanTopic,
      partition = 0,
      offset = Cutoff
    )

    accepted match
      case Right((state, evidence)) =>
        assert(state.contains(CutoffKey(ScanGroup, ScanTopic, 0)))
        assertEquals(evidence.evidenceType, CutoverEvidenceType.BootstrapPosition)
      case Left(error) => fail(error.getMessage)

    CutoverOffsetGuard.authorize(
      CutoverOffsetGuardState.empty,
      artifact,
      ScanGroup,
      ScanTopic,
      partition = 0,
      offset = Cutoff + 1L
    ) match
      case Left(_: CutoverBootstrapOffsetMismatch) => ()
      case other => fail(s"expected exact bootstrap mismatch, found $other")

  test("later records use at-or-after authorization and reject below-cutoff replay"):
    val artifact = verifiedArtifact(ScanGroup, ScanTopic, Cutoff)
    val bootstrapped = CutoverOffsetGuard.authorize(
      CutoverOffsetGuardState.empty,
      artifact,
      ScanGroup,
      ScanTopic,
      partition = 0,
      offset = Cutoff
    ).toOption.get._1

    val later = CutoverOffsetGuard.authorize(
      bootstrapped,
      artifact,
      ScanGroup,
      ScanTopic,
      partition = 0,
      offset = Cutoff + 7L
    )
    assertEquals(
      later.toOption.map(_._2.evidenceType),
      Some(CutoverEvidenceType.RecordOffsetAuthorization)
    )

    CutoverOffsetGuard.authorize(
      bootstrapped,
      artifact,
      ScanGroup,
      ScanTopic,
      partition = 0,
      offset = Cutoff - 1L
    ) match
      case Left(_: CutoverOffsetBelowCutoff) => ()
      case other => fail(s"expected below-cutoff rejection, found $other")

  test("missing assigned partition coverage fails closed"):
    val artifact = verifiedArtifact(ScanGroup, ScanTopic, Cutoff)
    val assigned = List(new TopicPartition(ScanTopic, 0), new TopicPartition(ScanTopic, 1))

    LockedTopicConsumer.validateCoverage(artifact, ScanGroup, ScanTopic, assigned) match
      case Left(_: MissingCutoverCoverage) => ()
      case other => fail(s"expected missing coverage rejection, found $other")

  test("locked consumer settings use earliest with a guard-enforced cutoff"):
    val settings = KafkaComponents.consumerSettings(kafkaConfig, kafkaConfig.loadConsumer)

    assertEquals(settings.properties(ConsumerConfig.GROUP_ID_CONFIG), kafkaConfig.loadConsumer)
    assertEquals(settings.properties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")
    assertEquals(settings.properties(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "false")
    assertEquals(settings.properties(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG), "false")
    assertEquals(settings.properties(ConsumerConfig.ISOLATION_LEVEL_CONFIG), "read_committed")

  test("topic provisioning accepts an already-existing topic race only"):
    assert(KafkaComponents.isTopicAlreadyExists(
      new ExecutionException(new TopicExistsException("topic already exists"))
    ))
    assert(!KafkaComponents.isTopicAlreadyExists(
      new ExecutionException(new IllegalStateException("topic creation failed"))
    ))

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
      payloadAuditTopic = "proxy.payload.audit",
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
      topicReplicationFactor = 3
    )

  private def verifiedArtifact(
      groupId: String,
      topic: String,
      cutoff: Long
  ): VerifiedCutoverArtifact =
    val keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair()
    val payload =
      s"{\"artifact_id\":\"locked-topic-test\",\"captured_at\":\"2026-07-21T20:00:00Z\"," +
        s"\"cluster_id\":\"redpanda-test\",\"cutoffs\":[{\"cutoff_offset\":$cutoff," +
        s"\"group_id\":\"$groupId\",\"partition\":0,\"topic\":\"$topic\"}]," +
        "\"group_version\":1,\"schema_version\":1}"
    val artifactSha256 = sha256(payload.getBytes(StandardCharsets.UTF_8))
    val document =
      s"{\"artifact_id\":\"locked-topic-test\",\"artifact_sha256\":\"$artifactSha256\"," +
        s"\"captured_at\":\"2026-07-21T20:00:00Z\",\"cluster_id\":\"redpanda-test\"," +
        s"\"cutoffs\":[{\"cutoff_offset\":$cutoff,\"group_id\":\"$groupId\"," +
        s"\"partition\":0,\"topic\":\"$topic\"}],\"group_version\":1,\"schema_version\":1}"
    val documentBytes = document.getBytes(StandardCharsets.UTF_8)
    val signer = Signature.getInstance("Ed25519")
    signer.initSign(keyPair.getPrivate)
    signer.update(documentBytes)
    val signatureBytes = Base64.getEncoder.encode(signer.sign())
    val publicKey = keyPair.getPublic.getEncoded
    val config = CutoverConfig(
      artifactPath = "/not-used/cutover.json",
      signaturePath = "/not-used/cutover.sig",
      publicKeyPath = "",
      publicKeyBase64 = Base64.getEncoder.encodeToString(publicKey),
      publicKeySha256 = sha256(publicKey),
      expectedSchemaVersion = 1,
      expectedClusterId = "redpanda-test",
      requiredConsumerGroups = List(groupId)
    )

    CutoverArtifactVerifier.verify(
      documentBytes,
      signatureBytes,
      Base64.getEncoder.encode(publicKey),
      config,
      Instant.parse("2026-07-21T20:05:00Z")
    ).fold(error => fail(error.getMessage), identity)

  private def sha256(bytes: Array[Byte]): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
