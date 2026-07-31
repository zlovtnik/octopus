package com.sslproxy.coordinator.cutover

import cats.effect.IO
import com.sslproxy.coordinator.config.CutoverConfig
import io.circe.Json
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.{KeyPair, KeyPairGenerator, Signature}
import java.time.Instant
import java.util.Base64

class CutoverArtifactSuite extends CatsEffectSuite:
  private val GroupA = "octopus-scan-tidb-v1"
  private val GroupB = "octopus-load-tidb-v1"
  private val Topic = "sync.scan.request"
  private val VerifiedAt = Instant.parse("2026-07-21T20:05:00Z")

  test("verify canonical artifact, checksum, detached Ed25519 signature, and durable evidence"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )

    val verified = verify(fixture)

    assertEquals(verified.artifact.groupVersion, 1)
    assertEquals(verified.artifact.artifactSha256.length, 64)
    assertEquals(verified.publicKeySha256, fixture.config.publicKeySha256)
    assertEquals(verified.cutoffFor(GroupA, Topic, 0), Right(100L))

    val authorized = verified.authorizeRecordOffset(GroupA, Topic, 0, 100L).toOption.get
    val authorizedAgain = verified.authorizeRecordOffset(GroupA, Topic, 0, 100L).toOption.get
    assertEquals(authorized.key, CutoffKey(GroupA, Topic, 0))
    assertEquals(authorized.verification.groupVersion, 1)
    assertEquals(authorized.verification.artifactSha256, verified.artifact.artifactSha256)
    assertEquals(authorized.durable.sha256.length, 64)
    assertEquals(authorized.durable, authorizedAgain.durable)
    assert(authorized.durable.canonicalJson.contains("\"group_id\":\"octopus-scan-tidb-v1\""))

  test("reject a canonical artifact whose signed payload was tampered"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )
    val tampered = new String(fixture.artifactBytes, StandardCharsets.UTF_8)
      .replace("\"cutoff_offset\":100", "\"cutoff_offset\":101")
      .getBytes(StandardCharsets.UTF_8)

    verifyEither(fixture.copy(artifactBytes = tampered)) match
      case Left(_: CutoverChecksumMismatch) => ()
      case other => fail(s"expected checksum mismatch, found $other")

  test("reject a detached signature made by a different Ed25519 key"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )
    val otherKeyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair()
    val wrongSignature = encodeSignature(sign(fixture.artifactBytes, otherKeyPair))

    verifyEither(fixture.copy(signatureFileBytes = wrongSignature)) match
      case Left(_: CutoverSignatureMismatch) => ()
      case other => fail(s"expected signature mismatch, found $other")

  test("reject a public key that does not match the configured SHA-256 pin"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )
    val config = fixture.config.copy(publicKeySha256 = "0" * 64)

    verifyEither(fixture.copy(config = config)) match
      case Left(_: CutoverPublicKeyPinMismatch) => ()
      case other => fail(s"expected key pin mismatch, found $other")

  test("reject non-canonical JSON even when its semantic content is unchanged"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )
    val nonCanonical = (new String(fixture.artifactBytes, StandardCharsets.UTF_8) + "\n")
      .getBytes(StandardCharsets.UTF_8)

    verifyEither(fixture.copy(artifactBytes = nonCanonical)) match
      case Left(_: NonCanonicalCutoverArtifact) => ()
      case other => fail(s"expected non-canonical artifact error, found $other")

  test("cutoff lookup identity includes group, topic, and partition"):
    val fixture = signedFixture(
      List(
        PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L),
        PartitionCutoff(GroupB, Topic, partition = 0, cutoffOffset = 200L)
      )
    )
    val verified = verify(fixture)

    assertEquals(verified.cutoffFor(GroupA, Topic, 0), Right(100L))
    assertEquals(verified.cutoffFor(GroupB, Topic, 0), Right(200L))

  test("reject missing partition and missing coverage without falling back"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )
    val verified = verify(fixture)

    verified.cutoffFor(GroupA, Topic, 1) match
      case Left(MissingCutoverOffset(group, topic, partition)) =>
        assertEquals((group, topic, partition), (GroupA, Topic, 1))
      case other => fail(s"expected missing cutoff, found $other")

    verified.requireCoverage(List(CutoffKey(GroupA, Topic, 0), CutoffKey(GroupA, Topic, 1))) match
      case Left(MissingCutoverCoverage(missing)) =>
        assertEquals(missing, List(CutoffKey(GroupA, Topic, 1)))
      case other => fail(s"expected missing coverage, found $other")

  test("reject record offsets below cutoff"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )
    val verified = verify(fixture)

    verified.authorizeRecordOffset(GroupA, Topic, 0, 99L) match
      case Left(CutoverOffsetBelowCutoff(group, topic, partition, cutoff, observed)) =>
        assertEquals((group, topic, partition), (GroupA, Topic, 0))
        assertEquals(cutoff, 100L)
        assertEquals(observed, 99L)
      case other => fail(s"expected below-cutoff rejection, found $other")

  test("bootstrap position must equal the captured end offset exactly"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )
    val verified = verify(fixture)

    assert(verified.verifyBootstrapPosition(GroupA, Topic, 0, 100L).isRight)
    verified.verifyBootstrapPosition(GroupA, Topic, 0, 101L) match
      case Left(_: CutoverBootstrapOffsetMismatch) => ()
      case other => fail(s"expected exact-position rejection, found $other")

  test("reject duplicate exact cutoff triples"):
    val duplicate = PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L)
    val fixture = signedFixture(List(duplicate, duplicate))

    verifyEither(fixture) match
      case Left(CutoverFormatError(detail)) =>
        assert(detail.contains("unique by (group_id, topic, partition)"))
      case other => fail(s"expected duplicate cutoff rejection, found $other")

  test("reject safe identifiers with trailing line terminators"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L)),
      artifactId = "prod-cutover-20260721\n"
    )

    verifyEither(fixture) match
      case Left(CutoverFormatError(detail)) => assert(detail.contains("artifact_id"))
      case other => fail(s"expected full-string identifier rejection, found $other")

  test("load and verify artifact, detached signature, and pinned public key from files"):
    val fixture = signedFixture(
      List(PartitionCutoff(GroupA, Topic, partition = 0, cutoffOffset = 100L))
    )

    IO.blocking {
      val directory = Files.createTempDirectory("octopus-cutover-")
      val artifactPath = Files.write(directory.resolve("cutover.json"), fixture.artifactBytes)
      val signaturePath = Files.write(directory.resolve("cutover.json.sig"), fixture.signatureFileBytes)
      val publicKeyPath = Files.write(directory.resolve("cutover-public.b64"), fixture.publicKeySource)
      (directory, artifactPath, signaturePath, publicKeyPath)
    }.bracket { case (_, artifactPath, signaturePath, publicKeyPath) =>
      val fileConfig = fixture.config.copy(
        artifactPath = artifactPath.toString,
        signaturePath = signaturePath.toString,
        publicKeyPath = publicKeyPath.toString,
        publicKeyBase64 = ""
      )
      CutoverArtifactLoader.loadAndVerify[IO](fileConfig).map { verified =>
        assertEquals(verified.cutoffFor(GroupA, Topic, 0), Right(100L))
      }
    } { case (directory, artifactPath, signaturePath, publicKeyPath) =>
      IO.blocking {
        val filesDeleted = List(artifactPath, signaturePath, publicKeyPath)
          .map(Files.deleteIfExists)
          .forall(identity)
        val directoryDeleted = Files.deleteIfExists(directory)
        filesDeleted && directoryDeleted
      }.void
    }

  private def verify(fixture: SignedFixture): VerifiedCutoverArtifact =
    verifyEither(fixture) match
      case Right(value) => value
      case Left(error)  => fail(error.getMessage)

  private def verifyEither(
      fixture: SignedFixture
  ): Either[CutoverError, VerifiedCutoverArtifact] =
    CutoverArtifactVerifier.verify(
      fixture.artifactBytes,
      fixture.signatureFileBytes,
      fixture.publicKeySource,
      fixture.config,
      VerifiedAt
    )

  private def signedFixture(
      cutoffs: List[PartitionCutoff],
      artifactId: String = "prod-cutover-20260721"
  ): SignedFixture =
    val keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair()
    val artifactBytes = artifactDocument(cutoffs, artifactId)
    val publicKeyBytes = keyPair.getPublic.getEncoded
    val publicKeyBase64 = Base64.getEncoder.encodeToString(publicKeyBytes)
    val groups = cutoffs.map(_.groupId).distinct
    val config = CutoverConfig(
      artifactPath = "/not-used/artifact.json",
      signaturePath = "/not-used/artifact.sig",
      publicKeyPath = "",
      publicKeyBase64 = publicKeyBase64,
      publicKeySha256 = CutoverSha256.hex(publicKeyBytes),
      expectedSchemaVersion = 1,
      expectedClusterId = "redpanda-prod-1",
      requiredConsumerGroups = groups
    )
    SignedFixture(
      artifactBytes,
      encodeSignature(sign(artifactBytes, keyPair)),
      publicKeyBase64.getBytes(StandardCharsets.US_ASCII),
      config
    )

  private def artifactDocument(cutoffs: List[PartitionCutoff], artifactId: String): Array[Byte] =
    val cutoffJson = cutoffs.sortBy(cutoff => (cutoff.groupId, cutoff.topic, cutoff.partition)).map { cutoff =>
      Json.obj(
        "cutoff_offset" -> Json.fromLong(cutoff.cutoffOffset),
        "group_id" -> Json.fromString(cutoff.groupId),
        "partition" -> Json.fromInt(cutoff.partition),
        "topic" -> Json.fromString(cutoff.topic)
      )
    }
    val payload = Json.obj(
      "artifact_id" -> Json.fromString(artifactId),
      "captured_at" -> Json.fromString("2026-07-21T20:00:00Z"),
      "cluster_id" -> Json.fromString("redpanda-prod-1"),
      "cutoffs" -> Json.arr(cutoffJson*),
      "group_version" -> Json.fromInt(1),
      "schema_version" -> Json.fromInt(1)
    )
    val payloadCanonical = CanonicalCutoverJson.render(payload)
    val checksum = CutoverSha256.hex(payloadCanonical.getBytes(StandardCharsets.UTF_8))
    val document = payload.mapObject(_.add("artifact_sha256", Json.fromString(checksum)))
    CanonicalCutoverJson.render(document).getBytes(StandardCharsets.UTF_8)

  private def sign(document: Array[Byte], keyPair: KeyPair): Array[Byte] =
    val signer = Signature.getInstance("Ed25519")
    signer.initSign(keyPair.getPrivate)
    signer.update(document)
    signer.sign()

  private def encodeSignature(signature: Array[Byte]): Array[Byte] =
    Base64.getEncoder.encode(signature)

  private final case class SignedFixture(
      artifactBytes: Array[Byte],
      signatureFileBytes: Array[Byte],
      publicKeySource: Array[Byte],
      config: CutoverConfig
  )
