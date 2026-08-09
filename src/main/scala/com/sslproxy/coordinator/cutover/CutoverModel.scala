package com.sslproxy.coordinator.cutover

import io.circe.{Json, JsonObject}
import io.circe.parser.parse

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.MessageDigest
import java.time.Instant
import java.util.HexFormat
import scala.util.Try

sealed abstract class CutoverError(message: String) extends RuntimeException(message)

final case class CutoverReadError(kind: String, path: String, detail: String)
    extends CutoverError(s"Unable to read cutover $kind at $path: $detail")

final case class CutoverSizeError(kind: String, actualBytes: Long, maximumBytes: Long)
    extends CutoverError(
      s"Cutover $kind is $actualBytes bytes; maximum allowed size is $maximumBytes bytes"
    )

final case class CutoverFormatError(detail: String)
    extends CutoverError(s"Invalid cutover artifact: $detail")

final case class NonCanonicalCutoverArtifact()
    extends CutoverError("Cutover artifact bytes are not canonical JSON")

final case class CutoverChecksumMismatch(expected: String, actual: String)
    extends CutoverError(s"Cutover artifact checksum mismatch: expected $expected, calculated $actual")

final case class CutoverSignatureError(detail: String)
    extends CutoverError(s"Invalid cutover signature: $detail")

final case class CutoverPublicKeyError(detail: String)
    extends CutoverError(s"Invalid cutover Ed25519 public key: $detail")

final case class CutoverPublicKeyPinMismatch(expected: String, actual: String)
    extends CutoverError(s"Cutover public key fingerprint mismatch: expected $expected, calculated $actual")

final case class CutoverSignatureMismatch()
    extends CutoverError("Detached Ed25519 cutover signature verification failed")

final case class MissingCutoverOffset(groupId: String, topic: String, partition: Int)
    extends CutoverError(
      s"Cutover artifact has no offset for group=$groupId topic=$topic partition=$partition"
    )

final case class CutoverOffsetBelowCutoff(
    groupId: String,
    topic: String,
    partition: Int,
    cutoffOffset: Long,
    observedOffset: Long
) extends CutoverError(
      s"Offset $observedOffset is below cutoff $cutoffOffset for " +
        s"group=$groupId topic=$topic partition=$partition"
    )

final case class CutoverBootstrapOffsetMismatch(
    groupId: String,
    topic: String,
    partition: Int,
    cutoffOffset: Long,
    observedOffset: Long
) extends CutoverError(
      s"Bootstrap position $observedOffset does not exactly match cutoff $cutoffOffset for " +
        s"group=$groupId topic=$topic partition=$partition"
    )

final case class MissingCutoverCoverage(missing: List[CutoffKey])
    extends CutoverError(
      "Cutover artifact is missing required offsets: " +
        missing.map(key => s"${key.groupId}/${key.topic}/${key.partition}").mkString(",")
    )

final case class CutoffKey(groupId: String, topic: String, partition: Int)

final case class PartitionCutoff(
    groupId: String,
    topic: String,
    partition: Int,
    cutoffOffset: Long
):
  def key: CutoffKey = CutoffKey(groupId, topic, partition)

/** The OCTOPUS-CUTOVER-1 signed document.
  *
  * `artifactSha256` is SHA-256 over the canonical root object with the
  * `artifact_sha256` member omitted. The detached Ed25519 signature covers the
  * complete canonical UTF-8 document, including `artifact_sha256`.
  */
final case class CutoverArtifact(
    schemaVersion: Int,
    artifactId: String,
    clusterId: String,
    capturedAt: Instant,
    groupVersion: Int,
    cutoffs: List[PartitionCutoff],
    artifactSha256: String
)

private[cutover] final case class ParsedCutoverArtifact(
    artifact: CutoverArtifact,
    canonicalBytes: Array[Byte],
    canonicalSha256: String
)

private[cutover] object CutoverArtifactCodec:
  private val RootKeys = Set(
    "artifact_id",
    "artifact_sha256",
    "captured_at",
    "cluster_id",
    "cutoffs",
    "group_version",
    "schema_version"
  )
  private val CutoffKeys = Set("cutoff_offset", "group_id", "partition", "topic")
  private val SafeIdentifier = "^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$".r
  private val KafkaTopic = "^[A-Za-z0-9._-]{1,249}$".r
  private val VersionedGroup = "^([A-Za-z0-9._-]+)[-_.]v([1-9][0-9]*)$".r
  private val Sha256Hex = "^[0-9a-f]{64}$".r

  def parseAndValidate(
      bytes: Array[Byte],
      expectedSchemaVersion: Int,
      expectedClusterId: String,
      requiredConsumerGroups: List[String]
  ): Either[CutoverError, ParsedCutoverArtifact] =
    for
      raw <- decodeUtf8(bytes)
      json <- parse(raw).left.map(error => CutoverFormatError(error.message))
      canonical = CanonicalCutoverJson.render(json)
      _ <- Either.cond(raw == canonical, (), NonCanonicalCutoverArtifact())
      root <- requiredObject(json, "root")
      _ <- exactKeys(root, RootKeys, "root")
      schemaVersion <- strictInt(root, "schema_version", "root")
      artifactId <- requiredString(root, "artifact_id", "root")
      artifactSha256 <- requiredString(root, "artifact_sha256", "root")
      capturedAtText <- requiredString(root, "captured_at", "root")
      clusterId <- requiredString(root, "cluster_id", "root")
      groupVersion <- strictInt(root, "group_version", "root")
      cutoffJson <- requiredArray(root, "cutoffs", "root")
      cutoffs <- decodeCutoffs(cutoffJson)
      capturedAt <- parseInstant(capturedAtText)
      artifact = CutoverArtifact(
        schemaVersion,
        artifactId,
        clusterId,
        capturedAt,
        groupVersion,
        cutoffs,
        artifactSha256
      )
      _ <- validateArtifact(
        artifact,
        expectedSchemaVersion,
        expectedClusterId,
        requiredConsumerGroups
      )
      payloadJson = Json.fromJsonObject(root.remove("artifact_sha256"))
      calculatedChecksum = CutoverSha256.hex(
        CanonicalCutoverJson.render(payloadJson).getBytes(StandardCharsets.UTF_8)
      )
      _ <- Either.cond(
        MessageDigest.isEqual(
          artifactSha256.getBytes(StandardCharsets.US_ASCII),
          calculatedChecksum.getBytes(StandardCharsets.US_ASCII)
        ),
        (),
        CutoverChecksumMismatch(artifactSha256, calculatedChecksum)
      )
      canonicalBytes = canonical.getBytes(StandardCharsets.UTF_8)
    yield ParsedCutoverArtifact(
      artifact,
      canonicalBytes,
      CutoverSha256.hex(canonicalBytes)
    )

  private def decodeCutoffs(values: Vector[Json]): Either[CutoverError, List[PartitionCutoff]] =
    traverse(values.zipWithIndex.toList) { case (json, index) =>
      for
        obj <- requiredObject(json, s"cutoffs[$index]")
        _ <- exactKeys(obj, CutoffKeys, s"cutoffs[$index]")
        groupId <- requiredString(obj, "group_id", s"cutoffs[$index]")
        topic <- requiredString(obj, "topic", s"cutoffs[$index]")
        partition <- strictInt(obj, "partition", s"cutoffs[$index]")
        cutoffOffset <- strictLong(obj, "cutoff_offset", s"cutoffs[$index]")
      yield PartitionCutoff(groupId, topic, partition, cutoffOffset)
    }

  private def decodeUtf8(bytes: Array[Byte]): Either[CutoverError, String] =
    Try {
      StandardCharsets.UTF_8
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString
    }.toEither.left.map(_ => CutoverFormatError("artifact must be valid UTF-8"))

  private def validateArtifact(
      artifact: CutoverArtifact,
      expectedSchemaVersion: Int,
      expectedClusterId: String,
      requiredConsumerGroups: List[String]
  ): Either[CutoverError, Unit] =
    val keys = artifact.cutoffs.map(_.key)
    val sortedKeys = keys.sortBy(key => (key.groupId, key.topic, key.partition))
    val groups = artifact.cutoffs.map(_.groupId).distinct.toSet
    val requiredGroups = requiredConsumerGroups.toSet
    val groupVersions = artifact.cutoffs.flatMap(cutoff => versionOf(cutoff.groupId))

    firstInvalid(
      List(
        Option.when(artifact.schemaVersion != expectedSchemaVersion)(
          s"schema_version ${artifact.schemaVersion} does not match expected $expectedSchemaVersion"
        ),
        Option.when(!SafeIdentifier.matches(artifact.artifactId))(
          "artifact_id must be a non-blank safe identifier of at most 128 characters"
        ),
        Option.when(!SafeIdentifier.matches(artifact.clusterId))(
          "cluster_id must be a non-blank safe identifier of at most 128 characters"
        ),
        Option.when(artifact.clusterId != expectedClusterId)(
          s"cluster_id ${artifact.clusterId} does not match expected $expectedClusterId"
        ),
        Option.when(artifact.groupVersion <= 0)("group_version must be positive"),
        Option.when(!Sha256Hex.matches(artifact.artifactSha256))(
          "artifact_sha256 must be 64 lowercase hexadecimal characters"
        ),
        Option.when(artifact.cutoffs.isEmpty)("cutoffs must not be empty"),
        Option.when(keys.distinct.size != keys.size)(
          "cutoffs must be unique by (group_id, topic, partition)"
        ),
        Option.when(keys != sortedKeys)(
          "cutoffs must be sorted by (group_id, topic, partition)"
        ),
        Option.when(artifact.cutoffs.exists(_.partition < 0))(
          "cutoff partitions must be non-negative"
        ),
        Option.when(artifact.cutoffs.exists(_.cutoffOffset < 0L))(
          "cutoff offsets must be non-negative"
        ),
        Option.when(artifact.cutoffs.exists(cutoff => !KafkaTopic.matches(cutoff.topic)))(
          "cutoff topics must use valid Kafka topic characters"
        ),
        Option.when(groupVersions.size != artifact.cutoffs.size)(
          "every cutoff group_id must end in a non-zero version suffix such as -v1"
        ),
        Option.when(groupVersions.exists(_ != artifact.groupVersion))(
          "every cutoff group_id version must equal group_version"
        ),
        Option.when(groups != requiredGroups)(
          "artifact consumer groups must exactly match cutover.required-consumer-groups"
        )
      )
    )

  private def versionOf(groupId: String): Option[Int] =
    groupId match
      case VersionedGroup(_, version) => version.toIntOption
      case _                          => None

  private def firstInvalid(errors: List[Option[String]]): Either[CutoverError, Unit] =
    errors.flatten.headOption match
      case Some(detail) => Left(CutoverFormatError(detail))
      case None         => Right(())

  private def requiredObject(json: Json, context: String): Either[CutoverError, JsonObject] =
    json.asObject.toRight(CutoverFormatError(s"$context must be an object"))

  private def exactKeys(
      obj: JsonObject,
      expected: Set[String],
      context: String
  ): Either[CutoverError, Unit] =
    val actual = obj.keys.toSet
    Either.cond(
      actual == expected,
      (),
      CutoverFormatError(
        s"$context fields must be exactly ${expected.toList.sorted.mkString(",")}; " +
          s"found ${actual.toList.sorted.mkString(",")}" 
      )
    )

  private def requiredString(
      obj: JsonObject,
      name: String,
      context: String
  ): Either[CutoverError, String] =
    obj(name).flatMap(_.asString).toRight(
      CutoverFormatError(s"$context.$name must be a string")
    )

  private def requiredArray(
      obj: JsonObject,
      name: String,
      context: String
  ): Either[CutoverError, Vector[Json]] =
    obj(name).flatMap(_.asArray).toRight(
      CutoverFormatError(s"$context.$name must be an array")
    )

  private def strictInt(
      obj: JsonObject,
      name: String,
      context: String
  ): Either[CutoverError, Int] =
    obj(name).flatMap(_.asNumber).flatMap { number =>
      number.toInt.filter(value => number.toString == value.toString)
    }.toRight(CutoverFormatError(s"$context.$name must be a canonical integer"))

  private def strictLong(
      obj: JsonObject,
      name: String,
      context: String
  ): Either[CutoverError, Long] =
    obj(name).flatMap(_.asNumber).flatMap { number =>
      number.toLong.filter(value => number.toString == value.toString)
    }.toRight(CutoverFormatError(s"$context.$name must be a canonical long"))

  private def parseInstant(value: String): Either[CutoverError, Instant] =
    Try(Instant.parse(value)).toEither.left.map { _ =>
      CutoverFormatError("captured_at must be an RFC 3339 UTC instant")
    }.flatMap { instant =>
      Either.cond(
        instant.toString == value,
        instant,
        CutoverFormatError("captured_at must use canonical UTC instant formatting")
      )
    }

  private def traverse[A, B](
      values: List[A]
  )(decode: A => Either[CutoverError, B]): Either[CutoverError, List[B]] =
    values.foldRight[Either[CutoverError, List[B]]](Right(List.empty)) { (value, result) =>
      for
        decoded <- decode(value)
        tail <- result
      yield decoded :: tail
    }

private[cutover] object CanonicalCutoverJson:
  /** Octopus' deliberately narrow canonical JSON profile: no whitespace or
    * terminal newline, recursively lexicographic object keys, preserved array
    * order, and Circe's compact JSON scalar representation. Artifact validation
    * further constrains all numeric values to canonical integers.
    */
  def render(json: Json): String =
    json.fold(
      "null",
      boolean => boolean.toString,
      number => number.toString,
      value => Json.fromString(value).noSpaces,
      values => values.iterator.map(render).mkString("[", ",", "]"),
      obj => obj.toIterable.toList.sortBy(_._1).iterator.map { case (key, value) =>
        s"${Json.fromString(key).noSpaces}:${render(value)}"
      }.mkString("{", ",", "}")
    )

private[cutover] object CutoverSha256:
  def hex(bytes: Array[Byte]): String =
    HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
