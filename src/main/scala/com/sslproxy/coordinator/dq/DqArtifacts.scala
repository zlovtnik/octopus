package com.sslproxy.coordinator.dq

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.config.ArchiveConfig
import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.parser.parse
import io.circe.{Json, Printer}
import io.minio.*
import io.minio.Http.Method
import io.minio.messages.VersioningConfiguration
import io.opentelemetry.api.trace.SpanKind
import okhttp3.OkHttpClient

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class DqPermanentError(val code: String) extends IllegalArgumentException(code)
final class DqStaleSource extends RuntimeException("source_revision_changed")

trait DqArtifacts:
  def initialize: IO[Unit]
  def putSnapshot(run: Json, snapshot: Json): IO[Json]
  def read(reference: Json, run: Json, output: Boolean): IO[Json]
  def authorizedAccess(reference: Json): IO[Json]

object DqArtifacts:
  val CanonicalPrinter: Printer = Printer.noSpaces.copy(sortKeys = true)
  def bytes(value: Json): Array[Byte] =
    DqJson.canonical(value).getBytes(StandardCharsets.UTF_8)
  def digest(value: Json): String = Sha256Utils.sha256Hex(bytes(value))

  private[dq] def httpClient(leaseSeconds: Int): OkHttpClient =
    // Each request is bounded well below the database lease. Retries belong to
    // the durable DQ state machine rather than an invisible HTTP retry loop.
    val seconds = math.max(1, leaseSeconds / 8).min(10).toLong
    new OkHttpClient.Builder()
      .connectTimeout(seconds, TimeUnit.SECONDS)
      .readTimeout(seconds, TimeUnit.SECONDS)
      .writeTimeout(seconds, TimeUnit.SECONDS)
      .callTimeout(seconds, TimeUnit.SECONDS)
      .retryOnConnectionFailure(false)
      .build()

  def client(storage: ArchiveConfig, leaseSeconds: Int): MinioClient =
    val value = MinioClient
      .builder()
      .endpoint(storage.endpoint)
      .credentials(storage.accessKey, storage.secretKey)
      .region(storage.region)
      .httpClient(httpClient(leaseSeconds), true)
      .build()
    value.setRetry(Set.empty[Integer].asJava, java.lang.Long.valueOf(0L), java.lang.Integer.valueOf(0))
    value

  def resource(
    config: DqConfig,
    storage: ArchiveConfig
  ): Resource[IO, DqArtifacts] =
    Resource
      .make(IO.blocking(client(storage, config.leaseSeconds)))(client => IO.blocking(client.close()))
      .map(client => new MinioDqArtifacts(client, config))

  private[dq] def required(value: Json, name: String): String =
    value.hcursor
      .get[String](name)
      .toOption
      .filter(_.nonEmpty)
      .getOrElse(throw DqPermanentError(s"missing_$name"))

  private[dq] def safeSegment(value: String): Boolean =
    "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$".r.matches(value)

  private[dq] def validatedObjectKey(
    reference: Json,
    run: Json,
    output: Boolean,
    config: DqConfig
  ): String =
    val installation = required(run, "installation_id")
    val namespace = required(run, "namespace")
    val runId = required(run, "run_id")
    val prefix = s"s3://${config.artifactBucket}/"
    val uri = required(reference, "uri")
    val key = uri.stripPrefix(prefix)
    val segments = key.split("/", -1).toList
    val attemptId = run.hcursor
      .downField("current_attempt")
      .get[String]("attempt_id")
      .toOption
    val expected =
      if output then attemptId.map(id => List(installation, namespace, "runs", runId, "attempts", id))
      else Some(List(installation, namespace, "inputs"))
    val schema =
      if output then "atheros.dedup.proposal.v1"
      else "atheros.dedup.snapshot.v1"
    val valid =
      installation == config.installationId && namespace == config.namespace &&
        required(reference, "installation_id") == installation && required(
          reference,
          "namespace"
        ) == namespace &&
        uri.startsWith(prefix) && segments.forall(safeSegment) &&
        expected.exists(value =>
          segments.take(
            value.size
          ) == value && segments.size == value.size + (if output then 1 else 2)
        ) &&
        required(reference, "schema_id") == schema && required(
          reference,
          "media_type"
        ) == "application/json" &&
        "^[0-9a-f]{64}$".r.matches(required(reference, "sha256")) &&
        required(reference, "version_id") != "null" &&
        (!output || (reference.hcursor
          .get[String]("producer_run_id")
          .contains(runId) &&
          reference.hcursor
            .get[String]("producer_attempt_id")
            .toOption == attemptId))
    if !valid then throw DqPermanentError("forbidden_artifact_reference")
    key

private final class MinioDqArtifacts(client: MinioClient, config: DqConfig) extends DqArtifacts:
  import DqArtifacts.*

  private def boundedCall[A](work: IO[A]): IO[A] =
    work.timeout((config.leaseSeconds / 2).seconds)

  def initialize: IO[Unit] = boundedCall(IO.blocking {
    val bucket = config.artifactBucket
    if !client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())
    then throw DqPermanentError("artifact_bucket_missing")
    val versioning = client.getBucketVersioning(
      GetBucketVersioningArgs.builder().bucket(bucket).build()
    )
    if versioning.status() != VersioningConfiguration.Status.ENABLED then
      throw DqPermanentError("artifact_bucket_versioning_required")
    val encryption = client.getBucketEncryption(
      GetBucketEncryptionArgs.builder().bucket(bucket).build()
    )
    if Option(encryption.rule()).isEmpty then throw DqPermanentError("artifact_bucket_encryption_required")
  })

  def putSnapshot(run: Json, snapshot: Json): IO[Json] =
    DqRuntime.traced("dq.minio.put_snapshot", run, SpanKind.CLIENT)(
      boundedCall(
        IO
          .blocking {
            val payload = bytes(snapshot)
            if payload.length > config.maxArtifactBytes then throw DqPermanentError("snapshot_size_exceeded")
            val sha = Sha256Utils.sha256Hex(payload)
            val installation = required(run, "installation_id")
            val namespace = required(run, "namespace")
            val runId = required(run, "run_id")
            if installation != config.installationId || namespace != config.namespace || !safeSegment(
                runId
              )
            then throw DqPermanentError("snapshot_scope_invalid")
            val key = s"$installation/$namespace/inputs/$runId/$sha"
            val stream = new ByteArrayInputStream(payload)
            val response =
              try
                client.putObject(
                  PutObjectArgs
                    .builder()
                    .bucket(config.artifactBucket)
                    .`object`(key)
                    .contentType("application/json")
                    .userMetadata(Map("sha256" -> sha).asJava)
                    .stream(stream, payload.length.toLong, -1L)
                    .build()
                )
              finally stream.close()
            val version = Option(response.versionId())
              .filter(value => value.nonEmpty && value != "null")
              .getOrElse(throw DqPermanentError("artifact_version_missing"))
            Json.obj(
              "artifact_id" -> Json.fromString(digest(Json.arr(Json.fromString(runId), Json.fromString(sha)))),
              "producer_run_id" -> Json.fromString(runId),
              "installation_id" -> Json.fromString(installation),
              "namespace" -> Json.fromString(namespace),
              "uri" -> Json.fromString(s"s3://${config.artifactBucket}/$key"),
              "version_id" -> Json.fromString(version),
              "etag" -> Json.fromString(response.etag()),
              "sha256" -> Json.fromString(sha),
              "schema_id" -> Json.fromString("atheros.dedup.snapshot.v1"),
              "media_type" -> Json.fromString("application/json"),
              "size_bytes" -> Json.fromLong(payload.length.toLong)
            )
          }
          .flatTap(reference => read(reference, run, output = false).void)
      )
    )

  def read(reference: Json, run: Json, output: Boolean): IO[Json] =
    DqRuntime.traced("dq.minio.read", run, SpanKind.CLIENT)(boundedCall(IO.blocking {
      val key = validatedObjectKey(reference, run, output, config)
      val version = required(reference, "version_id")
      val size = reference.hcursor
        .get[Long]("size_bytes")
        .fold(_ => throw DqPermanentError("artifact_size_missing"), identity)
      if size < 0L || size > config.maxArtifactBytes.toLong then throw DqPermanentError("artifact_size_exceeded")
      val stat = client.statObject(
        StatObjectArgs
          .builder()
          .bucket(config.artifactBucket)
          .`object`(key)
          .versionId(version)
          .build()
      )
      if stat.size() != size || stat.etag() != required(reference, "etag") then
        throw DqPermanentError("artifact_metadata_mismatch")
      val stream = client.getObject(
        GetObjectArgs
          .builder()
          .bucket(config.artifactBucket)
          .`object`(key)
          .versionId(version)
          .build()
      )
      val payload =
        try stream.readNBytes(config.maxArtifactBytes + 1)
        finally stream.close()
      if payload.length.toLong != size || Sha256Utils.sha256Hex(
          payload
        ) != required(reference, "sha256")
      then throw DqPermanentError("artifact_integrity_failed")
      parse(new String(payload, StandardCharsets.UTF_8))
        .fold(_ => throw DqPermanentError("artifact_json_invalid"), identity)
    }))

  def authorizedAccess(reference: Json): IO[Json] = boundedCall(IO.blocking {
    val prefix = s"s3://${config.artifactBucket}/${config.installationId}/${config.namespace}/"
    val uri = required(reference, "uri")
    val key = uri.stripPrefix(s"s3://${config.artifactBucket}/")
    if !uri.startsWith(prefix) || !key.split("/", -1).forall(safeSegment) ||
      required(reference, "version_id") == "null"
    then throw DqPermanentError("forbidden_artifact_reference")
    val url = client.getPresignedObjectUrl(
      GetPresignedObjectUrlArgs
        .builder()
        .method(Method.GET)
        .bucket(config.artifactBucket)
        .`object`(key)
        .expiry(60)
        .extraQueryParams(Map("versionId" -> required(reference, "version_id")).asJava)
        .build()
    )
    Json.obj(
      "url" -> Json.fromString(url),
      "expires_at" -> Json.fromString(java.time.Instant.now().plusSeconds(60).toString)
    )
  })
