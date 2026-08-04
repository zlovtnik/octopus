package com.sslproxy.coordinator.archive

import cats.effect.kernel.Async
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.sslproxy.coordinator.config.ArchiveConfig
import com.sslproxy.coordinator.tidb.ArchiveCandidate
import com.sslproxy.coordinator.util.Sha256Utils
import io.minio.errors.ErrorResponseException
import io.minio.{MinioClient, PutObjectArgs, StatObjectArgs}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.time.ZoneOffset
import scala.jdk.CollectionConverters.*

final case class ArchiveReceipt(uri: String, payloadBytes: Long, payloadSha256: String)

trait PayloadArchive[F[_]]:
  def archive(candidate: ArchiveCandidate): F[ArchiveReceipt]

private[archive] final case class StoredArchiveObject(
    size: Long,
    payloadSha256: Option[String]
)

private[archive] trait ArchiveObjectStore[F[_]]:
  def stat(objectKey: String): F[Option[StoredArchiveObject]]
  def put(objectKey: String, bytes: Array[Byte], payloadSha256: String): F[Unit]

private[archive] final class HashVerifiedPayloadArchive[F[_]: Async](
    store: ArchiveObjectStore[F],
    bucket: String
) extends PayloadArchive[F]:
  def archive(candidate: ArchiveCandidate): F[ArchiveReceipt] =
    val bytes = candidate.payload.getBytes(StandardCharsets.UTF_8)
    val actualSha256 = Sha256Utils.sha256Hex(bytes)
    if !MinioPayloadArchive.isLowercaseSha256(candidate.dedupeKey) then
      Async[F].raiseError(
        IllegalArgumentException(
          "archive dedupe key must be a lowercase SHA-256 value"
        )
      )
    else if actualSha256 != candidate.payloadSha256 then
      Async[F].raiseError(IllegalArgumentException(
        s"archive payload hash mismatch for ${candidate.streamName}/${candidate.dedupeKey}"
      ))
    else
      val objectKey = MinioPayloadArchive.objectKey(candidate)
      val receipt = ArchiveReceipt(
        s"s3://$bucket/$objectKey",
        bytes.length.toLong,
        actualSha256
      )
      verify(store.stat(objectKey), objectKey, bytes.length.toLong, actualSha256).flatMap {
        case true => Async[F].pure(receipt)
        case false =>
          store.put(objectKey, bytes, actualSha256) *>
            verify(store.stat(objectKey), objectKey, bytes.length.toLong, actualSha256).flatMap {
              case true  => Async[F].pure(receipt)
              case false =>
                Async[F].raiseError(
                  IllegalStateException(s"archive object $objectKey failed post-upload verification")
                )
            }
      }

  private def verify(
      value: F[Option[StoredArchiveObject]],
      objectKey: String,
      expectedSize: Long,
      expectedSha256: String
  ): F[Boolean] =
    value.flatMap {
      case Some(stored)
          if stored.payloadSha256.isEmpty =>
        Async[F].pure(false)
      case Some(stored)
          if stored.size == expectedSize && stored.payloadSha256.contains(expectedSha256) =>
        Async[F].pure(true)
      case Some(_) =>
        Async[F].raiseError(
          IllegalStateException(s"archive object $objectKey exists with different content")
        )
      case None => Async[F].pure(false)
    }

private final class MinioObjectStore(
    client: MinioClient,
    bucket: String
) extends ArchiveObjectStore[IO]:
  def put(objectKey: String, bytes: Array[Byte], payloadSha256: String): IO[Unit] =
    IO.blocking {
      val stream = new ByteArrayInputStream(bytes)
      try
        client.putObject(
          PutObjectArgs.builder()
            .bucket(bucket)
            .`object`(objectKey)
            .contentType("application/json")
            .userMetadata(Map("sha256" -> payloadSha256).asJava)
            .stream(stream, bytes.length.toLong, -1L)
            .build()
        )
      finally stream.close()
    }.void

  def stat(objectKey: String): IO[Option[StoredArchiveObject]] =
    IO.blocking {
      try
        val value = client.statObject(
          StatObjectArgs.builder().bucket(bucket).`object`(objectKey).build()
        )
        val storedSha256 = Option(value.userMetadata().get("sha256"))
          .flatMap(_.asScala.headOption)
        Some(StoredArchiveObject(value.size(), storedSha256))
      catch
        case error: ErrorResponseException
            if Set("NoSuchKey", "NoSuchObject", "NotFound").contains(error.errorResponse().code()) =>
          None
    }

object MinioPayloadArchive:
  private val LowercaseSha256 = "^[0-9a-f]{64}$".r
  def resource(config: ArchiveConfig): Resource[IO, PayloadArchive[IO]] =
    Resource.make(IO.blocking {
      MinioClient.builder()
        .endpoint(config.endpoint)
        .credentials(config.accessKey, config.secretKey)
        .region(config.region)
        .build()
    })(client => IO.blocking(client.close())).map { client =>
      new HashVerifiedPayloadArchive[IO](new MinioObjectStore(client, config.bucket), config.bucket)
    }

  private[archive] def objectKey(candidate: ArchiveCandidate): String =
    require(isLowercaseSha256(candidate.dedupeKey), "archive dedupe key must be a lowercase SHA-256 value")
    val date = candidate.observedAt.toInstant.atZone(ZoneOffset.UTC).toLocalDate
    f"wireless/${date.getYear}%04d/${date.getMonthValue}%02d/${date.getDayOfMonth}%02d/${candidate.dedupeKey}.json"

  private[archive] def isLowercaseSha256(value: String): Boolean =
    LowercaseSha256.matches(Option(value).getOrElse(""))
