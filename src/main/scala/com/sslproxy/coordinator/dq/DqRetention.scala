package com.sslproxy.coordinator.dq

import cats.Monad
import cats.effect.{IO, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.ArchiveConfig
import com.sslproxy.coordinator.observability.StructuredLogger
import com.sslproxy.coordinator.postgres.sql.DqRetentionSql
import doobie.ConnectionIO
import doobie.implicits.*
import fs2.Stream
import io.circe.Json
import io.minio.{ListObjectsArgs, MinioClient, RemoveObjectArgs}

import java.time.Instant
import scala.concurrent.duration.*

/** An Octopus workload, not a new service. Multiple collectors may remove the
  * same exact expired object version safely; metadata pruning is row-fenced.
  * Orphans wait seven days, longer than the maximum authorized run deadline.
  */
final class DqRetention private[dq](
    config: DqConfig,
    store: DqStore,
    client: MinioClient,
    dbSemaphore: Semaphore[IO]
):
  private val log = StructuredLogger(getClass)
  private def database[A](effect: ConnectionIO[A]): IO[A] =
    dbSemaphore.permit.use(_ => effect.transact(store.xa))

  private def remove(key: String, version: String): IO[Unit] = IO.blocking {
    client.removeObject(RemoveObjectArgs.builder().bucket(config.artifactBucket)
      .`object`(key).versionId(version).build())
  }

  private def expiredBatch: IO[Int] =
    for
      reopened <- database(DqRetentionSql.reopenArtifacts(config.installationId, config.namespace, 100))
      closed <- database(DqRetentionSql.closeArtifacts(config.installationId, config.namespace, 100))
      references <- database(DqRetentionSql.expiredArtifacts(config.installationId, config.namespace, 100))
      _ <- references.traverse_ { reference =>
        val version = DqArtifacts.required(reference, "version_id")
        val key = DqRetention.objectKey(reference, config)
        remove(key, version) *> database(DqRetentionSql.forgetArtifact(DqArtifacts.required(reference, "artifact_id"), version)).void
      }
      pruned <- database(DqRetentionSql.pruneRuns(config.installationId, config.namespace, 100))
      evidence <- database(DqRetentionSql.pruneEvidence(config.installationId, config.namespace, 1000))
    yield reopened + closed + references.size + pruned + evidence

  private def expired: IO[Unit] =
    // Small transactions bound locks; drain the backlog instead of capping the
    // entire installation to one batch per hour. A time budget yields to the
    // next maintenance cycle even while live traffic continues.
    IO.monotonic.flatMap { started =>
      Monad[IO].tailRecM[Unit, Unit](()) { _ =>
        expiredBatch.flatMap { changed =>
          IO.monotonic.map { now =>
            if changed == 0 || now - started >= 4.minutes then Right(()) else Left(())
          }
        }
      }
    }

  private def orphans: IO[Unit] =
    val prefix = s"${config.installationId}/${config.namespace}/"
    Stream.eval(IO.blocking(client.listObjects(ListObjectsArgs.builder()
      .bucket(config.artifactBucket).prefix(prefix).recursive(true).includeVersions(true).build()).iterator()))
      .flatMap(iterator => Stream.unfoldEval(iterator) { current =>
        IO.blocking(if current.hasNext then Some((current.next().get(), current)) else None)
      }).evalMap { item =>
        val key = item.objectName()
        val version = Option(item.versionId()).filter(value => value.nonEmpty && value != "null")
        val old = Option(item.lastModified()).exists(_.toInstant.isBefore(Instant.now().minusSeconds(7L * 86400L)))
        if item.isDeleteMarker() || !old || !DqRetention.allowedKey(key, config) then IO.unit
        else version.traverse_ { exact =>
          database(DqRetentionSql.referenced(s"s3://${config.artifactBucket}/$key", exact)).flatMap {
            case true => IO.unit
            case false => remove(key, exact)
          }
        }
      }.compile.drain

  def collect: IO[Unit] = expired *> orphans
  private def guarded(effect: IO[Unit]): IO[Unit] = effect.handleErrorWith(_ => IO(log.warn("dq_retention", "status" -> "dependency_unavailable")))
  def stream: Stream[IO, Unit] =
    (Stream.eval(guarded(expired)) ++ Stream.awakeEvery[IO](5.minutes).evalMap(_ => guarded(expired)))
      .merge(Stream.eval(guarded(orphans)) ++ Stream.awakeEvery[IO](1.hour).evalMap(_ => guarded(orphans)))

object DqRetention:
  def resource(config: DqConfig, storage: ArchiveConfig, store: DqStore, semaphore: Semaphore[IO]): Resource[IO, DqRetention] =
    Resource.make(IO.blocking(DqArtifacts.client(storage, config.leaseSeconds)))(client => IO.blocking(client.close()))
      .map(client => new DqRetention(config, store, client, semaphore))

  private[dq] def allowedKey(key: String, config: DqConfig): Boolean =
    val parts = key.split("/", -1).toList
    val scoped = parts.take(2) == List(config.installationId, config.namespace)
    val shape = parts match
      case _ :: _ :: "inputs" :: _ :: _ :: Nil => true
      case _ :: _ :: "runs" :: _ :: "attempts" :: _ :: _ :: Nil => true
      case _ => false
    scoped && shape && parts.forall(DqArtifacts.safeSegment)

  private[dq] def objectKey(reference: Json, config: DqConfig): String =
    val uri = DqArtifacts.required(reference, "uri")
    val prefix = s"s3://${config.artifactBucket}/"
    val key = uri.stripPrefix(prefix)
    if !uri.startsWith(prefix) || !allowedKey(key, config) || DqArtifacts.required(reference, "version_id") == "null" then
      throw DqPermanentError("forbidden_retention_reference")
    key
