package com.sslproxy.coordinator.metrics

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.config.StatsStoreConfig
import io.circe.Json
import io.circe.parser as circeParser
import io.minio.{GetObjectArgs, MinioClient, PutObjectArgs, BucketExistsArgs, MakeBucketArgs}
import io.minio.errors.ErrorResponseException
import redis.clients.jedis.JedisPooled

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import scala.util.control.NonFatal

trait StatsSnapshotStore:
  def load: IO[Option[Json]]
  def save(json: Json): IO[Unit]

trait StatsHistoryStore:
  def putHistory(path: String, json: Json): IO[Unit]
  def putDaily(path: String, json: Json): IO[Unit]

final class RedisStatsStore(jedis: JedisPooled, key: String, ttlSeconds: Int) extends StatsSnapshotStore:
  def load: IO[Option[Json]] =
    IO.blocking {
      Option(jedis.get(key))
    }.flatMap {
      case None    => IO.pure(None)
      case Some(s) =>
        circeParser.parse(s) match
          case Right(json) => IO.pure(Some(json))
          case Left(_)     => IO.pure(None)
    }

  def save(json: Json): IO[Unit] =
    IO.blocking {
      jedis.setex(key, ttlSeconds.toLong, json.noSpaces)
      ()
    }

  def acquireLock(lockKey: String, ttlSeconds: Int): IO[Boolean] =
    IO.blocking {
      "OK" == jedis.set(lockKey, "1", redis.clients.jedis.params.SetParams.setParams().nx().ex(ttlSeconds.toLong))
    }

final class MinioStatsStore(
  client: MinioClient,
  bucket: String,
  prefix: String
) extends StatsSnapshotStore
    with StatsHistoryStore:

  private def objectKey(name: String): String =
    if prefix.isEmpty || prefix.endsWith("/") then s"$prefix$name"
    else s"$prefix/$name"

  def load: IO[Option[Json]] =
    IO.blocking {
      try
        val stream = client.getObject(GetObjectArgs.builder().bucket(bucket).`object`(objectKey("latest.json")).build())
        try
          val bytes = stream.readAllBytes()
          new String(bytes, StandardCharsets.UTF_8)
        finally stream.close()
      catch
        case _: ErrorResponseException => null
    }.flatMap {
      case null  => IO.pure(None)
      case body =>
        circeParser.parse(body) match
          case Right(json) => IO.pure(Some(json))
          case Left(_)     => IO.pure(None)
    }

  def save(json: Json): IO[Unit] =
    putBytes(objectKey("latest.json"), json)

  def putHistory(path: String, json: Json): IO[Unit] =
    putBytes(objectKey(s"history/$path"), json)

  def putDaily(path: String, json: Json): IO[Unit] =
    putBytes(objectKey(s"daily/$path"), json)

  private def putBytes(key: String, json: Json): IO[Unit] =
    val bytes = json.noSpaces.getBytes(StandardCharsets.UTF_8)
    IO.blocking {
      val stream = new ByteArrayInputStream(bytes)
      try
        client.putObject(
          PutObjectArgs
            .builder()
            .bucket(bucket)
            .`object`(key)
            .contentType("application/json")
            .stream(stream, bytes.length.toLong, -1L)
            .build()
        )
      finally stream.close()
    }.void

object StatsStores:
  def redisResource(config: StatsStoreConfig): Resource[IO, RedisStatsStore] =
    val (host, port) = parseRedisAddr(config.redisAddr)
    Resource
      .make(IO.blocking {
        val clientConfig = redis.clients.jedis.DefaultJedisClientConfig
          .builder()
          .password(config.redisPassword)
          .build()
        new JedisPooled(new redis.clients.jedis.HostAndPort(host, port), clientConfig)
      }) { jedis =>
        IO.blocking(jedis.close())
      }
      .map(jedis => new RedisStatsStore(jedis, config.redisKey, config.redisTtlSeconds))

  private def parseRedisAddr(addr: String): (String, Int) =
    val cleaned = addr.stripPrefix("redis://").stripPrefix("rediss://")
    cleaned.split(':') match
      case Array(h, p) => (h, p.toIntOption.getOrElse(6379))
      case Array(h)    => (h, 6379)
      case _           => (cleaned, 6379)

  def minioResource(config: StatsStoreConfig): Resource[IO, MinioStatsStore] =
    Resource
      .make(
        IO.blocking(
          MinioClient
            .builder()
            .endpoint(config.minioEndpoint)
            .credentials(config.minioAccessKey, config.minioSecretKey)
            .region(config.minioRegion)
            .build()
        )
      )(client => IO.blocking(client.close()))
      .evalTap { client =>
        IO.blocking {
          val exists = client.bucketExists(BucketExistsArgs.builder().bucket(config.minioBucket).build())
          if !exists then
            try client.makeBucket(MakeBucketArgs.builder().bucket(config.minioBucket).build())
            catch
              case NonFatal(_) => ()
        }
      }
      .map(client => new MinioStatsStore(client, config.minioBucket, config.minioPrefix))
