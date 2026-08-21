package com.sslproxy.coordinator.archive

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.sslproxy.coordinator.tidb.ArchiveCandidate
import com.sslproxy.coordinator.util.Sha256Utils
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant

class MinioPayloadArchiveSuite extends CatsEffectSuite:
  test("object keys are deterministic and partitioned by UTC date"):
    val candidate = ArchiveCandidate(
      "00" * 32,
      "wireless.audit",
      Timestamp.from(Instant.parse("2026-08-03T23:59:59Z")),
      "{}",
      "00" * 32
    )
    assertEquals(
      MinioPayloadArchive.objectKey(candidate),
      s"wireless/2026/08/03/${"00" * 32}.json"
    )

  test("object keys reject non-hash dedupe keys"):
    val result = Either.catchNonFatal(
      MinioPayloadArchive.objectKey(
        validCandidate("invalid-key", "{}").copy(dedupeKey = "../escape")
      )
    )
    assert(result.left.exists(_.isInstanceOf[IllegalArgumentException])
    )

  test("bucket provisioning ignores only a bucket already owned by this principal"):
    assert(MinioPayloadArchive.isBucketAlreadyOwnedByCaller("BucketAlreadyOwnedByYou"))
    assert(!MinioPayloadArchive.isBucketAlreadyOwnedByCaller("BucketAlreadyExists"))

  test("a duplicate archive reuses verified content without another upload"):
    for
      store <- MemoryStore.create()
      archive = new HashVerifiedPayloadArchive[IO](store, "archive")
      candidate = validCandidate("duplicate", "{\"kind\":\"probe\"}")
      first <- archive.archive(candidate)
      second <- archive.archive(candidate)
      puts <- store.putCount
    yield
      assertEquals(second, first)
      assertEquals(puts, 1)

  test("a payload hash mismatch fails before object-store I/O"):
    for
      store <- MemoryStore.create()
      archive = new HashVerifiedPayloadArchive[IO](store, "archive")
      result <- archive.archive(validCandidate("bad-hash", "{}").copy(payloadSha256 = "00" * 32)).attempt
      puts <- store.putCount
    yield
      assert(result.left.exists(_.isInstanceOf[IllegalArgumentException]))
      assertEquals(puts, 0)

  test("post-upload metadata failure fails closed"):
    for
      store <- MemoryStore.create(corruptMetadata = true)
      archive = new HashVerifiedPayloadArchive[IO](store, "archive")
      result <- archive.archive(validCandidate("bad-metadata", "{}")).attempt
    yield assert(result.left.exists(_.isInstanceOf[IllegalStateException]))

  test("an existing object without hash metadata is safely replaced and verified"):
    for
      store <- MemoryStore.create()
      archive = new HashVerifiedPayloadArchive[IO](store, "archive")
      candidate = validCandidate("missing-metadata", "{\"frame\":1}")
      _ <- store.seed(
        MinioPayloadArchive.objectKey(candidate),
        StoredArchiveObject(candidate.payload.getBytes(StandardCharsets.UTF_8).length.toLong, None)
      )
      receipt <- archive.archive(candidate)
      stored <- store.objects
      puts <- store.putCount
    yield
      assertEquals(stored(MinioPayloadArchive.objectKey(candidate)).payloadSha256, Some(receipt.payloadSha256))
      assertEquals(puts, 1)

  test("concurrent archival converges on one deterministic object"):
    for
      store <- MemoryStore.create()
      archive = new HashVerifiedPayloadArchive[IO](store, "archive")
      candidate = validCandidate("concurrent", "{\"frame\":1}")
      receipts <- (archive.archive(candidate), archive.archive(candidate)).parTupled
      objects <- store.objects
    yield
      assertEquals(receipts._1, receipts._2)
      assertEquals(objects.keySet, Set(MinioPayloadArchive.objectKey(candidate)))

  private def validCandidate(key: String, payload: String): ArchiveCandidate =
    ArchiveCandidate(
      Sha256Utils.sha256Hex(
      key.getBytes(StandardCharsets.UTF_8)),
      "wireless.audit",
      Timestamp.from(Instant.parse("2026-08-03T23:59:59Z")),
      payload,
      Sha256Utils.sha256Hex(payload.getBytes(StandardCharsets.UTF_8))
    )

  private final class MemoryStore(
      state: Ref[IO, Map[String, StoredArchiveObject]],
      puts: Ref[IO, Int],
      corruptMetadata: Boolean
  ) extends ArchiveObjectStore[IO]:
    def stat(objectKey: String): IO[Option[StoredArchiveObject]] =
      state.get.map(_.get(objectKey))

    def put(objectKey: String, bytes: Array[Byte], payloadSha256: String): IO[Unit] =
      puts.update(_ + 1) *>
        state.update(_.updated(
          objectKey,
          StoredArchiveObject(
            bytes.length.toLong,
            Option.unless(corruptMetadata)(payloadSha256)
          )
        ))

    def putCount: IO[Int] = puts.get
    def objects: IO[Map[String, StoredArchiveObject]] = state.get
    def seed(objectKey: String, value: StoredArchiveObject): IO[Unit] =
      state.update(_.updated(objectKey, value))

  private object MemoryStore:
    def create(corruptMetadata: Boolean = false): IO[MemoryStore] =
      for
        state <- Ref.of[IO, Map[String, StoredArchiveObject]](Map.empty)
        puts <- Ref.of[IO, Int](0)
      yield new MemoryStore(state, puts, corruptMetadata)
