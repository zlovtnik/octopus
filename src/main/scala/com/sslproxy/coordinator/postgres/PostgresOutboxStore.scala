package com.sslproxy.coordinator.postgres

import cats.data.EitherT
import cats.effect.IO
import com.sslproxy.coordinator.persistence.{DbResultT, OutboxStore}

final class PostgresOutboxStore(repository: PostgresRepository) extends OutboxStore[IO]:
  def claim(
    ownerId: String,
    destinations: List[String],
    leaseSeconds: Int
  ): DbResultT[IO, Option[OutboxRecord]] =
    EitherT(repository.claimOutbox(ownerId, destinations, leaseSeconds))

  def acknowledge(record: OutboxRecord): DbResultT[IO, Boolean] =
    EitherT(repository.acknowledgeOutbox(record))

  def fail(
    record: OutboxRecord,
    error: String,
    retryBaseSeconds: Int,
    retryMaxSeconds: Int
  ): DbResultT[IO, OutboxFailureDisposition] =
    EitherT(repository.failOutbox(record, error, retryBaseSeconds, retryMaxSeconds))

  def recoverExpired: DbResultT[IO, Int] =
    EitherT(repository.recoverExpiredOutboxLeases())
