package com.sslproxy.coordinator.postgres

import cats.data.EitherT
import cats.effect.IO
import com.sslproxy.coordinator.domain.BrokerRecordMetadata
import com.sslproxy.coordinator.persistence.{DbResultT, ResultStore}

final class PostgresResultStore(repository: PostgresRepository) extends ResultStore[IO]:
  def recordResultWithEvidence(
    result: PostgresResult,
    metadata: BrokerRecordMetadata
  ): DbResultT[IO, Unit] =
    EitherT(repository.recordResultWithEvidence(result, metadata))
  def recordLoadResultsWithEvidence(
      records: List[(PostgresLoad, PostgresResult, BrokerRecordMetadata)]
  ): DbResultT[IO, Unit] =
    EitherT(repository.recordLoadResultsWithEvidence(records))

  def recordResultsWithEvidence(
      records: List[(PostgresResult, BrokerRecordMetadata)]
  ): DbResultT[IO, Unit] =
    EitherT(repository.recordResultsWithEvidence(records))
