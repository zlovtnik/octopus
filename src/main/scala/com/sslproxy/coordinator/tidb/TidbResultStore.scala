package com.sslproxy.coordinator.tidb

import cats.data.EitherT
import cats.effect.IO
import com.sslproxy.coordinator.domain.BrokerRecordMetadata
import com.sslproxy.coordinator.persistence.{DbResultT, ResultStore}

final class TidbResultStore(repository: TidbRepository) extends ResultStore[IO]:
  def recordResultWithEvidence(
    result: TidbResult,
    metadata: BrokerRecordMetadata
  ): DbResultT[IO, Unit] =
    EitherT(repository.recordResultWithEvidence(result, metadata))
  def recordLoadResultsWithEvidence(
      records: List[(TidbLoad, TidbResult, BrokerRecordMetadata)]
  ): DbResultT[IO, Unit] =
    EitherT(repository.recordLoadResultsWithEvidence(records))

  def recordResultsWithEvidence(
      records: List[(TidbResult, BrokerRecordMetadata)]
  ): DbResultT[IO, Unit] =
    EitherT(repository.recordResultsWithEvidence(records))
