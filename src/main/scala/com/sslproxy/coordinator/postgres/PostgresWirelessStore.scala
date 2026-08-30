package com.sslproxy.coordinator.postgres

import cats.data.EitherT
import cats.effect.IO
import com.sslproxy.coordinator.persistence.{DbResultT, WirelessStore}
import io.circe.Json

import java.time.Instant

final class PostgresWirelessStore(repository: PostgresRepository) extends WirelessStore[IO]:
  def saveBacklog(
    dedupeKey: String,
    streamName: String,
    payload: Json,
    failureStage: String
  ): DbResultT[IO, Unit] =
    EitherT(repository.saveWirelessBacklog(dedupeKey, streamName, payload, failureStage))

  def listPendingBacklog(limit: Int): DbResultT[IO, List[WirelessBacklogEntry]] =
    EitherT(repository.listPendingWirelessBacklog(limit))

  def markBacklogSynced(dedupeKey: String, streamName: String): DbResultT[IO, Boolean] =
    EitherT(repository.markWirelessBacklogSynced(dedupeKey, streamName))

  def pruneBacklog(before: Instant): DbResultT[IO, Int] =
    EitherT(repository.pruneWirelessBacklog(before))

  def lookupDeviceByMac(mac: String): DbResultT[IO, Option[String]] =
    EitherT(repository.lookupDeviceByMac(mac))

  def listAuthorizedNetworks: DbResultT[IO, String] =
    EitherT(repository.listAuthorizedNetworks())

  def flushProbeBatch(probesJson: String): DbResultT[IO, Int] =
    EitherT(repository.flushProbeBatch(probesJson))
