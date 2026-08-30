package com.sslproxy.coordinator.postgres

import cats.data.EitherT
import cats.effect.IO
import com.sslproxy.coordinator.persistence.{DbResultT, ProjectionStore}

final class PostgresProjectionStore(repository: PostgresRepository) extends ProjectionStore[IO]:
  def generateRfAlerts(limit: Int): DbResultT[IO, List[String]] =
    EitherT(repository.generateShadowAlerts(limit))

  def normalizeWirelessFrames(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.normalizeWirelessFrames(limit))

  def projectWirelessInventory(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectWirelessInventory(limit))

  def buildSearchDocuments(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.buildSearchDocuments(limit))

  def prepareEmbeddingJobs(limit: Int, embeddingModel: String): DbResultT[IO, Int] =
    EitherT(repository.prepareEmbeddingJobs(limit, embeddingModel))

  def projectBehavior(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectBehavior(limit))

  def projectTiming(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectTiming(limit))

  def projectSequences(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectSequences(limit))

  def projectBaselines(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectBaselines(limit))

  def projectSimilarities(
    limit: Int,
    eventDuplicateDistance: Double,
    behaviorSimilarityThreshold: Double,
    sequenceDistanceThreshold: Double
  ): DbResultT[IO, Int] =
    EitherT(
      repository.projectSimilarities(
        limit,
        eventDuplicateDistance,
        behaviorSimilarityThreshold,
        sequenceDistanceThreshold
      )
    )

  def projectClusterCandidates(limit: Int, minimumSimilarity: Double): DbResultT[IO, Int] =
    EitherT(repository.projectClusterCandidates(limit, minimumSimilarity))

  def projectApprovedIdentities(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectApprovedIdentities(limit))

  def projectInfrastructureGraph(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectInfrastructureGraph(limit))

  def projectDnsThreats(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectDnsThreats(limit))

  def projectRisk(limit: Int): DbResultT[IO, Int] =
    EitherT(repository.projectRisk(limit))
