package com.sslproxy.coordinator.dq

import cats.effect.IO
import doobie.{ConnectionIO, Transactor}
import doobie.implicits.*
import io.circe.Json
import com.sslproxy.coordinator.postgres.sql.DqControlSql

/** Durable control boundary shared by HTTP and Octopus's supervised runtime. */
final class DqStore(val xa: Transactor[IO], val settings: DqSettings):
  private val sql = new DqControlSql(settings)
  private def commit[A](operation: => ConnectionIO[A]): IO[A] =
    IO.defer(operation.transact(xa))
  def catalog(principal: DqPrincipal): IO[Json] = commit(sql.catalog(principal))
  def invocation(principal: DqPrincipal, key: String): IO[Json] = commit(sql.invocation(principal,key))
  def manifest(
      principal: DqPrincipal,
      processorId: String,
      version: String
  ): IO[Json] =
    IO(
      principal.requireRole(
        Set("viewer", "operator", "admin", "automation", "service")
      )
    ) *>
      commit(sql.manifest(processorId, version))
  def register(principal: DqPrincipal, request: Json): IO[Json] = commit(
    sql.register(principal, request)
  )
  def admit(principal: DqPrincipal, request: Json): IO[Json] = commit(
    sql.admit(principal, request)
  )
  def getRun(principal: DqPrincipal, runId: String): IO[Json] = commit(
    sql.getRun(principal, runId)
  )
  def cancel(principal: DqPrincipal, runId: String): IO[Json] = commit(
    sql.cancel(principal, runId)
  )
  def replay(principal: DqPrincipal, runId: String, request: Json): IO[Json] =
    commit(sql.replay(principal, runId, request))
  def heartbeat(request: Json): IO[Json] = commit(sql.heartbeat(request))
  def dispatches: IO[Json] = commit(sql.dispatches)
  def reserve(runId: String, request: Json): IO[Json] = commit(
    sql.reserve(runId, request)
  )
  def accepted(attemptId: String, request: Json): IO[Json] = commit(
    sql.lease(attemptId, request, true)
  )
  def renew(attemptId: String, request: Json): IO[Json] = commit(
    sql.lease(attemptId, request, false)
  )
  def receiveReceipt(request: Json): IO[Json] = commit(sql.receipt(request))
  def receiveReceipt(principal: DqPrincipal, request: Json): IO[Json] =
    IO(principal.requireRole(Set("service"))) *> receiveReceipt(request)
  def artifact(
      principal: DqPrincipal,
      artifactId: String,
      access: Boolean
  ): IO[Json] = commit(sql.artifact(principal, artifactId, access))
  def claimPreparation(
      owner: String,
      leaseSeconds: Int,
      limit: Int
  ): IO[List[Json]] = commit(sql.claimPreparation(owner, leaseSeconds, limit))
  def completePreparation(
      runId: String,
      owner: String,
      inputRefs: Json,
      sourceRevision: String
  ): IO[Boolean] =
    commit(sql.completePreparation(runId, owner, inputRefs, sourceRevision))
  def failPreparation(
      runId: String,
      owner: String,
      code: String,
      permanent: Boolean
  ): IO[Unit] =
    commit(sql.failWork(runId, owner, code, permanent, true))
  def claimOutput(
      owner: String,
      leaseSeconds: Int,
      limit: Int
  ): IO[List[Json]] = commit(sql.claimOutput(owner, leaseSeconds, limit))
  def guardOutputTx(
      runId: String,
      attemptId: String,
      fence: Long,
      owner: String,
      sourceRevision: String
  ): ConnectionIO[Boolean] =
    sql.guardOutput(runId, attemptId, fence, owner, sourceRevision)
  def completeOutputTx(
      runId: String,
      attemptId: String,
      fence: Long,
      owner: String,
      sourceRevision: String,
      applied: Boolean,
      summary: Json
  ): ConnectionIO[Boolean] = sql.completeOutput(
    runId,
    attemptId,
    fence,
    owner,
    sourceRevision,
    applied,
    summary
  )
  def failOutput(
      runId: String,
      owner: String,
      code: String,
      permanent: Boolean,
      needsPreparation: Boolean
  ): IO[Unit] =
    commit(sql.failWork(runId, owner, code, permanent, needsPreparation))
  def expireAttempts: IO[Int] = commit(sql.expireAttempts)
  def reconcilePolicies: IO[Unit] = commit(sql.reconcilePolicies)
  def claimOutbox(
      owner: String,
      leaseSeconds: Int,
      limit: Int
  ): IO[List[DqOutbox]] = commit(sql.claimOutbox(owner, leaseSeconds, limit))
  def acknowledgeOutbox(item: DqOutbox): IO[Boolean] = commit(
    sql.acknowledgeOutbox(item)
  )
  def failOutbox(item: DqOutbox): IO[Unit] = commit(sql.failOutbox(item))
