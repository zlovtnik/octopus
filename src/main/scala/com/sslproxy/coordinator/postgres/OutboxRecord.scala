package com.sslproxy.coordinator.postgres

final case class LeaseIdentity(
    ownerId: String,
    token: String,
    fence: Long
)

final case class OutboxRecord(
    outboxId: String,
    destinationTopic: String,
    messageKey: String,
    payload: String,
    attemptCount: Int,
    maxAttempts: Int,
    lease: LeaseIdentity
)

enum OutboxFailureDisposition:
  case RetryScheduled
  case Parked

object LeaseSql:
  /** Shared eligibility predicate used by claim implementations. It is kept in
    * one place so a future repository cannot accidentally re-introduce
    * `SKIP LOCKED` or claim work before its retry time.
    */
  val EligibilityPredicate: String =
    "next_attempt_at <= CURRENT_TIMESTAMP AND " +
      "(lease_expires_at IS NULL OR lease_expires_at <= CURRENT_TIMESTAMP)"

  def retryDelaySeconds(attempt: Int, baseSeconds: Int, maxSeconds: Int): Int =
    val safeAttempt = attempt.max(1).min(30)
    val multiplier = 1L << (safeAttempt - 1)
    Math.min(Math.max(1, maxSeconds).toLong, Math.max(1, baseSeconds).toLong * multiplier).toInt
