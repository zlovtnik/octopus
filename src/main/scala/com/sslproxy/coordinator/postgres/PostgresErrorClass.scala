package com.sslproxy.coordinator.postgres

import java.sql.{SQLException, SQLRecoverableException, SQLTransientException}
import scala.collection.mutable

enum PostgresErrorClass(val wireValue: String):
  case Retryable extends PostgresErrorClass("retryable")
  case Permanent extends PostgresErrorClass("permanent")

object PostgresErrorClass:

  def classify(failure: Throwable): PostgresErrorClass =
    if failure == null then return Permanent

    val failures = mutable.ArrayDeque[Throwable](failure)
    val visited = mutable.Set.empty[Throwable]

    while failures.nonEmpty do
      val current = failures.removeHead()
      if !visited.add(current) then ()
      else
        current match
          case _: SQLRecoverableException | _: SQLTransientException =>
            return Retryable
          case sqlEx: SQLException =>
            if isRetryableSqlState(sqlEx.getSQLState) || isRetryableVendorCode(sqlEx.getErrorCode) then return Retryable
            if sqlEx.getNextException != null then failures += sqlEx.getNextException
          case _ => ()

        if isRetryableMessage(current.getMessage) then return Retryable

        val cause = current.getCause
        if cause != null && cause != current then failures += cause

    Permanent

  private def isRetryableSqlState(sqlState: String): Boolean =
    sqlState != null && {
      val normalized = sqlState.toUpperCase(java.util.Locale.ROOT)
      normalized.startsWith("08") ||
      normalized.startsWith("40") ||
      normalized == "HYT00" ||
      normalized == "HYT01" ||
      normalized == "55P03" ||
      normalized == "57P03"
    }

  private def isRetryableVendorCode(_errorCode: Int): Boolean =
    false

  private def isRetryableMessage(message: String): Boolean =
    val normalized = if message == null then "" else message.toLowerCase(java.util.Locale.ROOT)
    normalized.contains("timeout") ||
    normalized.contains("temporarily unavailable") ||
    normalized.contains("connection reset") ||
    normalized.contains("deadlock") ||
    normalized.contains("could not serialize") ||
    normalized.contains("lock not available")
