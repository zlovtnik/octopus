package com.sslproxy.coordinator.postgres

import java.sql.{SQLException, SQLRecoverableException, SQLTransientException}
import scala.collection.mutable

enum PostgresErrorClass(val wireValue: String):
  case Retryable extends PostgresErrorClass("retryable")
  case Permanent extends PostgresErrorClass("permanent")

object PostgresErrorClass:

  def classify(failure: Throwable): PostgresErrorClass =
    val chain = exceptions(failure)
    val states = chain.collect { case sql: SQLException => Option(sql.getSQLState).filter(_.nonEmpty) }.flatten
    if states.nonEmpty then
      if states.exists(isRetryableSqlState) then Retryable else Permanent
    else if chain.exists {
      case _: SQLRecoverableException | _: SQLTransientException => true
      case error => isRetryableMessage(error.getMessage)
    } then Retryable
    else Permanent

  def exceptions(failure: Throwable, includeSuppressed: Boolean = false): List[Throwable] =
    if failure == null then return Nil
    val failures = mutable.ArrayDeque[Throwable](failure)
    val visited = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap[Throwable, java.lang.Boolean]())
    val result = List.newBuilder[Throwable]

    while failures.nonEmpty do
      val current = failures.removeHead()
      if !visited.add(current) then ()
      else
        result += current
        current match
          case sqlEx: SQLException =>
            if sqlEx.getNextException != null then failures += sqlEx.getNextException
          case _ => ()
        if includeSuppressed then current.getSuppressed.foreach(failures += _)
        val cause = current.getCause
        if cause != null && cause != current then failures += cause

    result.result()

  private def isRetryableSqlState(sqlState: String): Boolean =
    sqlState != null && {
      val normalized = sqlState.toUpperCase(java.util.Locale.ROOT)
      normalized.startsWith("08") ||
      normalized.startsWith("40") ||
      normalized == "HYT00" ||
      normalized == "HYT01" ||
      normalized == "55P03" ||
      normalized == "57014" ||
      normalized == "57P03"
    }

  private def isRetryableMessage(message: String): Boolean =
    val normalized = if message == null then "" else message.toLowerCase(java.util.Locale.ROOT)
    normalized.contains("timeout") ||
    normalized.contains("temporarily unavailable") ||
    normalized.contains("connection reset") ||
    normalized.contains("deadlock") ||
    normalized.contains("could not serialize") ||
    normalized.contains("lock not available")
