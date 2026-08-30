package com.sslproxy.coordinator.postgres

import java.sql.{SQLException, SQLRecoverableException, SQLTransientException}
import munit.*

class PostgresErrorClassSuite extends FunSuite:

  test("classify null as Permanent"):
    assertEquals(PostgresErrorClass.classify(null), PostgresErrorClass.Permanent)

  test("classify IllegalArgumentException as Permanent"):
    assertEquals(PostgresErrorClass.classify(IllegalArgumentException("bad")), PostgresErrorClass.Permanent)

  test("classify SQLRecoverableException as Retryable"):
    assertEquals(PostgresErrorClass.classify(SQLRecoverableException()), PostgresErrorClass.Retryable)

  test("classify SQLTransientException as Retryable"):
    assertEquals(PostgresErrorClass.classify(SQLTransientException()), PostgresErrorClass.Retryable)

  test("classify PostgreSQL connection error 2006 as Retryable"):
    assertEquals(
      PostgresErrorClass.classify(SQLException("PostgreSQL server has gone away", "08S01", 2006)),
      PostgresErrorClass.Retryable
    )

  test("classify PostgreSQL deadlock 1213 as Retryable"):
    assertEquals(
      PostgresErrorClass.classify(SQLException("Deadlock found", "40001", 1213)),
      PostgresErrorClass.Retryable
    )

  test("classify PostgreSQL write conflict 8002 as Retryable"):
    assertEquals(
      PostgresErrorClass.classify(SQLException("Write conflict", "40001", 8002)),
      PostgresErrorClass.Retryable
    )

  test("classify PostgreSQL lock not available as Retryable"):
    assertEquals(
      PostgresErrorClass.classify(SQLException("Lock not available", "55P03", 0)),
      PostgresErrorClass.Retryable
    )

  test("classify non-retryable SQL error as Permanent"):
    assertEquals(PostgresErrorClass.classify(SQLException("Syntax error", "42000", 1064)), PostgresErrorClass.Permanent)

  test("classify chained exception with recoverable at root as Retryable"):
    val root = SQLRecoverableException("connection lost")
    val outer = RuntimeException("wrapping", root)
    assertEquals(PostgresErrorClass.classify(outer), PostgresErrorClass.Retryable)

  test("classify message containing timeout as Retryable"):
    assertEquals(PostgresErrorClass.classify(RuntimeException("query timeout expired")), PostgresErrorClass.Retryable)

  test("classify message containing serialization failure as Retryable"):
    assertEquals(
      PostgresErrorClass.classify(RuntimeException("Could not serialize access")),
      PostgresErrorClass.Retryable
    )
