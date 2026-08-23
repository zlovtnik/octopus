package com.sslproxy.coordinator.postgres

import com.sslproxy.coordinator.config.AppConfig
import io.circe.parser.parse
import java.sql.{BatchUpdateException, SQLException, Statement}
import munit.FunSuite

class PostgresTransactorSuite extends FunSuite:

  test("validateBatchResults accepts known JDBC update counts"):
    PostgresTransactor.validateBatchResults(Array(1, 2, 0))

  test("validateBatchResults accepts successful batches with unknown update counts"):
    PostgresTransactor.validateBatchResults(Array(4, Statement.SUCCESS_NO_INFO, 0))

  test("validateBatchResults fails when JDBC reports EXECUTE_FAILED"):
    val error = intercept[BatchUpdateException] {
      PostgresTransactor.validateBatchResults(Array(1, Statement.EXECUTE_FAILED))
    }

    assertEquals(error.getUpdateCounts.toList, List(1, Statement.EXECUTE_FAILED))

  test("validateBatchResults fails for unsupported negative JDBC update counts"):
    intercept[SQLException] {
      PostgresTransactor.validateBatchResults(Array(-4))
    }

  test("disabled TLS uses the PostgreSQL disable mode"):
    val config = AppConfig.load.postgres.copy(sslMode = "disable")

    val url = PostgresTransactor.jdbcUrl(config)

    assert(url.contains("sslmode=disable"))

  test("secure JDBC URLs always pin full identity verification"):
    val config = AppConfig.load.postgres.copy(sslMode = "require")

    val url = PostgresTransactor.jdbcUrl(config)

    assert(url.contains("sslmode=verify-full"))
    assert(!url.contains("sslmode=require"))

  test("serialized alert arrays remain JSON arrays in details"):
    val details = PostgresTransactor.jsonDetails(
      "attack_chain" -> PostgresTransactor.parsedJson(Some("[\"deauth\"]")),
      "explanation" -> PostgresTransactor.parsedJson(Some("[\"burst\"]"))
    )
    val json = parse(details).fold(error => fail(error.message), identity)

    assertEquals(
      json.hcursor.downField("attack_chain").downArray.as[String],
      Right("deauth")
    )

  test("outbox retry delay clamps a non-positive maximum to one second"):
    assertEquals(LeaseSql.retryDelaySeconds(attempt = 3, baseSeconds = 5, maxSeconds = 0), 1)
