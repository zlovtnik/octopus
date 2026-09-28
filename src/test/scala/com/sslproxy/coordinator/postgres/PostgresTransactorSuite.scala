package com.sslproxy.coordinator.postgres

import com.sslproxy.coordinator.config.AppConfig
import io.circe.parser.parse
import munit.FunSuite

import java.sql.{BatchUpdateException, SQLException, Statement}

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

  test("wireless alert identities are deterministic and type scoped"):
    val source = PostgresTransactor.wirelessAlertSourceEventId("batch-1", 7L)

    assertEquals(source, PostgresTransactor.wirelessAlertSourceEventId("batch-1", 7L))
    assertNotEquals(
      PostgresTransactor.wirelessAlertId(source, "rogue_ap"),
      PostgresTransactor.wirelessAlertId(source, "signal_anomaly")
    )

  test("wireless canonical evidence retains common and type-specific fields"):
    val evidence = PostgresTransactor.wirelessAlertEvidence(
      "signal_anomaly",
      "sensor-1",
      "lab",
      Some("02:00:00:00:00:01"),
      None,
      Some("network"),
      Some(-42L),
      """{"dbm_delta":20}"""
    )
    val json = parse(evidence).fold(error => fail(error.message), identity)

    assertEquals(json.hcursor.get[String]("sensor_id"), Right("sensor-1"))
    assertEquals(json.hcursor.downField("details").get[Long]("dbm_delta"), Right(20L))

  test("wireless alert subjects fall back to the sensor when identifiers are absent"):
    assertEquals(PostgresTransactor.alertSubject(None, "sensor-1"), "sensor" -> "sensor-1")

  test("outbox retry delay clamps a non-positive maximum to one second"):
    assertEquals(LeaseSql.retryDelaySeconds(attempt = 3, baseSeconds = 5, maxSeconds = 0), 1)
