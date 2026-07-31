package com.sslproxy.coordinator.tidb

import com.sslproxy.coordinator.config.AppConfig
import java.sql.{BatchUpdateException, SQLException, Statement}
import munit.FunSuite

class TidbTransactorSuite extends FunSuite:

  test("validateBatchResults accepts known JDBC update counts"):
    TidbTransactor.validateBatchResults(Array(1, 2, 0))

  test("validateBatchResults accepts successful batches with unknown update counts"):
    TidbTransactor.validateBatchResults(Array(4, Statement.SUCCESS_NO_INFO, 0))

  test("validateBatchResults fails when JDBC reports EXECUTE_FAILED"):
    val error = intercept[BatchUpdateException] {
      TidbTransactor.validateBatchResults(Array(1, Statement.EXECUTE_FAILED))
    }

    assertEquals(error.getUpdateCounts.toList, List(1, Statement.EXECUTE_FAILED))

  test("validateBatchResults fails for unsupported negative JDBC update counts"):
    intercept[SQLException] {
      TidbTransactor.validateBatchResults(Array(-4))
    }

  test("disabled TLS does not allow public key retrieval by default"):
    val config = AppConfig.load.tidb.copy(sslMode = "DISABLED")

    val url = TidbTransactor.jdbcUrl(config)

    assert(url.contains("useSSL=false"))
    assert(!url.contains("allowPublicKeyRetrieval=true"))

  test("disabled TLS allows explicit local-development public key retrieval"):
    val config = AppConfig.load.tidb.copy(
      sslMode = "DISABLED",
      localDevAllowPublicKeyRetrieval = true
    )

    assert(TidbTransactor.jdbcUrl(config).contains("allowPublicKeyRetrieval=true"))

  test("secure JDBC URLs always pin identity verification"):
    val config = AppConfig.load.tidb.copy(sslMode = "REQUIRED")

    val url = TidbTransactor.jdbcUrl(config)

    assert(url.contains("sslMode=VERIFY_IDENTITY"))
    assert(!url.contains("sslMode=REQUIRED"))
