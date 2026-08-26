package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

import java.nio.file.{Files, Paths}

class IdentityGraphSqlSuite extends FunSuite:
  test("graph upserts qualify retained target columns"):
    val implementation = Files.readString(
      Paths.get("src/main/scala/com/sslproxy/coordinator/postgres/sql/IdentityGraphSql.scala")
    )

    assert(implementation.contains("graph_nodes.observed_at"))
    assert(implementation.contains("graph_edges.observed_at"))
    assert(implementation.contains("graph_nodes.location_id"))
    assert(!implementation.contains("COALESCE(observed_at, EXCLUDED.observed_at)"))
    assert(!implementation.contains("COALESCE(EXCLUDED.observed_at, observed_at)"))
