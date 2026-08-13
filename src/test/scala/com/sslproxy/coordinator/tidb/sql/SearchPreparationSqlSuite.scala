package com.sslproxy.coordinator.tidb.sql

import com.sslproxy.coordinator.processor.SearchDocumentKind
import munit.FunSuite

class SearchPreparationSqlSuite extends FunSuite:
  test("all public search kinds have document and embedding preparation paths"):
    val kinds = SearchPreparationSql.supportedKinds

    assertEquals(kinds.map(_.sourceKind).toSet, Set(
      "event",
      "device",
      "behaviour_window",
      "frame_sequence"
    ))
    assertEquals(kinds.map(_.embeddingKind).toSet, Set("event", "device", "behaviour", "sequence"))

    val candidateSql = kinds.map(kind => kind -> SearchPreparationSql.candidates(kind, 10).sql).toMap
    assert(candidateSql(SearchDocumentKind.Event).contains("FROM wireless_frames"))
    assert(candidateSql(SearchDocumentKind.Device).contains("atheros_search.inventory_devices"))
    assert(candidateSql(SearchDocumentKind.Behaviour).contains("atheros_search.behaviour_snapshots"))
    assert(candidateSql(SearchDocumentKind.Sequence).contains("atheros_search.frame_sequences"))

  test("embedding scans are scoped to the document kind"):
    SearchPreparationSql.supportedKinds.foreach { kind =>
      val statement = SearchPreparationSql.documentsMissingEmbeddingJobs(kind, "model", 10).sql
      assert(statement.contains("document.source_kind = ?"))
      assert(statement.contains("job.embedding_kind = ?"))
    }
