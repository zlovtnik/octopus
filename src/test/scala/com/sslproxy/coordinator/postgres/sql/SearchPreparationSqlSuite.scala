package com.sslproxy.coordinator.postgres.sql

import com.sslproxy.coordinator.processor.SearchDocumentKind
import munit.FunSuite

class SearchPreparationSqlSuite extends FunSuite:
  test("all public search kinds have document and embedding preparation paths"):
    val kinds = SearchPreparationSql.supportedKinds

    assertEquals(
      kinds.map(_.sourceKind).toSet,
      Set(
        "event",
        "device",
        "behaviour_window",
        "frame_sequence",
        "proxy_event",
        "proxy_blocked_host_window"
      )
    )
    assertEquals(kinds.map(_.embeddingKind).toSet, Set("event", "device", "behaviour", "sequence"))

    val candidateSql = kinds.map(kind => kind -> SearchPreparationSql.candidates(kind, 10).sql).toMap
    assert(candidateSql(SearchDocumentKind.Event).contains("FROM wireless_frames"))
    assert(candidateSql(SearchDocumentKind.Device).contains("atheros_search.devices"))
    assert(candidateSql(SearchDocumentKind.Behaviour).contains("atheros_search.behaviour_snapshots"))
    assert(candidateSql(SearchDocumentKind.Sequence).contains("atheros_search.frame_sequences"))
    assert(candidateSql(SearchDocumentKind.ProxyEvent).contains("octopus_core.proxy_events"))
    assert(candidateSql(SearchDocumentKind.ProxyEvent).contains("event.event_id"))
    assert(candidateSql(SearchDocumentKind.ProxyBlockedHostWindow).contains("date_trunc('hour'"))
    assert(candidateSql(SearchDocumentKind.ProxyBlockedHostWindow).contains("event_type_counts"))
    assert(candidateSql(SearchDocumentKind.ProxyBlockedHostWindow).contains("classification_counts"))
    assert(candidateSql(SearchDocumentKind.ProxyBlockedHostWindow).contains("status_code_distribution"))
    assert(candidateSql(SearchDocumentKind.ProxyBlockedHostWindow).contains("IS DISTINCT FROM"))

  test("embedding scans are scoped to the document kind"):
    SearchPreparationSql.supportedKinds.foreach { kind =>
      val statement = SearchPreparationSql.documentsMissingEmbeddingJobs(kind, "model", 10).sql
      assert(statement.contains("document.source_kind = ?"))
      assert(statement.contains("job.embedding_kind = ?"))
    }
