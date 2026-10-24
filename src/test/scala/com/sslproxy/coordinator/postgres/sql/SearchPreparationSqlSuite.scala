package com.sslproxy.coordinator.postgres.sql

import com.sslproxy.coordinator.processor.SearchDocumentKind
import munit.FunSuite

class SearchPreparationSqlSuite extends FunSuite:
  test("all durable search kinds have document and embedding preparation paths"):
    val kinds = SearchPreparationSql.supportedKinds

    assertEquals(
      kinds.map(_.sourceKind).toSet,
      Set(
        "event",
        "device",
        "behaviour_window",
        "frame_sequence",
        "device_profile",
        "ap_profile",
        "identity_summary",
        "observation_window"
      )
    )
    assertEquals(kinds.map(_.embeddingKind).toSet, Set("event", "device", "behaviour", "sequence"))

    val candidateSql = kinds.map(kind => kind -> SearchPreparationSql.candidates(kind, 10).sql).toMap
    assert(candidateSql(SearchDocumentKind.Event).contains("FROM wireless_frames"))
    assert(candidateSql(SearchDocumentKind.Device).contains("atheros_search.devices"))
    assert(candidateSql(SearchDocumentKind.Behaviour).contains("atheros_search.behaviour_snapshots"))
    assert(candidateSql(SearchDocumentKind.Sequence).contains("atheros_search.frame_sequences"))
    assert(!kinds.exists(_.sourceTable == "proxy_events"))
    assert(candidateSql(SearchDocumentKind.DeviceProfile).contains("wireless_topology_nodes"))
    assert(candidateSql(SearchDocumentKind.ApProfile).contains("wireless_topology_nodes"))
    assert(candidateSql(SearchDocumentKind.IdentitySummary).contains("identity_clusters"))
    assert(candidateSql(SearchDocumentKind.ObservationWindow).contains("wireless_observation_summaries"))

  test("embedding scans are scoped to the document kind"):
    SearchPreparationSql.supportedKinds.foreach { kind =>
      val statement = SearchPreparationSql.documentsMissingEmbeddingJobs(kind, "model", 10).sql
      assert(statement.contains("document.source_kind = ?"))
      assert(statement.contains("job.embedding_kind = ?"))
    }

  test("superseded documents cancel only their own unfinished embedding jobs"):
    val statement = SearchPreparationSql.cancelSupersededEmbeddingJobsUpdate(List("doc-1", "doc-2")).sql
    assert(statement.contains("status = 'cancelled'"))
    assert(statement.contains("status IN ('pending', 'leased')"))
    assert(statement.contains("owner_id = NULL"))
    assert(statement.contains("document_id"))
