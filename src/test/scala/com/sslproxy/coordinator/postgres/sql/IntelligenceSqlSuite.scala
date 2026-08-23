package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

class IntelligenceSqlSuite extends FunSuite:
  test("maintained window candidates compare authoritative source counts"):
    val behavior = IntelligenceSql.behaviorCandidates(10).sql
    val timing = IntelligenceSql.timingCandidates(10).sql
    val sequence = IntelligenceSql.sequenceCandidates(10).sql
    val baseline = IntelligenceSql.baselineCandidates(10).sql

    assert(behavior.contains("snapshot.event_count <> source.source_event_count"))
    assert(timing.contains("profile.source_event_count <> source.source_event_count"))
    assert(sequence.contains("sequence_row.frame_count <> source.source_event_count"))
    assert(baseline.contains("baseline.sample_count <> source.source_event_count"))
    List(behavior, timing, sequence, baseline).foreach { statement =>
      assert(statement.contains("LIMIT ?"), statement)
      assert(!statement.trim.endsWith("LIMIT ?"), s"outer LIMIT should be removed: $statement")
    }

  test("dynamic similarity identifiers remain closed by VectorKind"):
    IntelligenceSql.VectorKind.values.foreach { kind =>
      val anchors = IntelligenceSql.similarityAnchors(kind, 10).sql
      val statement = IntelligenceSql.similarityCandidatesForAnchor(kind, "document", "model", "[0.1,0.2]", 0.2d, 10).sql
      assert(anchors.contains(s"atheros_search.${kind.table}"))
      assert(anchors.contains("VEC_AS_TEXT"))
      assert(statement.contains(s"atheros_search.${kind.table}"))
      assert(statement.contains("VEC_COSINE_DISTANCE"))
      assert(!statement.contains("JOIN LATERAL"))
      assert(statement.contains("VEC_FROM_TEXT"))
      assert(statement.contains("ORDER BY VEC_COSINE_DISTANCE"))
      assert(IntelligenceSql.annReady(kind).sql.contains("INFORMATION_SCHEMA.TIFLASH_INDEXES"))
    }
