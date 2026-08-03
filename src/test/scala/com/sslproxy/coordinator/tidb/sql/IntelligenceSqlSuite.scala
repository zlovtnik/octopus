package com.sslproxy.coordinator.tidb.sql

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

  test("dynamic similarity identifiers remain closed by VectorKind"):
    IntelligenceSql.VectorKind.values.foreach { kind =>
      val statement = IntelligenceSql.similarityCandidates(kind, 0.2d, 10).sql
      assert(statement.contains(s"atheros_search.${kind.table}"))
      assert(statement.contains("VEC_COSINE_DISTANCE"))
    }
