package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

class MaintenanceSqlSuite extends FunSuite:
  test("retention candidates require archived terminal events and terminal dependent work"):
    val statement = MaintenanceSql.retentionCandidates(30, 100).sql

    assert(statement.contains("payload_archived = 1"), statement)
    assert(statement.contains("sync_event_payload_archives"), statement)
    assert(statement.contains("job.status NOT IN"), statement)
    assert(statement.contains("batch.status NOT IN"), statement)
    assert(statement.contains("outbox.status NOT IN"), statement)
    assert(statement.contains("LIMIT ?"), statement)

  test("tombstone pruning uses the persisted absolute expiry and a bound limit"):
    val statement = MaintenanceSql.pruneTombstones(100).sql

    assert(statement.contains("expires_at < CURRENT_TIMESTAMP"), statement)
    assert(!statement.contains("TIMESTAMPADD"), statement)
    assert(statement.contains("LIMIT ?"), statement)
