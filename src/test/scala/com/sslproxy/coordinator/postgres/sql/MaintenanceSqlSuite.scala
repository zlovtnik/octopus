package com.sslproxy.coordinator.postgres.sql

import munit.FunSuite

class MaintenanceSqlSuite extends FunSuite:
  test("archive candidates exclude quarantined integrity failures"):
    val statement = MaintenanceSql.archiveCandidates(7, 100).sql

    assert(statement.contains("status <> 'failed'"), statement)

  test("archive quarantine records a terminal event failure"):
    val statement = MaintenanceSql.quarantineArchiveCandidate(
      com.sslproxy.coordinator.postgres.ArchiveCandidate(
        "00" * 32,
        "wireless.audit",
        java.sql.Timestamp.from(java.time.Instant.EPOCH),
        "{}",
        "11" * 32
      ),
      "hash mismatch"
    ).sql

    assert(statement.contains("status = 'failed'"), statement)
    assert(statement.contains("attempt_count = attempt_count + 1"), statement)

  test("retention candidates require archived terminal events and terminal dependent work"):
    val statement = MaintenanceSql.retentionCandidates(30, 100).sql

    assert(statement.contains("payload_archived = true"), statement)
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
