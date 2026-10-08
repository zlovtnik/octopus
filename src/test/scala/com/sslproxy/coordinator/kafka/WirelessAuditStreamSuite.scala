package com.sslproxy.coordinator.kafka

import com.sslproxy.coordinator.domain.DatabaseError
import munit.FunSuite

class WirelessAuditStreamSuite extends FunSuite:
  test("coordinate payload-hash conflicts stay fail-closed"):
    val cause = IllegalStateException("wireless broker coordinate payload hash conflict")
    val error = DatabaseError.Permanent("postgres.project_wireless_stream", cause, cause.getMessage)
    assert(WirelessAuditStream.isCoordinateConflict(error))

  test("coordinate conflicts nested in wrappers stay fail-closed"):
    val cause = RuntimeException("wrapped", IllegalStateException("wireless broker coordinate payload hash conflict"))
    val error = DatabaseError.Permanent("postgres.project_wireless_stream", cause, "wrapped")
    assert(WirelessAuditStream.isCoordinateConflict(error))

  test("other permanent storage failures are not coordinate conflicts"):
    val error = DatabaseError.Permanent(
      "postgres.project_wireless_stream",
      java.sql.SQLException("constraint violation", "23505"),
      "constraint violation"
    )
    assert(!WirelessAuditStream.isCoordinateConflict(error))

  test("retryable storage failures are not coordinate conflicts"):
    val error = DatabaseError.Retryable(
      "postgres.project_wireless_stream",
      java.sql.SQLException("connection reset", "08006"),
      "connection reset"
    )
    assert(!WirelessAuditStream.isCoordinateConflict(error))
