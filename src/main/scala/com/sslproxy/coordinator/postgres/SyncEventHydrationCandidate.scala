package com.sslproxy.coordinator.postgres

import java.sql.Timestamp

final case class SyncEventHydrationCandidate(
    dedupeKey: String,
    streamName: String,
    observedAt: Timestamp,
    payloadRef: String,
    payloadJson: Option[String],
    payloadSha256: Option[String] = None
)

final case class HydrationCursor(
    dedupeKey: String,
    streamName: String,
    observedAt: Timestamp
)
