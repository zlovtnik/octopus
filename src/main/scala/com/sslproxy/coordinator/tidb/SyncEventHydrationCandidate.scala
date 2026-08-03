package com.sslproxy.coordinator.tidb

import java.sql.Timestamp

final case class SyncEventHydrationCandidate(
    dedupeKey: String,
    streamName: String,
    observedAt: Timestamp,
    payloadRef: String,
    payloadJson: Option[String],
    payloadSha256: Option[String] = None
)
