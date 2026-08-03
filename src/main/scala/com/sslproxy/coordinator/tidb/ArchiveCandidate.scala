package com.sslproxy.coordinator.tidb

final case class ArchiveCandidate(
    dedupeKey: String,
    streamName: String,
    observedAt: java.sql.Timestamp,
    payload: String,
    payloadSha256: String
)
