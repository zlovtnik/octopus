package com.sslproxy.coordinator.postgres

import io.circe.Json

import java.time.Instant

final case class WirelessBacklogEntry(
    dedupeKey: String,
    streamName: String,
    payload: Json,
    failureStage: String,
    attemptCount: Int,
    createdAt: Instant
)
