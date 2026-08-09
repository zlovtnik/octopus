package com.sslproxy.coordinator.tidb

final case class SchemaReadiness(
    requiredVersion: String,
    appliedVersion: Option[String],
    requiredChecksum: String,
    appliedChecksum: Option[String],
    ready: Boolean
)
