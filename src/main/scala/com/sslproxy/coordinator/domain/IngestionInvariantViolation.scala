package com.sslproxy.coordinator.domain

/** Contradictory durable evidence requires intervention, never poison parking. */
final class IngestionInvariantViolation(message: String) extends IllegalStateException(message)
