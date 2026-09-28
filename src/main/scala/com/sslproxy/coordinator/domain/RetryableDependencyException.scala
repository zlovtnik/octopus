package com.sslproxy.coordinator.domain

final class RetryableDependencyException(
  val dependency: String,
  val operation: String,
  cause: Throwable
) extends RuntimeException(
      s"$dependency dependency temporarily unavailable during $operation",
      cause
    )
