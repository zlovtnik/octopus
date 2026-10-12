package com.sslproxy.coordinator.dq

import io.circe.parser.decode
import pureconfig.ConfigReader

final case class DqConfig(
    enabled: Boolean = false,
    installationId: String = "",
    namespace: String = "",
    scopeModesJson: String = "{}",
    policyVersion: String = "",
    approvedDigestsJson: String = "[]",
    controlToken: String = "",
    artifactBucket: String = "dq-artifacts",
    pollIntervalMs: Long = 1000L,
    batchSize: Int = 8,
    leaseSeconds: Int = 60,
    maxQueued: Int = 10000,
    maxArtifactBytes: Int = 8 * 1024 * 1024
) derives ConfigReader:
  def scopeModes: Map[String, String] =
    decode[Map[String, String]](scopeModesJson).fold(_ => Map.empty, identity)

  def approvedDigests: Set[String] =
    decode[List[String]](approvedDigestsJson).fold(_ => Set.empty, _.toSet)

  def settings: DqSettings = DqSettings(
    installationId,
    namespace,
    if enabled then scopeModes else Map.empty,
    policyVersion,
    approvedDigests,
    maxQueued,
    leaseSeconds,
    artifactBucket
  )

  def errors: List[String] =
    val safeSegment = "^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$".r
    val validScopes = decode[Map[String, String]](scopeModesJson).toOption
    val validDigests = decode[List[String]](approvedDigestsJson).toOption
    List(
      Option.when(enabled && !safeSegment.matches(installationId))(
        "dq.installation-id must be a safe non-empty identifier"
      ),
      Option.when(enabled && !safeSegment.matches(namespace))(
        "dq.namespace must be a safe non-empty identifier"
      ),
      Option.when(enabled && policyVersion.trim.isEmpty)(
        "dq.policy-version must not be blank"
      ),
      Option.when(enabled && controlToken.length < 32)(
        "dq.control-token must contain at least 32 characters"
      ),
      Option.when(
        !"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$".r.matches(artifactBucket)
      )("dq.artifact-bucket must be a valid bucket name"),
      Option.when(validScopes.isEmpty)(
        "dq.scope-modes-json must be a JSON object"
      ),
      Option.when(validScopes.exists(_.exists { case (scope, mode) =>
        (scope != "*" && !safeSegment.matches(scope)) || !Set(
          "shadow",
          "enabled"
        ).contains(mode)
      }))("dq.scope-modes-json contains an invalid scope or mode"),
      Option.when(validScopes.exists(scopes => scopes.contains("*") && scopes.size > 1))(
        "dq.scope-modes-json must not mix wildcard and location scopes"
      ),
      Option.when(enabled && validScopes.forall(_.isEmpty))(
        "dq.scope-modes-json must authorize at least one scope when enabled"
      ),
      Option.when(
        validDigests.isEmpty || validDigests.exists(
          _.exists(value => !"^sha256:[0-9a-f]{64}$".r.matches(value))
        )
      )(
        "dq.approved-digests-json must contain sha256 image digests"
      ),
      Option.when(enabled && validDigests.forall(_.isEmpty))(
        "dq.approved-digests-json must approve at least one image when enabled"
      ),
      Option.when(pollIntervalMs < 100L || pollIntervalMs > 60000L)(
        "dq.poll-interval-ms must be between 100 and 60000"
      ),
      Option.when(batchSize < 1 || batchSize > 100)(
        "dq.batch-size must be between 1 and 100"
      ),
      Option.when(leaseSeconds < 10 || leaseSeconds > 3600)(
        "dq.lease-seconds must be between 10 and 3600"
      ),
      Option.when(maxQueued < 1 || maxQueued > 1000000)(
        "dq.max-queued must be between 1 and 1000000"
      ),
      Option.when(
        maxArtifactBytes < 1024 || maxArtifactBytes > 64 * 1024 * 1024
      )("dq.max-artifact-bytes must be between 1024 and 67108864")
    ).flatten
