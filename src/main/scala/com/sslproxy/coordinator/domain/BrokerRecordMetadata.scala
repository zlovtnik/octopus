package com.sslproxy.coordinator.domain

import com.sslproxy.coordinator.util.Sha256Utils

/** Immutable broker coordinates used for durable idempotency evidence. Kafka
  * owns the restart position through committed consumer-group offsets; PostgreSQL
  * independently records every processed broker coordinate.
  *
  * `groupVersion` and `artifactSha256` retain their persisted column names for
  * schema compatibility. They now describe the versioned Kafka consumer
  * contract and do not authorize or restrict offsets.
  */
final case class BrokerRecordMetadata(
  topic: String,
  partition: Int,
  offset: Long,
  consumerGroup: String,
  groupVersion: Int,
  artifactSha256: String,
  messageKey: Option[String],
  payloadSha256: String
)

final case class BrokerConsumerContract(
  groupVersion: Int,
  contractSha256: String
)

object BrokerConsumerContract:
  private val VersionedGroup = "^.*[-_.]v([1-9][0-9]*)$".r

  def from(groupId: String, topic: String): Either[IllegalArgumentException, BrokerConsumerContract] =
    for
      normalizedGroup <- nonBlank(groupId, "consumer group")
      normalizedTopic <- nonBlank(topic, "topic")
      version <- normalizedGroup match
        case VersionedGroup(value) =>
          value.toIntOption
            .filter(_ > 0)
            .toRight(
              IllegalArgumentException(s"consumer group version is out of range: $normalizedGroup")
            )
        case _ =>
          Left(
            IllegalArgumentException(
              s"consumer group must end in a non-zero version suffix such as -v1: $normalizedGroup"
            )
          )
      canonical = lengthPrefixed(normalizedGroup) + lengthPrefixed(normalizedTopic)
    yield BrokerConsumerContract(version, Sha256Utils.sha256Hex(canonical))

  private def nonBlank(value: String, label: String): Either[IllegalArgumentException, String] =
    Option(value)
      .map(_.trim)
      .filter(_.nonEmpty)
      .toRight(
        IllegalArgumentException(s"$label must not be blank")
      )

  private def lengthPrefixed(value: String): String =
    s"${value.length}:$value"

enum IngestionDisposition(val databaseValue: String):
  case Processed extends IngestionDisposition("processed")
  case Deduplicated extends IngestionDisposition("duplicate")
  case Retrying extends IngestionDisposition("retrying")
  case Parked extends IngestionDisposition("parked")
  case Rejected extends IngestionDisposition("rejected")

final case class IngestionDecision(
  disposition: IngestionDisposition,
  dedupeKey: String,
  jobId: String,
  batchId: String
)
