package com.sslproxy.coordinator.domain

/** Immutable broker coordinates joined to the signed cutover artifact for
  * durable no-replay evidence. Kafka offsets are the offset of the record,
  * while cutover offsets are the first record allowed for the new group.
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
