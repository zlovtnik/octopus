package com.sslproxy.coordinator.processor

enum ProcessorOwner(val value: String):
  case Octopus extends ProcessorOwner("octopus")
  case AtherosSearch extends ProcessorOwner("atheros-search")

enum ProcessorFamily(val value: String):
  case Sync extends ProcessorFamily("sync")
  case Wireless extends ProcessorFamily("wireless")
  case Embedding extends ProcessorFamily("embedding")
  case SearchProjection extends ProcessorFamily("search_projection")
  case Maintenance extends ProcessorFamily("maintenance")

enum ProcessorMode(val value: String):
  case Continuous extends ProcessorMode("continuous")
  case Periodic extends ProcessorMode("periodic")

enum ProcessorId(
  val value: String,
  val owner: ProcessorOwner,
  val family: ProcessorFamily
):
  case SyncScanIngestion extends ProcessorId("sync-scan-ingestion", ProcessorOwner.Octopus, ProcessorFamily.Sync)
  case SyncJobPlanner extends ProcessorId("sync-job-planner", ProcessorOwner.Octopus, ProcessorFamily.Sync)
  case SyncBacklogRecovery extends ProcessorId("sync-backlog-recovery", ProcessorOwner.Octopus, ProcessorFamily.Sync)
  case SyncLoadDispatch extends ProcessorId("sync-load-dispatch", ProcessorOwner.Octopus, ProcessorFamily.Sync)
  case SyncLoadConsumer extends ProcessorId("sync-load-consumer", ProcessorOwner.Octopus, ProcessorFamily.Sync)
  case SyncResultConsumer extends ProcessorId("sync-result-consumer", ProcessorOwner.Octopus, ProcessorFamily.Sync)
  case SyncOutboxPublisher extends ProcessorId("sync-outbox-publisher", ProcessorOwner.Octopus, ProcessorFamily.Sync)
  case WirelessHeartbeatIngestion
      extends ProcessorId("wireless-heartbeat-ingestion", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessFrameNormalizer
      extends ProcessorId("wireless-frame-normalizer", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessInventoryProjector
      extends ProcessorId("wireless-inventory-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessIdentityProjector
      extends ProcessorId("wireless-identity-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessBehaviorProjector
      extends ProcessorId("wireless-behavior-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessTimingProjector
      extends ProcessorId("wireless-timing-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessSequenceProjector
      extends ProcessorId("wireless-sequence-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessBaselineProjector
      extends ProcessorId("wireless-baseline-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessSimilarityProjector
      extends ProcessorId("wireless-similarity-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case ThreatRiskProjector
      extends ProcessorId("threat-risk-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case EmbeddingPreparer extends ProcessorId("embedding-preparer", ProcessorOwner.Octopus, ProcessorFamily.Embedding)
  case EmbeddingCompleter
      extends ProcessorId("embedding-completer", ProcessorOwner.AtherosSearch, ProcessorFamily.Embedding)
  case EmbeddingLeaseRecovery
      extends ProcessorId("embedding-lease-recovery", ProcessorOwner.AtherosSearch, ProcessorFamily.Embedding)
  case EmbeddingTextBuilder
      extends ProcessorId("embedding-text-builder", ProcessorOwner.Octopus, ProcessorFamily.Embedding)
  case RfAlertProjector
      extends ProcessorId("rf-alert-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case EventRetention extends ProcessorId("event-retention", ProcessorOwner.Octopus, ProcessorFamily.Maintenance)
  case SearchRetention extends ProcessorId("search-retention", ProcessorOwner.Octopus, ProcessorFamily.Maintenance)
  case StaleWorkerCleanup
      extends ProcessorId("stale-worker-cleanup", ProcessorOwner.Octopus, ProcessorFamily.Maintenance)
  case ScheduledReconciliation
      extends ProcessorId("scheduled-reconciliation", ProcessorOwner.Octopus, ProcessorFamily.Maintenance)

object ProcessorId:
  val all: List[ProcessorId] = List(
    ProcessorId.SyncScanIngestion,
    ProcessorId.SyncJobPlanner,
    ProcessorId.SyncBacklogRecovery,
    ProcessorId.SyncLoadDispatch,
    ProcessorId.SyncLoadConsumer,
    ProcessorId.SyncResultConsumer,
    ProcessorId.SyncOutboxPublisher,
    ProcessorId.WirelessHeartbeatIngestion,
    ProcessorId.WirelessFrameNormalizer,
    ProcessorId.WirelessInventoryProjector,
    ProcessorId.WirelessIdentityProjector,
    ProcessorId.WirelessBehaviorProjector,
    ProcessorId.WirelessTimingProjector,
    ProcessorId.WirelessSequenceProjector,
    ProcessorId.WirelessBaselineProjector,
    ProcessorId.WirelessSimilarityProjector,
    ProcessorId.ThreatRiskProjector,
    ProcessorId.EmbeddingPreparer,
    ProcessorId.EmbeddingCompleter,
    ProcessorId.EmbeddingLeaseRecovery,
    ProcessorId.EmbeddingTextBuilder,
    ProcessorId.RfAlertProjector,
    ProcessorId.EventRetention,
    ProcessorId.SearchRetention,
    ProcessorId.StaleWorkerCleanup,
    ProcessorId.ScheduledReconciliation
  )

  private val byValue: Map[String, ProcessorId] = all.iterator.map(id => id.value -> id).toMap

  def fromString(value: String): Either[String, ProcessorId] =
    byValue.get(value).toRight(s"unknown or retired processor id: $value")

  val octopusOwned: List[ProcessorId] = all.filter(_.owner == ProcessorOwner.Octopus)
  val kafkaConsumers: Set[ProcessorId] = Set(
    ProcessorId.SyncScanIngestion,
    ProcessorId.SyncLoadConsumer,
    ProcessorId.SyncResultConsumer,
    ProcessorId.WirelessHeartbeatIngestion
  )

final case class ProcessorContract(
  id: ProcessorId,
  mode: ProcessorMode,
  inputs: List[String],
  outputs: List[String],
  dependencies: List[ProcessorId],
  dedupeKey: String,
  leaseScope: String,
  terminalBehavior: String,
  reconciliationPolicy: String,
  defaultEnabled: Boolean = false
)

/** Runtime view of the shared processor contract in
  * `sql/postgres/contracts/processors.json`. Tests keep both representations exact.
  */
object ProcessorCatalog:
  val contracts: List[ProcessorContract] = List(
    continuous(
      ProcessorId.SyncScanIngestion,
      List("sync.scan.request"),
      List("sync_events", "ingestion_evidence"),
      Nil,
      "group/topic/partition/offset",
      "kafka partition",
      "park/DLQ invalid records",
      "bounded offset audit"
    ),
    periodic(
      ProcessorId.SyncJobPlanner,
      List("sync_events"),
      List("sync_jobs", "sync_batches"),
      List(ProcessorId.SyncScanIngestion),
      "stream_name/dedupe_key",
      "stream",
      "park exhausted work",
      "orphan event scan"
    ),
    periodic(
      ProcessorId.SyncBacklogRecovery,
      List("expired sync leases"),
      List("sync_jobs", "sync_batches"),
      List(ProcessorId.SyncJobPlanner),
      "job_id/batch_id",
      "batch",
      "fail exhausted batch",
      "lease expiry scan"
    ),
    periodic(
      ProcessorId.SyncLoadDispatch,
      List("sync_batches"),
      List("outbox_events"),
      List(ProcessorId.SyncJobPlanner),
      "batch_id/attempt",
      "batch",
      "park exhausted dispatch",
      "batch/outbox audit"
    ),
    continuous(
      ProcessorId.SyncLoadConsumer,
      List("sync.oracle.load"),
      List("domain tables", "sync.oracle.result"),
      List(ProcessorId.SyncOutboxPublisher),
      "batch_id/attempt",
      "kafka partition",
      "sync result failure",
      "batch checksum"
    ),
    continuous(
      ProcessorId.SyncResultConsumer,
      List("sync.oracle.result"),
      List("sync_jobs", "sync_batches", "sync_cursors"),
      List(ProcessorId.SyncLoadConsumer),
      "batch_id/attempt",
      "kafka partition",
      "terminal job failure",
      "batch/cursor scan"
    ),
    continuous(
      ProcessorId.SyncOutboxPublisher,
      List("outbox_events"),
      List("Redpanda"),
      List(ProcessorId.SyncLoadDispatch),
      "destination_topic/message_key",
      "outbox row",
      "park exhausted publication",
      "publish-attempt audit"
    ),
    continuous(
      ProcessorId.WirelessHeartbeatIngestion,
      List("wireless.sensor.heartbeat"),
      List("ingestion_receipts", "sensors"),
      Nil,
      "consumer-group/topic/partition/offset and event-id",
      "kafka partition",
      "sanitize and park malformed records",
      "monotonic sensor heartbeat audit"
    ),
    continuous(
      ProcessorId.WirelessFrameNormalizer,
      List("wireless.audit"),
      List("wireless normalized tables"),
      List(ProcessorId.SyncLoadConsumer),
      "event_id",
      "wireless event",
      "park invalid frame",
      "source/frame checksum"
    ),
    periodic(
      ProcessorId.WirelessInventoryProjector,
      List("wireless normalized tables"),
      List("device/client/sensor inventory"),
      List(ProcessorId.WirelessFrameNormalizer),
      "device/window",
      "inventory key",
      "record reconciliation finding",
      "inventory rebuild"
    ),
    periodic(
      ProcessorId.WirelessIdentityProjector,
      List("inventory"),
      List("identity projections"),
      List(ProcessorId.WirelessInventoryProjector),
      "identity/source",
      "identity key",
      "record reconciliation finding",
      "identity rebuild"
    ),
    periodic(
      ProcessorId.WirelessBehaviorProjector,
      List("wireless normalized tables"),
      List("behaviour_snapshots"),
      List(ProcessorId.WirelessFrameNormalizer),
      "mac/window",
      "behaviour window",
      "record reconciliation finding",
      "behaviour rebuild"
    ),
    periodic(
      ProcessorId.WirelessTimingProjector,
      List("wireless normalized tables"),
      List("timing_profiles"),
      List(ProcessorId.WirelessFrameNormalizer),
      "mac/session",
      "timing window",
      "record reconciliation finding",
      "timing rebuild"
    ),
    periodic(
      ProcessorId.WirelessSequenceProjector,
      List("wireless normalized tables"),
      List("frame_sequences", "sequence_transitions"),
      List(ProcessorId.WirelessFrameNormalizer),
      "session/token",
      "sequence session",
      "record reconciliation finding",
      "sequence rebuild"
    ),
    periodic(
      ProcessorId.WirelessBaselineProjector,
      List("wireless normalized tables"),
      List("baseline_profiles"),
      List(ProcessorId.WirelessFrameNormalizer),
      "bssid/window",
      "baseline window",
      "record reconciliation finding",
      "baseline rebuild"
    ),
    periodic(
      ProcessorId.WirelessSimilarityProjector,
      List("search_vectors"),
      List("similarity_pairs"),
      List(ProcessorId.EmbeddingLeaseRecovery),
      "kind/anchor/candidate",
      "similarity anchor",
      "record reconciliation finding",
      "similarity rescan"
    ),
    periodic(
      ProcessorId.ThreatRiskProjector,
      List("wireless alerts", "similarity_pairs"),
      List("threat_signals", "ap_risk_scores"),
      List(ProcessorId.WirelessSimilarityProjector, ProcessorId.RfAlertProjector),
      "signal/subject/window",
      "risk subject",
      "record reconciliation finding",
      "risk rescan"
    ),
    periodic(
      ProcessorId.EmbeddingPreparer,
      List("search documents"),
      List("embedding_jobs"),
      List(ProcessorId.EmbeddingTextBuilder),
      "document/model/kind/checksum",
      "embedding job",
      "park invalid source",
      "missing-job scan"
    ),
    continuous(
      ProcessorId.EmbeddingCompleter,
      List("embedding_jobs"),
      List("search_vectors"),
      List(ProcessorId.EmbeddingPreparer),
      "job_id/model/fence",
      "embedding job",
      "bounded retry/DLQ",
      "source/vector checksum"
    ),
    continuous(
      ProcessorId.EmbeddingLeaseRecovery,
      List("expired embedding leases"),
      List("embedding_jobs"),
      List(ProcessorId.EmbeddingCompleter),
      "job_id/attempt/fence",
      "embedding job",
      "fail exhausted job",
      "lease expiry scan"
    ),
    periodic(
      ProcessorId.EmbeddingTextBuilder,
      List("normalized domain rows"),
      List("search_documents", "search_document_tokens"),
      List(ProcessorId.WirelessInventoryProjector),
      "source/checksum",
      "source row",
      "park malformed text input",
      "document checksum"
    ),
    periodic(
      ProcessorId.RfAlertProjector,
      List("wireless events"),
      List("wireless alerts"),
      Nil,
      "rule/subject/window",
      "alert key",
      "park invalid evidence",
      "rule replay"
    ),
    periodic(
      ProcessorId.EventRetention,
      List("expired core events", "archive metadata"),
      List("retention_runs"),
      List(ProcessorId.SyncResultConsumer),
      "policy/cutoff",
      "retention policy",
      "record failed run",
      "archive-before-delete audit"
    ),
    periodic(
      ProcessorId.SearchRetention,
      List("expired search rows"),
      List("retention_runs"),
      List(ProcessorId.EmbeddingLeaseRecovery),
      "policy/cutoff",
      "retention policy",
      "record failed run",
      "search expiry scan"
    ),
    periodic(
      ProcessorId.StaleWorkerCleanup,
      List("expired worker leases"),
      List("jobs", "leases"),
      List(ProcessorId.SyncBacklogRecovery),
      "work_id/fence",
      "work item",
      "park exhausted work",
      "lease scan"
    ),
    periodic(
      ProcessorId.ScheduledReconciliation,
      List("domain/projection state"),
      List("reconciliation_findings"),
      List(ProcessorId.StaleWorkerCleanup),
      "processor/entity/version",
      "processor shard",
      "persist unresolved finding",
      "deterministic diff/repair"
    )
  )

  val byId: Map[ProcessorId, ProcessorContract] = contracts.map(value => value.id -> value).toMap

  private def continuous(
    id: ProcessorId,
    inputs: List[String],
    outputs: List[String],
    dependencies: List[ProcessorId],
    dedupeKey: String,
    leaseScope: String,
    terminalBehavior: String,
    reconciliationPolicy: String
  ): ProcessorContract =
    ProcessorContract(
      id,
      ProcessorMode.Continuous,
      inputs,
      outputs,
      dependencies,
      dedupeKey,
      leaseScope,
      terminalBehavior,
      reconciliationPolicy
    )

  private def periodic(
    id: ProcessorId,
    inputs: List[String],
    outputs: List[String],
    dependencies: List[ProcessorId],
    dedupeKey: String,
    leaseScope: String,
    terminalBehavior: String,
    reconciliationPolicy: String
  ): ProcessorContract =
    ProcessorContract(
      id,
      ProcessorMode.Periodic,
      inputs,
      outputs,
      dependencies,
      dedupeKey,
      leaseScope,
      terminalBehavior,
      reconciliationPolicy
    )
