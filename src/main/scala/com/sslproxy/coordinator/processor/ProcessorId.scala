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
  case PayloadAuditIngestion extends ProcessorId("payload-audit-ingestion", ProcessorOwner.Octopus, ProcessorFamily.Sync)
  case WirelessFrameNormalizer extends ProcessorId("wireless-frame-normalizer", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessInventoryProjector extends ProcessorId("wireless-inventory-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessIdentityProjector extends ProcessorId("wireless-identity-projector", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessBacklogSave extends ProcessorId("wireless-backlog-save", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessBacklogList extends ProcessorId("wireless-backlog-list", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessBacklogSynced extends ProcessorId("wireless-backlog-synced", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessBacklogPrune extends ProcessorId("wireless-backlog-prune", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessMacLookup extends ProcessorId("wireless-mac-lookup", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessNetworksAuthorized extends ProcessorId("wireless-networks-authorized", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case WirelessProbeFlush extends ProcessorId("wireless-probe-flush", ProcessorOwner.Octopus, ProcessorFamily.Wireless)
  case EmbeddingPreparer extends ProcessorId("embedding-preparer", ProcessorOwner.Octopus, ProcessorFamily.Embedding)
  case EmbeddingCompleter extends ProcessorId("embedding-completer", ProcessorOwner.AtherosSearch, ProcessorFamily.Embedding)
  case EmbeddingLeaseRecovery extends ProcessorId("embedding-lease-recovery", ProcessorOwner.AtherosSearch, ProcessorFamily.Embedding)
  case EmbeddingTextBuilder extends ProcessorId("embedding-text-builder", ProcessorOwner.Octopus, ProcessorFamily.Embedding)
  case BehaviorProjector extends ProcessorId("behavior-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case TimingProjector extends ProcessorId("timing-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case BaselineProjector extends ProcessorId("baseline-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case SequenceProjector extends ProcessorId("sequence-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case GraphProjector extends ProcessorId("graph-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case SimilarityProjector extends ProcessorId("similarity-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case ClusteringProjector extends ProcessorId("clustering-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case DnsAlertProjector extends ProcessorId("dns-alert-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case RfAlertProjector extends ProcessorId("rf-alert-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case RiskProjector extends ProcessorId("risk-projector", ProcessorOwner.Octopus, ProcessorFamily.SearchProjection)
  case EventRetention extends ProcessorId("event-retention", ProcessorOwner.Octopus, ProcessorFamily.Maintenance)
  case SearchRetention extends ProcessorId("search-retention", ProcessorOwner.Octopus, ProcessorFamily.Maintenance)
  case StaleWorkerCleanup extends ProcessorId("stale-worker-cleanup", ProcessorOwner.Octopus, ProcessorFamily.Maintenance)
  case ScheduledReconciliation extends ProcessorId("scheduled-reconciliation", ProcessorOwner.Octopus, ProcessorFamily.Maintenance)

object ProcessorId:
  private val byValue: Map[String, ProcessorId] =
    ProcessorId.values.iterator.map(id => id.value -> id).toMap

  def fromString(value: String): Either[String, ProcessorId] =
    byValue.get(value).toRight(s"unknown processor id: $value")

  val all: List[ProcessorId] = ProcessorId.values.toList
  val octopusOwned: List[ProcessorId] = all.filter(_.owner == ProcessorOwner.Octopus)

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
    continuous(ProcessorId.SyncScanIngestion, List("sync.scan.request"), List("sync_events", "ingestion_evidence"), Nil, "group/topic/partition/offset", "kafka partition", "park/DLQ invalid records", "bounded offset audit"),
    periodic(ProcessorId.SyncJobPlanner, List("sync_events"), List("sync_jobs", "sync_batches"), List(ProcessorId.SyncScanIngestion), "stream_name/dedupe_key", "stream", "park exhausted work", "orphan event scan"),
    periodic(ProcessorId.SyncBacklogRecovery, List("expired sync leases"), List("sync_jobs", "sync_batches"), List(ProcessorId.SyncJobPlanner), "job_id/batch_id", "batch", "fail exhausted batch", "lease expiry scan"),
    periodic(ProcessorId.SyncLoadDispatch, List("sync_batches"), List("outbox_events"), List(ProcessorId.SyncJobPlanner), "batch_id/attempt", "batch", "park exhausted dispatch", "batch/outbox audit"),
    continuous(ProcessorId.SyncLoadConsumer, List("sync.oracle.load"), List("domain tables", "sync.oracle.result"), List(ProcessorId.SyncOutboxPublisher), "batch_id/attempt", "kafka partition", "sync result failure", "batch checksum"),
    continuous(ProcessorId.SyncResultConsumer, List("sync.oracle.result"), List("sync_jobs", "sync_batches", "sync_cursors"), List(ProcessorId.SyncLoadConsumer), "batch_id/attempt", "kafka partition", "terminal job failure", "batch/cursor scan"),
    continuous(ProcessorId.SyncOutboxPublisher, List("outbox_events"), List("Redpanda"), List(ProcessorId.SyncLoadDispatch), "destination_topic/message_key", "outbox row", "park exhausted publication", "publish-attempt audit"),
    continuous(ProcessorId.PayloadAuditIngestion, List("payload.audit.ingest"), List("ingestion scan_requests"), List(ProcessorId.SyncScanIngestion), "group/topic/partition/offset", "kafka partition", "DLQ invalid records", "bounded offset audit"),
    continuous(ProcessorId.WirelessFrameNormalizer, List("wireless.audit"), List("wireless normalized tables"), List(ProcessorId.SyncLoadConsumer), "event_id", "wireless event", "park invalid frame", "source/frame checksum"),
    periodic(ProcessorId.WirelessInventoryProjector, List("wireless normalized tables"), List("device/client/sensor inventory"), List(ProcessorId.WirelessFrameNormalizer), "device/window", "inventory key", "record reconciliation finding", "inventory rebuild"),
    periodic(ProcessorId.WirelessIdentityProjector, List("inventory", "similarity evidence"), List("identity projections"), List(ProcessorId.WirelessInventoryProjector, ProcessorId.SimilarityProjector), "identity/source", "identity key", "record reconciliation finding", "identity rebuild"),
    continuous(ProcessorId.WirelessBacklogSave, List("wireless.backlog.save"), List("wireless backlog table"), Nil, "topic/partition/offset", "kafka partition", "DLQ invalid records", "bounded offset audit"),
    continuous(ProcessorId.WirelessBacklogList, List("wireless.backlog.list"), List("wireless backlog table"), Nil, "topic/partition/offset", "kafka partition", "DLQ invalid records", "bounded offset audit"),
    continuous(ProcessorId.WirelessBacklogSynced, List("wireless.backlog.synced"), List("wireless backlog table"), Nil, "topic/partition/offset", "kafka partition", "DLQ invalid records", "bounded offset audit"),
    continuous(ProcessorId.WirelessBacklogPrune, List("wireless.backlog.prune"), List("wireless backlog table"), Nil, "topic/partition/offset", "kafka partition", "DLQ invalid records", "bounded offset audit"),
    continuous(ProcessorId.WirelessMacLookup, List("wireless.mac.lookup"), List("wireless device table"), Nil, "topic/partition/offset", "kafka partition", "DLQ invalid records", "bounded offset audit"),
    continuous(ProcessorId.WirelessNetworksAuthorized, List("wireless.networks.authorized"), List("wireless networks table"), Nil, "topic/partition/offset", "kafka partition", "DLQ invalid records", "bounded offset audit"),
    continuous(ProcessorId.WirelessProbeFlush, List("wireless.probe.flush"), List("wireless probe table"), Nil, "topic/partition/offset", "kafka partition", "DLQ invalid records", "bounded offset audit"),
    periodic(ProcessorId.EmbeddingPreparer, List("search documents"), List("embedding_jobs"), List(ProcessorId.EmbeddingTextBuilder), "document/model/kind/checksum", "embedding job", "park invalid source", "missing-job scan"),
    continuous(ProcessorId.EmbeddingCompleter, List("embedding_jobs"), List("search_vectors"), List(ProcessorId.EmbeddingPreparer), "job_id/model/fence", "embedding job", "bounded retry/DLQ", "source/vector checksum"),
    continuous(ProcessorId.EmbeddingLeaseRecovery, List("expired embedding leases"), List("embedding_jobs"), List(ProcessorId.EmbeddingCompleter), "job_id/attempt/fence", "embedding job", "fail exhausted job", "lease expiry scan"),
    periodic(ProcessorId.EmbeddingTextBuilder, List("normalized domain rows"), List("search_documents", "search_document_tokens"), List(ProcessorId.WirelessInventoryProjector), "source/checksum", "source row", "park malformed text input", "document checksum"),
    periodic(ProcessorId.BehaviorProjector, List("wireless/proxy events"), List("behavior snapshots"), List(ProcessorId.WirelessFrameNormalizer), "subject/window/version", "projection key", "record reconciliation finding", "window rebuild"),
    periodic(ProcessorId.TimingProjector, List("events"), List("timing projections"), List(ProcessorId.WirelessFrameNormalizer), "subject/window/version", "projection key", "record reconciliation finding", "window rebuild"),
    periodic(ProcessorId.BaselineProjector, List("timing", "behavior"), List("baseline projections"), List(ProcessorId.BehaviorProjector, ProcessorId.TimingProjector), "subject/window/version", "projection key", "record reconciliation finding", "baseline rebuild"),
    periodic(ProcessorId.SequenceProjector, List("ordered events"), List("sequence transitions"), List(ProcessorId.WirelessFrameNormalizer), "subject/from/to/window", "projection key", "record reconciliation finding", "transition rebuild"),
    periodic(ProcessorId.GraphProjector, List("documents", "identities"), List("graph nodes", "graph edges"), List(ProcessorId.WirelessIdentityProjector), "node-or-edge/version", "graph key", "record reconciliation finding", "graph rebuild"),
    periodic(ProcessorId.SimilarityProjector, List("search_vectors"), List("similarity projections"), List(ProcessorId.EmbeddingCompleter), "left/right/model", "pair key", "record reconciliation finding", "similarity rebuild"),
    periodic(ProcessorId.ClusteringProjector, List("similarities"), List("identity clusters"), List(ProcessorId.SimilarityProjector), "member/model/version", "cluster key", "record reconciliation finding", "cluster rebuild"),
    periodic(ProcessorId.DnsAlertProjector, List("proxy DNS events"), List("threat signals"), List(ProcessorId.SyncLoadConsumer), "rule/subject/window", "alert key", "park invalid evidence", "rule replay"),
    periodic(ProcessorId.RfAlertProjector, List("wireless events"), List("wireless alerts", "threat signals"), List(ProcessorId.BaselineProjector, ProcessorId.SequenceProjector), "rule/subject/window", "alert key", "park invalid evidence", "rule replay"),
    periodic(ProcessorId.RiskProjector, List("alerts", "behavior"), List("risk projections"), List(ProcessorId.DnsAlertProjector, ProcessorId.RfAlertProjector), "subject/model/version", "risk key", "record reconciliation finding", "risk rebuild"),
    periodic(ProcessorId.EventRetention, List("expired core events", "archive metadata"), List("retention_runs"), List(ProcessorId.SyncResultConsumer), "policy/cutoff", "retention policy", "record failed run", "archive-before-delete audit"),
    periodic(ProcessorId.SearchRetention, List("expired search rows"), List("retention_runs"), List(ProcessorId.EmbeddingLeaseRecovery), "policy/cutoff", "retention policy", "record failed run", "search expiry scan"),
    periodic(ProcessorId.StaleWorkerCleanup, List("expired worker leases"), List("jobs", "leases"), List(ProcessorId.SyncBacklogRecovery), "work_id/fence", "work item", "park exhausted work", "lease scan"),
    periodic(ProcessorId.ScheduledReconciliation, List("domain/projection state"), List("reconciliation_findings"), List(ProcessorId.StaleWorkerCleanup), "processor/entity/version", "processor shard", "persist unresolved finding", "deterministic diff/repair")
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
    ProcessorContract(id, ProcessorMode.Continuous, inputs, outputs, dependencies, dedupeKey, leaseScope, terminalBehavior, reconciliationPolicy)

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
    ProcessorContract(id, ProcessorMode.Periodic, inputs, outputs, dependencies, dedupeKey, leaseScope, terminalBehavior, reconciliationPolicy)
