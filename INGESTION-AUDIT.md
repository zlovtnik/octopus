# Octopus capability and ingestion audit

## Scope and cleanup plan

The requested boundary is Redpanda to PostgreSQL for every ingested event type.
Projection, search preparation, DQ, archive and retention remain optional
downstream capabilities. Preserve producer payloads, broker group identities,
at-least-once delivery, durable replay checks and existing deployment gates.

Before changing implementations, run the coordinator suite and add regression
coverage for each confirmed defect. Execute these bounded passes:

1. Separate durable intake from typed load eligibility. Store unknown scan
   streams as completed raw events with evidence; create legacy load jobs only
   for supported sink types. Heartbeats must update sensor state transactionally
   without manufacturing unsupported load work.
2. Unify scan and heartbeat durable handling and error classification. Preserve
   broker offsets on transient storage errors and contradictory replay evidence.
3. Isolate permanent load-result persistence failures to their individual
   records. Preserve database lookup failures as errors rather than missing data.
4. Fix independently verified downstream correctness defects, with focused
   tests before each change. Remove unreachable consumer implementations and
   speculative unused code where repository references prove it is unused.
5. Record every wired processor, HTTP surface, external dependency and dormant
   implementation. Verify focused suites, coordinator tests, compiler/static
   checks, schema contracts and documentation references.

No database DDL is applied by this work. The canonical schema remains owned by
the provisioning executor. No deployment or live resource changes are included.

## Capability boundary

Octopus durably ingests records from its explicitly wired Redpanda topics into
PostgreSQL. A scan request's `stream_name` describes its payload; it does not
select a new Kafka subscription. Accepting a new stream name therefore requires
no typed sink implementation, while consuming a new broker topic still requires
an explicit consumer contract and workload.

The shared broker boundary validates and decodes a record, performs its durable
handler, then commits the Kafka offset. A new consumer group starts from the
earliest retained record; an existing group resumes at its committed offset.
Malformed/permanent poison records require acknowledged DLQ publication before
commit. Retryable storage failures and contradictory replay evidence fail closed.
See [the shared consumer](src/main/scala/com/sslproxy/coordinator/kafka/LockedTopicConsumer.scala)
and [ingestion storage](src/main/scala/com/sslproxy/coordinator/postgres/PostgresRepository.scala).

Scan intake stores the resolved raw payload and its hash in `sync_events`, with
broker coordinates and source-record hash in `ingestion_evidence`. Unknown
stream names complete raw ingestion without creating unsupported load jobs.
Known typed streams retain the existing job/batch/outbox path as downstream work.
Heartbeats update sensor state in their ingestion transaction without load jobs.
The compatibility payload resolver still accepts `inline://json/` and local
`outbox://` references; producers using the latter must provide the referenced
file. See [payload resolution](src/main/scala/com/sslproxy/coordinator/postgres/PostgresPayloadResolver.scala)
and [typed sink targets](src/main/scala/com/sslproxy/coordinator/postgres/PostgresSinkTarget.scala).

Optional downstream processing remains in Octopus: projections, search-document
preparation, DQ coordination, archival and retention. Embedding execution belongs
to [Atheros Search](../../apps/integration-console/atheros-search/README.md).
Public snapshot publication belongs to [Octopus Metrics](../octopus-metrics/README.md),
and public snapshot serving belongs to [Stats Reader](../stats-reader/README.md).

## Consumer inventory

These five workloads are declared in
[ConsumerWorkloads](src/main/scala/com/sslproxy/coordinator/wiring/workloads/ConsumerWorkloads.scala).
The consumer gate unions their IDs into the enabled set. The wireless projection
workload additionally requires `WIRELESS_PROJECTION_MODE=shadow` or `projected`;
`legacy` leaves that stream idle. DQ has a separate report consumer below.

| Processor | Input | Durable behavior and downstream work |
|---|---|---|
| `sync-scan-ingestion` | `sync.scan.request` | Persist raw payload and broker evidence for any valid stream name; supported sink types also receive jobs/batches. In projected wireless mode, the companion `wireless.audit` discovery request is skipped because its direct consumer owns processing. [Source](src/main/scala/com/sslproxy/coordinator/kafka/ScanRequestStream.scala) |
| `sync-load-consumer` | `sync.oracle.load` | Authorize against durable dispatch, stream bounded typed JDBC inserts, persist load outcomes/evidence, and enqueue result publication. [Source](src/main/scala/com/sslproxy/coordinator/kafka/PostgresLoadStream.scala) |
| `sync-result-consumer` | `sync.oracle.result` | Authorize and apply outcomes to jobs, batches, cursors and completion state, with broker evidence. [Source](src/main/scala/com/sslproxy/coordinator/kafka/PostgresResultStream.scala) |
| `wireless-audit-projection` | `wireless.audit` | Persist compact observation summaries, dedupe/receipt state, topology evidence and eligible embedding sources; expire projection state through atomic claims. [Source](src/main/scala/com/sslproxy/coordinator/kafka/WirelessAuditStream.scala) |
| `wireless-heartbeat-ingestion` | `wireless.sensor.heartbeat` | Validate/enrich heartbeat identity, persist raw event/evidence and monotonic sensor state without typed load work. [Source](src/main/scala/com/sslproxy/coordinator/kafka/WirelessHeartbeatStream.scala) |

`sync.oracle.load` and `sync.oracle.result` retain legacy topic names; their
runtime database is PostgreSQL. `proxy.payload_audit` is supported as a scan
payload type, not as an additional direct subscription.

## Typed load capabilities

These thirteen targets are implemented by
[PostgresTransformService](src/main/scala/com/sslproxy/coordinator/postgres/PostgresTransformService.scala),
[PostgresLoadHandler](src/main/scala/com/sslproxy/coordinator/postgres/PostgresLoadHandler.scala)
and [BatchSinkSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/BatchSinkSql.scala).
Eligibility for typed loads is separate from raw intake. Load dispatch also
requires the stream to appear in its configured downstream selection.

| Target stream | Accepted aliases | Typed output |
|---|---|---|
| `proxy.events` | None | Proxy events, blocked-host rollups and proxy preparation dirty-source markers |
| `proxy.payload_audit` | None | Payload-audit metadata |
| `wireless.audit` | None | Wireless audit frames and sensor metadata |
| `audit.wireless.bandwidth` | None | Bandwidth windows |
| `wireless.alert.rogue_ap` | `wireless.rogue_ap` | Rogue-AP alerts |
| `wireless.alert.deauth_flood` | `wireless.deauth_flood` | Deauthentication-flood alerts |
| `wireless.alert.signal_anomaly` | `wireless.signal_anomaly` | Signal-anomaly alerts |
| `wireless.alert.pmf_attack` | `wireless.pmf_attack` | Protected-management-frame attack alerts |
| `wireless.client.inventory` | `wireless.client_inventory` | Client inventory |
| `wireless.probe.flush` | `wireless.probe_requests` | Probe-request evidence |
| `wireless.alert.attack_sequence` | `wireless.attack_sequence` | Attack-sequence alerts |
| `wireless.alert.sequence` | `wireless.sequence`, `wireless.sequence_alert` | Sequence alerts |
| `wireless.alert.handshake` | `wifi.alert.handshake`, `wireless.handshake` | Handshake alerts |

Loads are authorized against coordinator-owned outbox state before payload
resolution. A complete load executes in one transaction while parsing and
inserting bounded row chunks; output includes row count/checksum or classified
failure. Domain decoding or transform failure cannot turn an unknown stream's
raw intake into unsupported typed load work.

## Downstream processor inventory

The following twenty scheduled workloads are declared in
[ScheduledWorkloads](src/main/scala/com/sslproxy/coordinator/wiring/workloads/ScheduledWorkloads.scala).
Their implementations live in
[CronScheduler](src/main/scala/com/sslproxy/coordinator/cron/CronScheduler.scala)
and the linked store/SQL modules. They run only when explicitly selected through
the processor configuration and its dependency checks. Database processors use
distributed fenced work leases; outbox publication uses its own atomic claims.

| Processor | Capability | Implementation |
|---|---|---|
| `sync-job-planner` | Plan jobs/batches for eligible staged work | [JobBatchSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/JobBatchSql.scala) |
| `sync-backlog-recovery` | Recover stale/expired batches and reconcile completed events | [PostgresRepository](src/main/scala/com/sslproxy/coordinator/postgres/PostgresRepository.scala) |
| `sync-load-dispatch` | Create durable load-dispatch outbox records | [JobBatchSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/JobBatchSql.scala) |
| `sync-outbox-publisher` | Publish claimed load/result outbox records to Redpanda and record fenced acknowledgements/retries | [BatchDispatchService](src/main/scala/com/sslproxy/coordinator/dispatch/BatchDispatchService.scala) |
| `wireless-frame-normalizer` | Normalize frame, radio, QoS, network, application, identity and security evidence | [WirelessProcessorSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/WirelessProcessorSql.scala) |
| `wireless-inventory-projector` | Maintain device, client and sensor inventories | [WirelessProcessorSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/WirelessProcessorSql.scala) |
| `wireless-identity-projector` | Derive device similarity, operator-review merge candidates, approved identity clusters and infrastructure graph | [IdentityGraphSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/IdentityGraphSql.scala) |
| `wireless-behavior-projector` | Maintain behavior snapshots from complete source windows | [WirelessProcessorSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/WirelessProcessorSql.scala) |
| `wireless-timing-projector` | Maintain timing profiles from complete sessions | [WirelessProcessorSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/WirelessProcessorSql.scala) |
| `wireless-sequence-projector` | Maintain frame sequences and replay-safe transition contributions | [WirelessProcessorSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/WirelessProcessorSql.scala) |
| `wireless-baseline-projector` | Maintain BSSID baseline profiles | [WirelessProcessorSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/WirelessProcessorSql.scala) |
| `wireless-similarity-projector` | Scan event, behavior and sequence vectors for similar pairs | [IntelligenceSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/IntelligenceSql.scala) |
| `threat-risk-projector` | Derive DNS threat signals and AP risk scores | [ThreatRiskSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/ThreatRiskSql.scala) |
| `embedding-text-builder` | Build versioned wireless search documents, normalized text, checksums, tokens and tags | [SearchPreparationSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/SearchPreparationSql.scala) |
| `embedding-preparer` | Prepare idempotent embedding jobs under a pending-queue high-water bound | [SearchPreparationSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/SearchPreparationSql.scala) |
| `proxy-search-preparer` | Prepare safe proxy evidence, versioned search documents and embedding work under source/revision fences | [ProxyPreparationSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/ProxyPreparationSql.scala) |
| `rf-alert-projector` | Derive deterministic RF alerts from wireless evidence | [ProjectionSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/ProjectionSql.scala) |
| `search-retention` | Remove eligible superseded search documents and dependent search state | [SearchRetentionProcessor](src/main/scala/com/sslproxy/coordinator/processor/SearchRetentionProcessor.scala), [MaintenanceSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/MaintenanceSql.scala) |
| `stale-worker-cleanup` | Recover expired worker/job/lease state | [MaintenanceSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/MaintenanceSql.scala) |
| `scheduled-reconciliation` | Compare maintained wireless projections and persist consistency findings | [MaintenanceSql](src/main/scala/com/sslproxy/coordinator/postgres/sql/MaintenanceSql.scala) |

The twenty-sixth Octopus processor, `event-retention`, is assembled separately by
[RetentionWorkloads](src/main/scala/com/sslproxy/coordinator/wiring/workloads/RetentionWorkloads.scala).
It archives eligible wireless payloads to MinIO, verifies hashes, persists archive
metadata, and applies archive-before-delete retention with replay tombstones and
retention-run records. Selecting it requires archival configuration.

The shared [processor contract](../../sql/postgres/contracts/processors.json)
also contains `embedding-completer` and `embedding-lease-recovery`. These two
processors are Search-owned and have no Octopus workload. The 26 Octopus IDs and
their ownership, inputs, outputs, dependencies and disabled defaults are mirrored
in [ProcessorCatalog](src/main/scala/com/sslproxy/coordinator/processor/ProcessorId.scala).

## Runtime support and external I/O

| Capability | Gate or lifetime | Source |
|---|---|---|
| PostgreSQL resource ownership, TLS, manifest preflight, pooled transactions, worker concurrency and health capacity | Database resource acquisition; runtime validation requires the canonical schema contract | [DatabaseModule](src/main/scala/com/sslproxy/coordinator/wiring/DatabaseModule.scala) |
| Periodic canonical-manifest verification | Either main runtime lane enabled | [RuntimeStreams](src/main/scala/com/sslproxy/coordinator/wiring/RuntimeStreams.scala) |
| One-time hydration of historical staged payloads | Either main runtime lane enabled; failure is logged separately from live consumers | [SyncEventHydrationService](src/main/scala/com/sslproxy/coordinator/ingest/SyncEventHydrationService.scala) |
| Initial configured cursor creation | Runtime fiber startup | [CoordinatorApplication](src/main/scala/com/sslproxy/coordinator/wiring/CoordinatorApplication.scala) |
| Pending-ledger backpressure checks and operational heartbeats | Processor support stream, with processor runtime and selected processors enabled | [CronScheduler](src/main/scala/com/sslproxy/coordinator/cron/CronScheduler.scala), [BackpressureService](src/main/scala/com/sslproxy/coordinator/dispatch/BackpressureService.scala) |
| Durable processor lifecycle/runs, dependency readiness and bounded restart backoff | Selected consumer/processor workloads | [ProcessorSupervisor](src/main/scala/com/sslproxy/coordinator/processor/ProcessorSupervisor.scala) |
| Structured logs, Micrometer/Prometheus, Kafka lag observations and OTLP spans | Process resources and active workload boundaries | [ObservabilityModule](src/main/scala/com/sslproxy/coordinator/wiring/ObservabilityModule.scala), [CoordinatorMetrics](src/main/scala/com/sslproxy/coordinator/observability/CoordinatorMetrics.scala) |
| Local payload-file reads | Compatibility `outbox://` resolution during intake/load/historical hydration | [PostgresPayloadResolver](src/main/scala/com/sslproxy/coordinator/postgres/PostgresPayloadResolver.scala) |
| MinIO archive reads/writes | Selected event retention | [MinioPayloadArchive](src/main/scala/com/sslproxy/coordinator/archive/MinioPayloadArchive.scala) |
| DQ policy reconciliation | Startup, including when DQ is disabled | [CoordinatorApplication](src/main/scala/com/sslproxy/coordinator/wiring/CoordinatorApplication.scala) |

## Optional DQ subsystem

DQ is independently gated by `OCTOPUS_DQ_ENABLED`; its streams are merged outside
the main consumer/processor gates. Octopus owns the durable registry, admission
ledger, run and attempt leases, receipts, retries, outbox, artifact lifecycle and
fenced application of worker proposals. The [DQ manager and isolated workers](../dq/README.md)
remain database-free.

| Capability | Durable boundary / external dependency | Source |
|---|---|---|
| Discover eligible work and reconcile operator decisions | PostgreSQL admission with scoped policy and pinned processor manifests | [DqRuntime](src/main/scala/com/sslproxy/coordinator/dq/DqRuntime.scala), [DqStore](src/main/scala/com/sslproxy/coordinator/dq/DqStore.scala) |
| Prepare immutable worker snapshots | PostgreSQL snapshot reads, MinIO artifact publication, durable preparation acknowledgement | [DqSnapshot](src/main/scala/com/sslproxy/coordinator/dq/DqSnapshot.scala), [DqArtifacts](src/main/scala/com/sslproxy/coordinator/dq/DqArtifacts.scala) |
| Consume worker reports | `dq.worker-reports.v1`; durable receipt or quarantine before offset commit, partition-local retry | [DqRuntime](src/main/scala/com/sslproxy/coordinator/dq/DqRuntime.scala) |
| Apply worker proposals | Verify input/output artifacts and source revision, then apply a fenced PostgreSQL transaction | [DqApply](src/main/scala/com/sslproxy/coordinator/dq/DqApply.scala) |
| Publish DQ outbox | Redpanda publication followed by durable acknowledgement | [DqRuntime](src/main/scala/com/sslproxy/coordinator/dq/DqRuntime.scala) |
| Expire attempts, retain artifacts and remove orphan objects | PostgreSQL claims and MinIO deletion/listing | [DqRetention](src/main/scala/com/sslproxy/coordinator/dq/DqRetention.scala) |
| Validate DQ broker prerequisites | Replication and cleanup/retention policy checks for `dq.commands.v1`, `dq.worker-reports.v1`, `dq.events.v1`, `dq.dlq.v1`, `dq.registry.v1`, `dq.audit.v1` | [DqRuntime](src/main/scala/com/sslproxy/coordinator/dq/DqRuntime.scala) |

## HTTP capability inventory

HTTP resources are assembled by
[ServerModule](src/main/scala/com/sslproxy/coordinator/wiring/ServerModule.scala).
Health, metrics and internal live telemetry are available without selecting a
processor. Public diagnostic stats and DQ routes have their own gates.

| Routes | Behavior | Source |
|---|---|---|
| `GET /live` | Process liveness | [HealthRoutes](src/main/scala/com/sslproxy/coordinator/http/HealthRoutes.scala) |
| `GET /ready`, `/health`, `/actuator/health` | Bounded PostgreSQL, processor and enabled-DQ readiness checks; unhealthy response is 503 | [HealthRoutes](src/main/scala/com/sslproxy/coordinator/http/HealthRoutes.scala) |
| `GET /metrics`, `/actuator/prometheus` | Prometheus text including JVM, workload and database metrics | [HealthRoutes](src/main/scala/com/sslproxy/coordinator/http/HealthRoutes.scala) |
| `GET /internal/metrics/live` | In-process readings only, with freshness gates and no database/store I/O | [LiveMetricsRoutes](src/main/scala/com/sslproxy/coordinator/http/LiveMetricsRoutes.scala) |
| `GET` / `OPTIONS /public/stats` | Optional diagnostic peaks/live JSON, CORS allowlist, cached background database refresh, request timeout | [PublicStatsRoutes](src/main/scala/com/sslproxy/coordinator/http/PublicStatsRoutes.scala) |
| `/internal/dq/v1/processors`, `/processor-versions`, `/processor-versions/{processor}/{version}` | Read catalog, register version, read pinned manifest | [DqRoutes](src/main/scala/com/sslproxy/coordinator/http/DqRoutes.scala) |
| `/internal/dq/v1/invocations/{key}`, `/runs`, `/runs/{id}`, `/runs/{id}/cancel`, `/runs/{id}/replay` | Invocation lookup, run admission/status/cancellation/replay | [DqRoutes](src/main/scala/com/sslproxy/coordinator/http/DqRoutes.scala) |
| `/internal/dq/v1/receipts`, `/workers/{id}/heartbeat`, `/dispatches` | Service-only worker receipts, heartbeat and dispatch discovery | [DqRoutes](src/main/scala/com/sslproxy/coordinator/http/DqRoutes.scala) |
| `/internal/dq/v1/runs/{id}/attempts/reserve`, `/attempts/{id}/accepted`, `/attempts/{id}/renew` | Service-only attempt reservation, acceptance and lease renewal | [DqRoutes](src/main/scala/com/sslproxy/coordinator/http/DqRoutes.scala) |
| `/internal/dq/v1/artifacts/{id}`, `/artifacts/{id}/access` | Scoped artifact metadata and authorized access | [DqRoutes](src/main/scala/com/sslproxy/coordinator/http/DqRoutes.scala) |

All DQ routes require the configured service bearer identity and scoped principal
checks; operator actions additionally enforce roles and installation/namespace.

## Removed dormant implementations

The audit found a direct payload-audit consumer, seven wireless operational
request/reply consumers, and a second canonical-ingestion implementation with no
runtime callers. These were implementation leftovers, not active capabilities.
Removing them leaves the five explicit consumer workloads above and avoids a
second, weaker retry/commit policy. Backlog state and existing persisted contracts
are not deleted by removing unreachable consumer code.

## Findings and verification

### Correctness fixes

- Unknown scan-stream names now persist a raw PostgreSQL event and broker
  evidence. Only known `PostgresSinkTarget` names create the legacy load job
  and batch. This makes the durable intake boundary independent of event type
  without widening the five explicit broker subscriptions.
- Heartbeats now validate the envelope, persist broker evidence, and update the
  canonical sensor row in the same transaction. They no longer create a load
  job for a sink with no heartbeat target.
- Contradictory broker coordinates and changed event identities are explicit
  `IngestionInvariantViolation`s. They stop the consumer instead of being
  parked and committed as ordinary poison records.
- A permanent persistence failure in a load-result batch is isolated to that
  record after the batch fails; retryable, invariant, and DLQ-publication
  failures still stop offset commits.
- A load outbox acknowledgement is fenced to its load attempt. A fast retry
  result reserves the next attempt, so an older acknowledgement cannot change
  it back to dispatched.
- Temporary payload lookup failures now remain retryable. They are no longer
  converted to a missing-payload permanent failure.
- Search retention removes unified embeddings, similarity pairs and scan state;
  threat/risk candidate selection advances across all eligible subjects; DQ
  readiness combines maintenance and report-consumer health; supervisor and
  dispatch errors use the standard sanitizer.

### Simplifications

- Removed roughly 1,700 lines of unwired payload-audit, wireless request/reply and
  duplicate canonical-ingestion code, along with their tests.
- Scan and heartbeat consumers share one durable-write/parking policy.
- Retained optional projections, search preparation, DQ, archival and retention
  as explicit downstream PostgreSQL workflows.

### Verification

- `sbt test` — 576 passed, 1 skipped (`LiveHydrationProbeSuite`, an existing
  opt-in live probe).
- `sbt "testOnly com.sslproxy.coordinator.ingest.OctopusPersistenceIntegrationSuite"`
  — 56 passed.
- `python3 scripts/check-docs.py` — passed.
- `make dependency-boundaries` — passed.
- `git diff --check` — passed.

Remaining operational risk: producers still need to publish through one of the
five subscribed topics, and `outbox://` payload resolution remains a deliberate
compatibility path. The intake no longer rejects unfamiliar stream names once a
record reaches `sync.scan.request`.
