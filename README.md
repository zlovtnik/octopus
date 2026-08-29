# Octopus

Octopus is the Scala 3, Cats Effect, FS2, Doobie, and fs2-kafka coordinator for
durable ingestion into PostgreSQL. It owns ingestion evidence, deduplication,
monotonic cursors, job and batch state, fenced outbox publication, PostgreSQL load
results, wireless normalization inputs, and maintained projections. Embedding
execution and vector writes belong to Atheros Search.

All runtime lanes are disabled by default. PostgreSQL and MongoDB are not
runtime fallbacks.

## Current runtime

The currently wired binary provides:

- ordinary Kafka consumer-group restart positions for the three locked consumers;
- at-least-once ingestion evidence keyed by consumer group, topic, partition,
  and offset;
- durable scan ingestion from `sync.scan.request`;
- PostgreSQL load work on `sync.oracle.load` and outcomes on
  `sync.oracle.result`;
- durable jobs, batches, cursors, outbox leases, retry state, and DLQ handling;
- payload-audit ingestion;
- all seven wireless operations: backlog save, oldest-100 pending list,
  idempotent mark-synced, seven-day prune, MAC lookup, authorized-network
  lookup, and probe flush;
- JSON hydration and typed JDBC batch sinks for proxy and wireless rows;
- persisted processor state/runs, dependency validation, deterministic retry
  jitter, backpressure, expired outbox-lease recovery, shadow-alert generation,
  and periodic canonical-manifest verification;
- idempotent wireless frame normalization across frame, radio, QoS, network,
  application, identity, and security tables, plus device/client inventory;
- deterministic wireless search-document text, checksums, tokens, tags,
  version supersession, and one embedding job per document/model/checksum;
- fenced, disabled-by-default wireless payload archival and event retention,
  including deterministic MinIO object keys, hash verification, durable archive
  metadata, archive-before-delete enforcement, tombstones, and retention runs;
- fenced search-document retention, stale worker cleanup, and scheduled
  wireless projection reconciliation with durable findings;
- pure, deterministic behavior-window, timing percentile/jitter, 13-token
  sequence, baseline, vector-similarity, clustering, DNS-threat, and AP-risk
  transformations with PostgreSQL projection writers;
- approved-merge identity membership and infrastructure graph projection;
- `/live`, `/ready`, `/metrics`, `/health`, `/actuator/health`, and
  `/actuator/prometheus` HTTP routes;
- OTLP spans for locked Kafka consume/commit batches, outbox/DLQ publication,
  and every PostgreSQL durable operation, with error recording and bounded SDK shutdown.

All 34 Octopus-owned processor IDs have exactly one workload declaration and
remain disabled by default.

## Components

| Package | Responsibility |
|---|---|
| `config` | PureConfig model, environment overrides, and fail-closed validation |
| `kafka` | Locked consumers, committed-offset restart, DLQ conversion, and wireless handlers |
| `postgres` | Repository implementations, transaction retry, transforms, checksums, schema preflight, and typed batch sinks |
| `postgres.sql` | Named JDBC SQL constants and total parameterized query builders |
| `persistence` | Effect-polymorphic store algebras, `DbResultT`, and `BatchStatement` |
| `processor` | Stable IDs, ownership contracts, retry policy, leases, runners, and supervision model |
| `archive` | Hash-verified, idempotent MinIO payload storage acquired as a `Resource` |
| `cron` | Currently wired periodic ingest, recovery, dispatch, audit, and manifest checks |
| `dispatch` | Backpressure and durable outbox publication |
| `http` | Liveness, readiness, compatibility health, and metrics routes |
| `observability` | Structured logs, Micrometer counters/gauges, and OTLP tracing |

The generic metadata-driven sink, metadata cache, `SinkPipe`, and
`SystemRegistry` were removed. Runtime DDL is forbidden: the provisioning
schema executor applies the ordered manifests under `sql/postgres/`, while Octopus
verifies them and fails closed.

## Processor ownership

The machine-readable source of truth is
[`sql/postgres/contracts/processors.json`](../../sql/postgres/contracts/processors.json).
Every entry declares its owner, family, mode, inputs, outputs, dependencies,
dedupe key, lease scope, terminal behavior, reconciliation policy, and default
state. All 36 entries default to disabled.

| Owner | Count | Processor IDs |
|---|---:|---|
| Octopus | 34 | `sync-scan-ingestion`, `sync-job-planner`, `sync-backlog-recovery`, `sync-load-dispatch`, `sync-load-consumer`, `sync-result-consumer`, `sync-outbox-publisher`, `payload-audit-ingestion`, `wireless-frame-normalizer`, `wireless-inventory-projector`, `wireless-identity-projector`, `wireless-backlog-save`, `wireless-backlog-list`, `wireless-backlog-synced`, `wireless-backlog-prune`, `wireless-mac-lookup`, `wireless-networks-authorized`, `wireless-probe-flush`, `embedding-preparer`, `embedding-text-builder`, `behavior-projector`, `timing-projector`, `baseline-projector`, `sequence-projector`, `graph-projector`, `similarity-projector`, `clustering-projector`, `dns-alert-projector`, `rf-alert-projector`, `risk-projector`, `event-retention`, `search-retention`, `stale-worker-cleanup`, `scheduled-reconciliation` |
| Atheros Search | 2 | `embedding-completer`, `embedding-lease-recovery` |

There is no Rails/console processor family. The `integration_console` database
remains reserved and provisioned with no runtime owner; the SolidJS Atheros
Search UI remains active and reads through the Search API. Redis remains a
non-authoritative shared cache.

## Topic contracts

These names and meanings are locked:

| Topic | Direction | Meaning |
|---|---|---|
| `sync.scan.request` | producers to Octopus | work discovery and durable scan ingestion |
| `sync.oracle.load` | Octopus outbox to Octopus load consumer | PostgreSQL load work; `oracle` is a legacy name |
| `sync.oracle.result` | Octopus load consumer to result consumer | PostgreSQL load outcomes; `oracle` is a legacy name |
| `wireless.audit` | Atheros Sensor to Redpanda/Octopus | versioned wireless evidence |

Additional currently consumed topics are `proxy.payload_audit`,
`wireless.backlog.save`, `wireless.backlog.list`, `wireless.backlog.synced`,
`wireless.backlog.prune`, `wireless.mac.lookup`,
`wireless.networks.authorized`, and `wireless.probe.flush`.
Request/reply destinations are validated before
publication. Non-retryable poison messages go to `<source-topic>.dlq`.

## Persistence and delivery guarantees

- Delivery is at least once from each consumer group's committed Kafka offset;
  a group without committed offsets starts at the earliest retained record.
- Ingestion evidence is unique by group/topic/partition/offset.
- Consumer offsets advance monotonically in the same PostgreSQL transaction as
  durable processing evidence.
- Stream cursors advance monotonically and handle numeric wireless cursors
  without lexicographic regression.
- Outbox mutations require owner, lease token, and fence matches.
- Every periodic workload claims and continuously renews a persisted
  `work_leases` fence; a competing replica skips the tick, and lease loss
  cancels the in-flight operation.
- Behavior, timing, baseline, and sequence projections compare authoritative
  source counts so late-arriving evidence is reprocessed. Sequence transition
  contributions are stored per session before aggregate probabilities are
  rebuilt, making replay idempotent.
- Retryable database failures use bounded exponential delay; permanent failures
  fail closed or are parked according to the record contract.
- JDBC batch sinks retain prepared statements and batched execution; SQL is
  named in catalog modules and values remain bound parameters.

## Configuration and rollout

The complete configuration reference is the checked-in
[`application.conf`](src/main/resources/application.conf). Each environment
override is declared directly beside its typed default, so the file is the
authoritative list of accepted variable names and defaults. `AppConfig.validate`
is the authoritative startup-requirement check; `AppConfigSuite` loads
the reference and exercises the fail-closed bounds and conditional gates.

| Configuration block | Environment families | Startup requirement |
|---|---|---|
| `postgres` | `POSTGRES_*` | Required when either runtime lane is enabled; external host, non-root account, password, verified TLS identity, canonical manifest digest, positive pool/timeouts |
| `kafka` | `SYNC_*`, legacy `COORDINATOR_*` aliases | Positive polling/batch/partition/replication bounds, versioned consumer groups, earliest retained startup for new groups, manual commit after durable processing, and one shared `SYNC_DLQ_SUFFIX` for locked and wireless consumers |
| `cron` | `COORDINATOR_*`, `SCHEMA_REFRESH_INTERVAL_SECS` | Every interval, attempt count, lease, fetch count, and batch size must be positive |
| `backpressure` | `COORDINATOR_BACKPRESSURE_*`, `COORDINATOR_ADAPTIVE_PULL_*` | Multiplier, change threshold, and restart interval must be positive |
| `wireless` | `WIRELESS_*` | Consumer count and poll bound must be positive; topics and versioned groups are required for an enabled consumer lane |
| `processors` | `OCTOPUS_PROCESSOR_*`, `OCTOPUS_ENABLED_PROCESSORS`, similarity/distance variables | Enabled IDs must be Octopus-owned with dependencies enabled; delays, interval, and batch size positive; scores finite and in range |
| `archive` | `OCTOPUS_ARCHIVE_ENABLED`, `MINIO_*`, retention and archive variables | Credentials and bucket required when enabled; retention ordering, intervals, and batch size validated |

Unknown or obsolete overrides are not alternate configuration sources. In
particular, `WIRELESS_DLQ_SUFFIX` is retired; use `SYNC_DLQ_SUFFIX` for every
consumer DLQ.

Important gates:

| Variable | Default | Meaning |
|---|---|---|
| `POSTGRES_ENABLED` | `false` | Enables PostgreSQL after TLS, least-privilege, and schema validation |
| `POSTGRES_SCHEMA_MANIFEST_SHA256` | bundled `octopus_core` manifest digest | Exact executor-recorded canonical schema digest; startup and periodic verification fail closed on drift |
| `OCTOPUS_CONSUMERS_ENABLED` | `false` | Enables Kafka consumers using durable committed offsets |
| `OCTOPUS_PROCESSORS_ENABLED` | `false` | Enables the processor lane |
| `OCTOPUS_ENABLED_PROCESSORS` | `[]` | Comma-separated Octopus-owned processor IDs |
| `OCTOPUS_PROCESSOR_RESTART_BASE_DELAY_MS` | `1000` | Initial retry delay |
| `OCTOPUS_PROCESSOR_RESTART_MAX_DELAY_MS` | `30000` | Maximum retry delay |
| `OCTOPUS_PROCESSOR_BATCH_SIZE` | `250` | Bound for normalized projection and search-preparation passes |
| `OCTOPUS_PROCESSOR_INTERVAL_SECONDS` | `10` | Periodic search preparation interval |
| `OCTOPUS_EMBEDDING_MODEL` | `sentence-transformers/all-MiniLM-L6-v2` | Model attached to newly prepared embedding jobs |
| `OCTOPUS_EVENT_DUPLICATE_DISTANCE` | `0.05` | Characterized event-vector duplicate distance |
| `OCTOPUS_BEHAVIOR_SIMILARITY_THRESHOLD` | `0.88` | Characterized behavior similarity threshold |
| `OCTOPUS_SEQUENCE_DISTANCE_THRESHOLD` | `0.10` | Characterized frame-sequence distance threshold |
| `OCTOPUS_ARCHIVE_ENABLED` | `false` | Required gate for `event-retention` |
| `MINIO_ENDPOINT` | `http://minio:9000` | S3-compatible archive endpoint |
| `MINIO_ACCESS_KEY_ID` / `MINIO_SECRET_ACCESS_KEY` | empty | Archive credentials sourced from the runtime secret |
| `WIRELESS_RAW_ARCHIVE_BUCKET` | `ssl-proxy-wireless-raw-archive` | Raw wireless payload archive bucket |
| `WIRELESS_RAW_PAYLOAD_HOT_DAYS` | `7` | Age before a hot payload becomes archive-eligible |
| `SYNC_EVENT_ROW_RETENTION_DAYS` | `30` | Age before an archived terminal event becomes deletion-eligible |
| `SEARCH_RETENTION_DAYS` | `30` | Age before a terminal superseded search document becomes deletion-eligible |
| `SYNC_EVENT_TOMBSTONE_RETENTION_DAYS` | `45` | Replay-protection period after event deletion |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | SDK default | OTLP endpoint for Kafka and PostgreSQL boundary spans |
| `OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG` | SDK defaults | Trace sampling policy; the Kustomize base uses `traceidratio` |

PostgreSQL uses `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DATABASE`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`, `POSTGRES_POOL_SIZE`, and explicit `POSTGRES_SSL_MODE`. Canonical
Kustomize sets `verify-full` with the PgBouncer listener CA and
`POSTGRES_SSL_SERVER_NAME=postgres-pgbouncer`; the listener private key is never
mounted in Octopus. Enabled runtime rejects loopback, root accounts,
warn-only schema validation, and invalid consumer-group contracts.

The checked-in Kubernetes deployment enables both runtime lanes, archival, and
all 34 Octopus-owned processors. A new consumer group replays every retained
record; an existing group resumes from its committed Kafka offsets. Rollback
must preserve consumer offsets, schemas, and ingestion evidence.

## Health and diagnosis

| Route | Meaning |
|---|---|
| `/live` | process is serving HTTP |
| `/ready` | PostgreSQL is reachable and every enabled processor is ready or intentionally disabled |
| `/metrics` | Prometheus text exposition of Micrometer measurements |
| `/health` | compatibility alias for readiness |
| `/actuator/health` | Spring-compatible readiness response |
| `/actuator/prometheus` | compatibility alias for metrics |

Processor metrics include a one-hot lifecycle gauge per processor, the current
persisted restart count, and supervised retry counters. Existing ingestion,
pending-ledger, backpressure, outbox, and DLQ counters remain available on the
same surface.

When work stalls, check consumer membership and committed offsets, lag, ingestion
evidence, pending jobs/batches, outbox lease/fence state, retry timestamps, and
DLQ topics in that order. Do not repair incidents by deleting offsets, outbox
rows, tombstones, or authoritative ingestion evidence.

## Build and verification

Octopus is sbt-only:

```bash
sbt test
sbt assembly
```

Repository-level checks include:

```bash
python3 scripts/check-postgres-schema-contract.py
make dependency-boundaries
```

Docker-backed PostgreSQL/Redpanda/MinIO tests skip when no Docker daemon is
available; report those skips explicitly rather than treating them as coverage.
