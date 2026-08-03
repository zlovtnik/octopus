# Octopus

Octopus is the Scala 3, Cats Effect, FS2, Doobie, and fs2-kafka coordinator for
durable ingestion into TiDB. It owns ingestion evidence, deduplication,
monotonic cursors, job and batch state, fenced outbox publication, TiDB load
results, wireless normalization inputs, and maintained projections. Embedding
execution and vector writes belong to Atheros Search.

All runtime lanes are disabled by default. PostgreSQL and MongoDB are not
runtime fallbacks.

## Current runtime

The currently wired binary provides:

- signed-cutover bootstrap for the three locked consumers;
- at-least-once ingestion evidence keyed by consumer group, topic, partition,
  and offset;
- durable scan ingestion from `sync.scan.request`;
- TiDB load work on `sync.oracle.load` and outcomes on
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
- `/live`, `/ready`, `/metrics`, `/health`, `/actuator/health`, and
  `/actuator/prometheus` HTTP routes.

MinIO archival, retention execution, search-document preparation, and
projection/intelligence families
are represented in the shared contract but are not yet wired into this binary.
They must remain disabled until their implementations and integration suites
land. Octopus does not currently export OTLP spans.

## Components

| Package | Responsibility |
|---|---|
| `config` | PureConfig model, environment overrides, and fail-closed validation |
| `cutover` | Signature, key-pin, cluster, group, partition, and offset verification |
| `kafka` | Locked consumers, common durable offset bootstrap, DLQ conversion, and wireless handlers |
| `tidb` | Repository implementations, transaction retry, transforms, checksums, schema preflight, and typed batch sinks |
| `tidb.sql` | Named JDBC SQL constants and total parameterized query builders |
| `persistence` | Effect-polymorphic store algebras, `DbResultT`, and `BatchStatement` |
| `processor` | Stable IDs, ownership contracts, retry policy, leases, runners, and supervision model |
| `cron` | Currently wired periodic ingest, recovery, dispatch, audit, and manifest checks |
| `dispatch` | Backpressure and durable outbox publication |
| `http` | Liveness, readiness, compatibility health, and metrics routes |
| `observability` | Structured logs and Micrometer counters/gauges |

The generic metadata-driven sink, metadata cache, `SinkPipe`, and
`SystemRegistry` were removed. Runtime DDL is forbidden: the provisioning
schema executor applies the ordered manifests under `sql/tidb/`, while Octopus
verifies them and fails closed.

## Processor ownership

The machine-readable source of truth is
[`sql/tidb/contracts/processors.json`](../../sql/tidb/contracts/processors.json).
Every entry declares its owner, family, mode, inputs, outputs, dependencies,
dedupe key, lease scope, terminal behavior, reconciliation policy, and default
state. All 28 entries default to disabled.

| Owner | Count | Processor IDs |
|---|---:|---|
| Octopus | 26 | `sync-scan-ingestion`, `sync-job-planner`, `sync-backlog-recovery`, `sync-load-dispatch`, `sync-load-consumer`, `sync-result-consumer`, `sync-outbox-publisher`, `wireless-frame-normalizer`, `wireless-inventory-projector`, `wireless-identity-projector`, `embedding-preparer`, `embedding-text-builder`, `behavior-projector`, `timing-projector`, `baseline-projector`, `sequence-projector`, `graph-projector`, `similarity-projector`, `clustering-projector`, `dns-alert-projector`, `rf-alert-projector`, `risk-projector`, `event-retention`, `search-retention`, `stale-worker-cleanup`, `scheduled-reconciliation` |
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
| `sync.oracle.load` | Octopus outbox to Octopus load consumer | TiDB load work; `oracle` is a legacy name |
| `sync.oracle.result` | Octopus load consumer to result consumer | TiDB load outcomes; `oracle` is a legacy name |
| `wireless.audit` | Atheros Sensor to Redpanda/Octopus | versioned wireless evidence |

Additional currently consumed topics are `proxy.payload_audit`,
`wireless.backlog.save`, `wireless.backlog.list`, `wireless.backlog.synced`,
`wireless.backlog.prune`, `wireless.mac.lookup`,
`wireless.networks.authorized`, and `wireless.probe.flush`.
Request/reply destinations are validated before
publication. Non-retryable poison messages go to `<source-topic>.dlq`.

## Persistence and delivery guarantees

- Delivery is at least once after the signed cutover offset.
- Ingestion evidence is unique by group/topic/partition/offset.
- Consumer offsets advance monotonically in the same TiDB transaction as
  durable processing evidence.
- Stream cursors advance monotonically and handle numeric wireless cursors
  without lexicographic regression.
- Outbox mutations require owner, lease token, and fence matches.
- Retryable database failures use bounded exponential delay; permanent failures
  fail closed or are parked according to the record contract.
- JDBC batch sinks retain prepared statements and batched execution; SQL is
  named in catalog modules and values remain bound parameters.

## Configuration and rollout

Important gates:

| Variable | Default | Meaning |
|---|---|---|
| `TIDB_ENABLED` | `false` | Enables TiDB after TLS, least-privilege, and schema validation |
| `OCTOPUS_CONSUMERS_ENABLED` | `false` | Enables signed-cutover Kafka consumers |
| `OCTOPUS_PROCESSORS_ENABLED` | `false` | Enables the processor lane |
| `OCTOPUS_ENABLED_PROCESSORS` | `[]` | Comma-separated Octopus-owned processor IDs |
| `OCTOPUS_PROCESSOR_RESTART_BASE_DELAY_MS` | `1000` | Initial retry delay |
| `OCTOPUS_PROCESSOR_RESTART_MAX_DELAY_MS` | `30000` | Maximum retry delay |
| `OCTOPUS_CUTOVER_DEV_BYPASS` | `false` | Development-only bypass; production rejects it |

TiDB uses `TIDB_HOST`, `TIDB_PORT`, `TIDB_DATABASE`, `TIDB_USER`,
`TIDB_PASSWORD`, `TIDB_POOL_SIZE`, and the `TIDB_SSL_*` settings. Enabled
runtime rejects loopback, root accounts, missing TLS identity verification,
warn-only schema validation, and incomplete cutover evidence.

Roll out canonical schema first, then a binary with all new processors
disabled. Enable processors in dependency order with
`OCTOPUS_ENABLED_PROCESSORS`, run bounded reconciliation comparisons, and only
then schedule live work. Rollback disables processors and replays durable work;
it never reverses schema or deletes ingestion evidence.

## Health and diagnosis

| Route | Meaning |
|---|---|
| `/live` | process is serving HTTP |
| `/ready` | TiDB is reachable and every enabled processor is ready or intentionally disabled |
| `/metrics` | Prometheus text exposition of Micrometer measurements |
| `/health` | compatibility alias for readiness |
| `/actuator/health` | Spring-compatible readiness response |
| `/actuator/prometheus` | compatibility alias for metrics |

When work stalls, check the signed cutover artifact, consumer lag, ingestion
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
python3 scripts/check-tidb-schema-contract.py
make dependency-boundaries
```

Docker-backed TiDB/Redpanda/MinIO tests skip when no Docker daemon is
available; report those skips explicitly rather than treating them as coverage.
