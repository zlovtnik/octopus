# Octopus

Octopus is the Scala 3, Cats Effect, FS2, Doobie, and fs2-kafka coordinator for
durable ingestion into PostgreSQL. It owns ingestion evidence, deduplication,
monotonic cursors, job and batch state, fenced outbox publication, PostgreSQL load
results, wireless normalization inputs, and maintained projections. Embedding
execution and vector writes belong to Atheros Search.

All runtime lanes are disabled by default. PostgreSQL is the only stack
runtime database; MongoDB is not a fallback.

## Current runtime

Production runs one coordinator replica with a `Recreate` rollout, a 4 GiB
container memory limit, and a 60% JVM heap ceiling. All consumer groups and
processors run inside that instance; Kafka assigns it the available partitions.
Updates briefly pause consumption until the replacement starts and resumes from
committed offsets. Staging currently reuses the production coordinator patch.

Behavior, timing, sequence, and baseline projections fetch rows through JDBC
cursors in chunks of 128 and persist one complete window, session, or BSSID at a
time. The processor batch size limits groups, not source frames. Memory therefore
depends on the largest group instead of the entire batch; unusually large
individual sessions or BSSID histories still need capacity monitoring. Do not
add an outer row limit: partial groups would corrupt counts and replay detection.

The currently wired binary provides:

- ordinary Kafka consumer-group restart positions for the three locked consumers;
- at-least-once ingestion evidence keyed by consumer group, topic, partition,
  and offset;
- durable scan ingestion from `sync.scan.request`;
- PostgreSQL load work on `sync.oracle.load` and outcomes on
  `sync.oracle.result`;
- durable jobs, batches, cursors, outbox leases, retry state, and DLQ handling;
- `wireless.sensor.heartbeat` ingestion via `wireless-heartbeat-ingestion`,
  with malformed records sanitized and parked on `wireless.sensor.heartbeat.dlq`;
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
- deterministic RF-alert projection with PostgreSQL projection writers;
- wireless inventory and identity projection maintenance, with device-vector
  similarity scans producing merge candidates for operator review; approved
  decisions become identity clusters;
- `/live`, `/ready`, `/metrics`, `/health`, `/actuator/health`, and
  `/actuator/prometheus` HTTP routes;
- OTLP spans for locked Kafka consume/commit batches, outbox/DLQ publication,
  and every PostgreSQL durable operation, with error recording and bounded SDK shutdown.

All 24 Octopus-owned processor IDs have exactly one workload declaration and
remain disabled by default.

## Components

| Package | Responsibility |
|---|---|
| `config` | PureConfig model, environment overrides, and fail-closed validation |
| `ingest` | Scan-request hydration and `proxy.payload_audit` consumption |
| `domain` | Scan request, load, payload-audit, and broker metadata types |
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
state. All 20 entries default to disabled.

| Owner | Count | Processor IDs |
|---|---:|---|
| Octopus | 24 | `sync-scan-ingestion`, `sync-job-planner`, `sync-backlog-recovery`, `sync-load-dispatch`, `sync-load-consumer`, `sync-result-consumer`, `sync-outbox-publisher`, `wireless-heartbeat-ingestion`, `wireless-frame-normalizer`, `wireless-inventory-projector`, `wireless-identity-projector`, `wireless-behavior-projector`, `wireless-timing-projector`, `wireless-sequence-projector`, `wireless-baseline-projector`, `wireless-similarity-projector`, `threat-risk-projector`, `embedding-preparer`, `embedding-text-builder`, `rf-alert-projector`, `event-retention`, `search-retention`, `stale-worker-cleanup`, `scheduled-reconciliation` |
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

The additional currently consumed topic is `wireless.sensor.heartbeat`.
Request/reply destinations are validated before
publication. Non-retryable poison messages go to `<source-topic>.dlq`.

`wireless-heartbeat-ingestion` translates each `wireless.sensor.heartbeat`
record into a scan request with an explicit `event_id` or a payload-SHA-256
fallback. Empty or invalid payloads are parked on
`wireless.sensor.heartbeat.dlq` before the consumer commits the offset.
Retryable persistence failures propagate to processor supervision, which
applies bounded exponential backoff; permanent failures are parked on the same
DLQ so poison does not redeliver forever.

Hydration and load planning use the configured ingest stream names (defaults
include `proxy.events`, `wireless.audit`, wireless alerts, and
`proxy.payload_audit`). Override with `SYNC_STREAM_NAMES` /
`COORDINATOR_STREAM_NAMES` and `COORDINATOR_LOAD_STREAM_NAMES`.

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
| `postgres.statement-timeout-secs` | `POSTGRES_STATEMENT_TIMEOUT_SECS` | Optional, default 30; transaction-local PostgreSQL statement timeout for sink transactions only |
| `postgres.network-timeout-secs` | `POSTGRES_NETWORK_TIMEOUT_SECS` | Optional, default 60; JDBC network timeout for sink attempts, restored before connection reuse |
| `kafka` | `SYNC_*`, legacy `COORDINATOR_*` aliases | Positive polling/batch/partition/replication bounds, versioned consumer groups, earliest retained startup for new groups, manual commit after durable processing, and one shared `SYNC_DLQ_SUFFIX` for locked and wireless consumers |
| `kafka.*-consumers-count` | `SYNC_SCAN_CONSUMERS_COUNT`, `SYNC_LOAD_CONSUMERS_COUNT`, `SYNC_RESULT_CONSUMERS_COUNT` | Optional, default 4; maximum concurrently processed batches per locked topic. Every assigned partition remains drainable; this is not a Kafka client count |
| `kafka.locked-batch-max-bytes` | `COORDINATOR_LOCKED_BATCH_MAX_BYTES` | Optional, default 8388608; positive cumulative serialized-value byte limit, alongside count/time limits; oversized records are parked in the existing DLQ before offset commit |
| `postgres.load-chunk-max-bytes` | `POSTGRES_LOAD_CHUNK_MAX_BYTES` | Optional, default 4194304; positive cumulative JSON-row byte limit alongside 500 rows; oversized individual rows fail the load permanently |
| `cron` | `COORDINATOR_*`, `SCHEMA_REFRESH_INTERVAL_SECS` | Every interval, attempt count, lease, fetch count, and batch size must be positive |
| `backpressure` | `COORDINATOR_BACKPRESSURE_*`, `COORDINATOR_ADAPTIVE_PULL_*` | Multiplier, change threshold, and restart interval must be positive |
| `wireless` | `WIRELESS_*` | Consumer count and poll bound must be positive; topics and versioned groups are required for an enabled consumer lane |
| `processors` | `OCTOPUS_PROCESSOR_*`, `OCTOPUS_ENABLED_PROCESSORS`, similarity/distance variables | Enabled IDs must be Octopus-owned with dependencies enabled; delays, interval, and batch size positive; scores finite and in range |
| `archive` | `OCTOPUS_ARCHIVE_ENABLED`, `MINIO_*`, retention and archive variables | Credentials and bucket required when enabled; retention ordering, intervals, and batch size validated |

Both sink timeouts must be positive, at most 2147483 seconds, and the network
timeout must exceed the statement timeout. Connection acquisition remains five
seconds (`POSTGRES_CONNECTION_TIMEOUT_MS=5000`); health queries retain their
five-second timeout. Each sink attempt uses parameterized transaction-local
`set_config('statement_timeout', ?, true)` before business SQL. Repository
transactions retain their existing timeout behavior. Sink retries use three
total attempts with 200/400 ms backoff after releasing each connection.

Scan admission pauses at the pending-ledger high watermark and resumes at half
that watermark. Load/result consumers keep draining because completing their
work releases the backlog. They pull bounded batches only as database processing
completes. Partition-local processing preserves offset order; a shared permit
caps active batch work. Kafka prefetch is one batch per partition, fetch max is
the configured batch byte limit, and partition fetch max is at most 1 MiB.
Kafka may exceed fetch limits for its first oversized record batch, so these are
not hard broker-allocation limits.

The metrics endpoint exports Micrometer JVM memory (heap/non-heap including
metaspace), buffer pool memory (`id="direct"`), and GC pause timers. Timer
series appear after a collection. Consumer metrics are sampled every 10 seconds
with a 5-second timeout: `coordinator_kafka_partition_lag_value` reports native
Kafka fetch-position lag, tagged by group/topic/partition, and
`coordinator_kafka_rebalances_value` reports the client rebalance total.
Fetch-position lag is not durable committed-offset lag. Revoked-partition
series are removed on the next sample; stopping the consumer removes its series.

### Read-only timeout incident procedure

Correlate `postgres_attempt` timestamps and `operation`, `attempt`, `outcome`,
`acquisition_ms`, `transaction_ms`, and `elapsed_ms`. Outcomes include `retrying`,
`exhausted`, `permanent_failure`, and `recovered`; ordinary success is DEBUG.
Failures include bounded SQLSTATE/vendor-code classifications without driver
messages or parameters. Pool snapshots are `pool_active`, `pool_idle`, and
`pool_waiting` (sampled after connection release).

1. Compare acquisition time and pool waiting counts with PgBouncer `SHOW POOLS`
   and `SHOW STATS` using the approved read-only monitoring connection. Check
   waiting clients and server availability in the same time window.
2. On PostgreSQL, inspect `pg_stat_activity` for `pid`, `state`, `xact_start`,
   `query_start`, `wait_event_type`, `wait_event`, and `pg_blocking_pids(pid)`.
   Avoid exporting query text, which may contain values. Compare blockers and
   wait events with transaction duration and SQLSTATE `57014` cancellations.
3. Compare consumer-group lag with retry/exhaustion timestamps, JVM GC pause
   telemetry, and container CPU throttled periods/time. Preserve the existing
   Cats Effect starvation warning: temporal proximity does not establish a
   shared cause. A JDBC transaction already runs on `IO.blocking`.
4. Save the time window and safe diagnostic fields for review. Do not change
   pool size, concurrency, CPU, database settings, offsets, or managed workloads
   during this procedure. Production changes follow reviewed Git/Argo CD delivery.

Unknown or obsolete overrides are not alternate configuration sources. In
particular, `WIRELESS_DLQ_SUFFIX` is retired; use `SYNC_DLQ_SUFFIX` for every
consumer DLQ.

Important gates:

| Variable | Default | Meaning |
|---|---|---|
| `POSTGRES_ENABLED` | `false` | Enables PostgreSQL after TLS, least-privilege, and schema validation |
| `POSTGRES_SCHEMA_MANIFEST_SHA256` | bundled `octopus_core` manifest digest | Exact executor-recorded canonical schema digest; startup and periodic verification fail closed on drift |
| `OCTOPUS_CONSUMERS_ENABLED` | `false` | Enables the Kafka consumer processor set (`ProcessorId.kafkaConsumers`): the three locked consumers plus `wireless-heartbeat-ingestion`. Those IDs do not have to be repeated in `OCTOPUS_ENABLED_PROCESSORS`. |
| `OCTOPUS_PROCESSORS_ENABLED` | `false` | Enables the processor lane (cron, projections, retention, and support streams) |
| `OCTOPUS_ENABLED_PROCESSORS` | `[]` | Comma-separated Octopus-owned processor IDs for the processor catalog. If this list is non-empty while consumers are enabled, it must include the three locked consumers. |
| `OCTOPUS_PROCESSOR_RESTART_BASE_DELAY_MS` | `1000` | Initial retry delay |
| `OCTOPUS_PROCESSOR_RESTART_MAX_DELAY_MS` | `30000` | Maximum retry delay |
| `OCTOPUS_PROCESSOR_BATCH_SIZE` | `250` | Bound for normalized projection and search-preparation passes |
| `OCTOPUS_PROCESSOR_INTERVAL_SECONDS` | `10` | Periodic search preparation interval |
| `OCTOPUS_EMBEDDING_MODEL` | `nomic-embed-text-v2-moe` | Only supported model; attached to newly prepared embedding jobs. Other values fail startup validation. |
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
`POSTGRES_PASSWORD` (or the preferred, mutually exclusive `POSTGRES_PASSWORD_FILE`),
`POSTGRES_POOL_SIZE`, and explicit `POSTGRES_SSL_MODE`. Canonical
Kustomize sets `verify-full` with the PgBouncer listener CA and
`POSTGRES_SSL_SERVER_NAME=postgres-pgbouncer`; the listener private key is never
mounted in Octopus. Enabled runtime rejects loopback, root accounts,
warn-only schema validation, and invalid consumer-group contracts.

The checked-in Kubernetes deployment sets `POSTGRES_ENABLED`,
`OCTOPUS_PROCESSORS_ENABLED`, `OCTOPUS_CONSUMERS_ENABLED`, and archival. The
processor catalog lists the 14 periodic/locked-load processors; the remaining
four Kafka consumer processor IDs start because `OCTOPUS_CONSUMERS_ENABLED`
is true, not because they appear in `OCTOPUS_ENABLED_PROCESSORS`. Together that
is all 24 Octopus-owned processors.

A new consumer group replays every retained record; an existing group resumes
from its committed Kafka offsets. Rollback must preserve consumer offsets,
schemas, and ingestion evidence.

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

Coverage and BDD commands:

```bash
sbt jacoco
sbt "testOnly com.sslproxy.coordinator.bdd.RunCucumberTest"
python3 scripts/check_coverage.py target/scala-3.3.8/jacoco/report/jacoco.xml
```

`sbt jacoco` writes its HTML report to
`target/scala-3.3.8/jacoco/report/html/index.html` and machine-readable XML
and CSV reports beside it. `coverage-policy.json` records the overall and
package floors for `config`, `persistence`, `processor`, `postgres`,
`dispatch`, `http`, and `observability`. CI runs this coverage task with
Docker-backed tests required, enforces the committed policy, and retains both
the JaCoCo report and the Cucumber HTML, JSON, and JUnit reports as the
`octopus-test-reports` artifact.

Repository-level checks include:

```bash
python3 scripts/check-postgres-schema-contract.py
make dependency-boundaries
```

Docker-backed PostgreSQL tests skip when Docker is unavailable during local
development. CI sets `OCTOPUS_REQUIRE_DOCKER=true`, turning an unavailable
Docker daemon into a test failure so missing integration coverage cannot pass
silently.
