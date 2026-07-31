# Octopus

A durable ingestion coordinator that consumes events from Kafka, transforms raw JSON into typed database rows, and persists to TiDB. Built with Scala 3, Cats Effect, FS2, Doobie, and fs2-kafka.

Octopus is the sole owner of durable ingestion state in TiDB for the four-domain system (proxy, wireless, audit, alerts).

---

## What It Does

The coordinator is the durable ingestion owner for the four-domain system:

- **Ingests events** from Redpanda (`sync.scan.request`) — scan requests, proxy payloads, wireless probes — and records them in TiDB.
- **Manages batch lifecycle** — moves events through a state machine (`pending → batched → dispatched → completed/failed`) with idempotent, deduplicated writes.
- **Transforms payloads** — converts raw JSON into typed database rows with field validation, alias resolution, and type coercion (10 target schemas).
- **Dispatches batches** to TiDB workers via `sync.oracle.load` and handles their results from `sync.oracle.result`.
- **Handles wireless operations** — 7 independent request/reply and fire-and-forget handlers for wireless sensor data (backlog management, MAC address lookup, authorized networks, probe flush).
- **Applies backpressure** — suspends scan consumption when the pending ledger exceeds a configurable budget.
- **Generates shadow device alerts** — detects wireless MACs with strong signal but no proxy presence.
- **Exposes health and metrics** via http4s endpoint and OpenTelemetry.

---

## Architecture

The coordinator is a **cron-driven loop** implemented with FS2 streams. The main loop ticks at the configured interval (default 250 ms) and sequences through these phases:

```text
cron:coordinator-loop
  │
  ├─ adaptivePull          Shrink Kafka fetch size when DB is behind
  ├─ backpressureV2        Suspend scan consumer when pending count ≥ budget
  ├─ processScanRequests   Delegate to scan request consumer
  ├─ processIngest         Promote pending events → jobs → batches
  ├─ recoverStaleBatches   Recover dispatched-but-stale batches
  ├─ dispatchBatches       Get next batch → publish to sync.oracle.load
  ├─ handleResults         Delegate to result consumer
  ├─ shadowAudit           Rate-limited shadow device alert generation
  ├─ wirelessOperations    Delegate to 7 wireless handlers
  └─ heartbeat             Record loop counter, stream states, emit heartbeat log
```

**Independent FS2 Kafka consumer streams** (not in the main loop):

- Scan request consumer — consumes `sync.scan.request`, accumulates into batches, flushes to TiDB
- Result consumer — consumes `sync.oracle.result` and records batch/job outcomes and monotonic cursor progress in TiDB
- TiDB load consumer — consumes `sync.oracle.load`, performs TiDB sink operations, publishes results to `sync.oracle.result`
- Wireless handlers — 7 independent consumer streams for wireless operations

**Timer-based background streams**:

- `wireless-payload-archive` — periodically archives old wireless payloads to MinIO
- `retention-prune` — periodically prunes expired events and tombstones from TiDB
- Scan request flush timer — flushes partial scan record batches every 1 second
- Result flush timer — flushes partial result batches every 1 second

### Stream Map

| Stream ID | Type | Source | Description |
|---|---|---|---|
| `coordinator-main-loop` | cron | `cron:coordinator-loop` | Drives the main state-machine loop |
| `coordinator-adaptive-pull` | direct | FS2 stream | Shrinks Kafka fetch size when DB is behind |
| `coordinator-backpressure` | direct | FS2 stream | Suspends/resumes scan consumer based on pending count |
| `coordinator-scan-requests` | direct | FS2 stream | Pass-through (delegation marker) |
| `coordinator-ingest` | direct | FS2 stream | Promotes pending events through jobs into batches |
| `coordinator-stale-recovery` | direct | FS2 stream | Calls `coordinator.recover_stale_dispatched_batches()` |
| `coordinator-dispatch` | direct | FS2 stream | Loops dispatching batches to `sync.oracle.load` |
| `coordinator-results` | direct | FS2 stream | Pass-through (delegation marker) |
| `coordinator-shadow-audit` | direct | FS2 stream | Rate-limited shadow alert generation |
| `coordinator-wireless` | direct | FS2 stream | Pass-through (delegation marker) |
| `coordinator-heartbeat` | direct | FS2 stream | Metrics + heartbeat log |
| `scan-request-consumer` | kafka | `sync.scan.request` | Consumes scan requests, records in DB |
| `scan-request-flush-timer` | timer | 1s interval | Flushes partial scan batches |
| `tidb-load-consumer` | kafka | `sync.oracle.load` | TiDB sink worker (conditional on `tidb-sink.enabled`) |
| `result-consumer` | kafka | `sync.oracle.result` | Records batch/job outcomes and monotonic cursor progress in TiDB |
| `result-flush-timer` | timer | 1s interval | Flushes partial result batches |
| `wireless-backlog-save` | kafka | `wireless.backlog.save` | Saves wireless backlog entry |
| `wireless-backlog-list` | kafka | `wireless.backlog.list` | Lists pending backlog, publishes reply |
| `wireless-backlog-synced` | kafka | `wireless.backlog.synced` | Marks backlog entry synced |
| `wireless-backlog-prune` | kafka | `wireless.backlog.prune` | Prunes backlog, publishes reply |
| `wireless-mac-lookup` | kafka | `wireless.mac.lookup` | Looks up device by MAC, publishes reply |
| `wireless-networks-authorized` | kafka | `wireless.networks.authorized` | Lists authorized networks, publishes reply |
| `wireless-probe-flush` | kafka | `wireless.probe.flush` | Flushes probe batch |
| `wireless-payload-archive` | timer | configurable interval | Archives old payloads to MinIO |
| `retention-prune` | timer | configurable interval | Prunes expired events and tombstones |

---

## Redpanda Topics

The coordinator interacts with the following Redpanda topics:

### Consumed Topics

| Topic | Consumer Group | Stream | Purpose |
|---|---|---|---|
| `sync.scan.request` | `octopus-scan` | `scan-request-consumer` | Ingest scan request events |
| `sync.oracle.load` | `octopus-load` | `tidb-load-consumer` | TiDB sink workload |
| `sync.oracle.result` | `octopus-result` | `result-consumer` | Batch results |
| `proxy.payload_audit` | `octopus-payload-audit` | (future) | Payload audit events |
| `wireless.backlog.save` | `wireless-backlog-save` | `wireless-backlog-save` | Save wireless backlog entry |
| `wireless.backlog.list` | `wireless-backlog-list` | `wireless-backlog-list` | List pending backlog |
| `wireless.backlog.synced` | `wireless-backlog-synced` | `wireless-backlog-synced` | Mark backlog synced |
| `wireless.backlog.prune` | `wireless-backlog-prune` | `wireless-backlog-prune` | Prune backlog |
| `wireless.mac.lookup` | `wireless-mac-lookup` | `wireless-mac-lookup` | MAC address lookup |
| `wireless.networks.authorized` | `wireless-networks-authorized` | `wireless-networks-authorized` | List authorized networks |
| `wireless.probe.flush` | `wireless-probe-flush` | `wireless-probe-flush` | Flush probe data |

### Produced Topics

| Topic | Producer | Purpose |
|---|---|---|
| `sync.oracle.load` | `coordinator-dispatch` stream | Dispatch batches to TiDB workers |
| `sync.oracle.result` | `tidb-load-consumer` stream | TiDB worker output |
| `wireless.backlog.list.reply` | `wireless-backlog-list` stream | Reply to backlog list requests |
| `wireless.backlog.prune.reply` | `wireless-backlog-prune` stream | Reply to backlog prune requests |
| `wireless.mac.lookup.reply` | `wireless-mac-lookup` stream | Reply to MAC lookup requests |
| `wireless.networks.authorized.reply` | `wireless-networks-authorized` stream | Reply to authorized networks requests |
| DLQ topics | Error handler | `{topic}.dlq` for each consumed topic |

### Stream Names

The coordinator tracks cursor positions per **stream name**. Configured streams:
- `proxy.events`
- `wireless.audit`
- `audit.wireless.bandwidth`
- `wireless.alert.rogue_ap`
- `wireless.alert.deauth_flood`
- `wireless.alert.signal_anomaly`
- `wireless.alert.pmf_attack`
- `wireless.client.inventory`
- `wireless.probe.flush`
- `proxy.payload_audit`

A subset of these (`tidb-stream-names`) are additionally dispatched to TiDB workers.

---

## Wireless Operations

Seven independent Kafka consumer streams handle wireless sensor operations. Each has its own consumer group for independent consumption and offset management.

| Handler | Pattern | DB Function |
|---|---|---|
| `backlog-save` | Fire-and-forget | `coordinator.save_backlog_entry()` |
| `backlog-list` | Request/Reply | `coordinator.list_pending_backlog()` |
| `backlog-synced` | Fire-and-forget | `coordinator.mark_backlog_synced()` |
| `backlog-prune` | Request/Reply | `coordinator.prune_backlog()` |
| `mac-lookup` | Request/Reply | `coordinator.lookup_device_by_mac()` |
| `networks-authorized` | Request/Reply | `coordinator.list_authorized_networks()` |
| `probe-flush` | Fire-and-forget | `coordinator.flush_probe_batch()` |

Request/reply handlers parse an optional `reply_topic` field from the incoming JSON payload. If provided, the reply is published to that topic; otherwise it goes to the configured default reply topic. Reply topics are validated against an allowlist.

---

## TiDB Integration

The coordinator connects to TiDB via JDBC:

- **TLS authentication** using standard MySQL TLS or password auth.
- **Schema validation** with optional `warn-only` mode for graceful degradation.
- **Error classification** into permanent vs. retryable errors.
- **Configurable connection pool**.
- **Health check** via the http4s health endpoint.

TiDB is required for runtime operation (`tidb.enabled=true`).

---

## Configuration

The coordinator is configured exclusively through environment variables, loaded via pureconfig.

### Core Settings

| Variable | Default | Description |
|---|---|---|
| `SYNC_STREAM_NAME` | `proxy.events` | Primary stream name |
| `SYNC_STREAM_NAMES` | (see reference.conf) | Comma-separated list of all stream names |
| `SYNC_TIDB_STREAM_NAMES` | (see reference.conf) | Stream names dispatched to TiDB |
| `SYNC_REDPANDA_BOOTSTRAP_SERVERS` | `localhost:9092` | Redpanda/Kafka bootstrap servers |
| `TIDB_ENABLED` | `false` | Required: explicitly set to `true` for runtime startup |
| `TIDB_HOST` | `127.0.0.1` | TiDB host; loopback is rejected when TiDB is enabled |
| `TIDB_PORT` | `4000` | TiDB port |
| `TIDB_DATABASE` | `coordinator` | TiDB database |
| `TIDB_USER` | `root` | Disabled-mode default; enabled TiDB requires a non-root account |
| `TIDB_PASSWORD` | *(empty)* | Required when TiDB is enabled |
| `TIDB_POOL_SIZE` | `4` | Hikari/Doobie connection pool size |

### Redpanda Topics

| Variable | Default | Description |
|---|---|---|
| `SYNC_SCAN_TOPIC` | `sync.scan.request` | Scan request topic |
| `SYNC_LOAD_TOPIC` | `sync.oracle.load` | Load topic (legacy name) |
| `SYNC_RESULT_TOPIC` | `sync.oracle.result` | Result topic (legacy name) |
| `SYNC_PAYLOAD_AUDIT_TOPIC` | `proxy.payload_audit` | Payload audit topic |
| `WIRELESS_BACKLOG_SAVE_TOPIC` | `wireless.backlog.save` | Backlog save topic |
| `WIRELESS_BACKLOG_LIST_TOPIC` | `wireless.backlog.list` | Backlog list topic |
| `WIRELESS_BACKLOG_SYNCED_TOPIC` | `wireless.backlog.synced` | Backlog synced topic |
| `WIRELESS_BACKLOG_PRUNE_TOPIC` | `wireless.backlog.prune` | Backlog prune topic |
| `WIRELESS_MAC_LOOKUP_TOPIC` | `wireless.mac.lookup` | MAC lookup topic |
| `WIRELESS_NETWORKS_AUTHORIZED_TOPIC` | `wireless.networks.authorized` | Networks authorized topic |
| `WIRELESS_PROBE_FLUSH_TOPIC` | `wireless.probe.flush` | Probe flush topic |

### Consumer Groups

| Variable | Default | Description |
|---|---|---|
| `SYNC_SCAN_CONSUMER` | `octopus-scan` | Scan request consumer group |
| `SYNC_LOAD_CONSUMER` | `octopus-load` | Load consumer group |
| `SYNC_RESULT_CONSUMER` | `octopus-result` | Result consumer group |
| `SYNC_PAYLOAD_AUDIT_CONSUMER` | `octopus-payload-audit` | Payload audit consumer group |
| `WIRELESS_BACKLOG_SAVE_CONSUMER` | `wireless-backlog-save` | Backlog save consumer group |
| `WIRELESS_BACKLOG_LIST_CONSUMER` | `wireless-backlog-list` | Backlog list consumer group |
| `WIRELESS_BACKLOG_SYNCED_CONSUMER` | `wireless-backlog-synced` | Backlog synced consumer group |
| `WIRELESS_BACKLOG_PRUNE_CONSUMER` | `wireless-backlog-prune` | Backlog prune consumer group |
| `WIRELESS_MAC_LOOKUP_CONSUMER` | `wireless-mac-lookup` | MAC lookup consumer group |
| `WIRELESS_NETWORKS_AUTHORIZED_CONSUMER` | `wireless-networks-authorized` | Networks authorized consumer group |
| `WIRELESS_PROBE_FLUSH_CONSUMER` | `wireless-probe-flush` | Probe flush consumer group |

### Backpressure & Tuning

| Variable | Default | Description |
|---|---|---|
| `SYNC_INGEST_BATCH_SIZE` | `1000` | Ingest batch size |
| `SYNC_DISPATCH_BATCH_SIZE` | `50` | Dispatch batch size |
| `SYNC_BACKPRESSURE_BUDGET_MULTIPLIER` | `4` | Budget = `ingest_batch_size × multiplier` |
| `SYNC_IDLE_SLEEP_MS` | `250` | Main loop interval |
| `SYNC_IDLE_SLEEP_BACKOFF_MS` | `1000` | Backoff when idle |
| `SYNC_SCAN_FETCH_COUNT` | `500` | Kafka `maxPollRecords` for scan consumer |
| `SYNC_RESULT_FETCH_COUNT` | `200` | Kafka `maxPollRecords` for result consumer |
| `SYNC_SCAN_CONSUMERS_COUNT` | `1` | Concurrent consumers for scan topic |
| `SYNC_RESULT_CONSUMERS_COUNT` | `1` | Concurrent consumers for result topic |
| `WIRELESS_CONSUMERS_COUNT` | `1` | Concurrent consumers per wireless handler |
| `WIRELESS_MAX_POLL_RECORDS` | `1` | Max poll records for wireless handlers |
| `SYNC_ADAPTIVE_PULL_CHANGE_THRESHOLD` | `50` | Threshold for adaptive pull adjustments |

### Retention & Archiving

| Variable | Default | Description |
|---|---|---|
| `WIRELESS_RAW_ARCHIVE_ENABLED` | `true` | Enable archiving old payloads to MinIO |
| `WIRELESS_RAW_PAYLOAD_HOT_DAYS` | `7` | Days before payloads are eligible for archiving |
| `SYNC_EVENT_ROW_RETENTION_DAYS` | `30` | Event row retention |
| `SYNC_EVENT_TOMBSTONE_RETENTION_DAYS` | `45` | Tombstone retention |
| `WIRELESS_RAW_ARCHIVE_BATCH_SIZE` | `100` | Batch size for archive operations |
| `RETENTION_PRUNE_BATCH_SIZE` | `5000` | Batch size for retention pruning |
| `WIRELESS_RAW_ARCHIVE_INTERVAL_MS` | `300000` (5 min) | Archive cycle interval |
| `RETENTION_MAINTENANCE_INTERVAL_MS` | `3600000` (1 hr) | Retention prune interval |
| `WIRELESS_RAW_ARCHIVE_BUCKET` | `ssl-proxy-wireless-raw-archive` | MinIO bucket name |
| `MINIO_ENDPOINT` | `http://minio:9000` | MinIO server endpoint |
| `MINIO_ACCESS_KEY_ID` | *(required when archive enabled)* | MinIO access key |
| `MINIO_SECRET_ACCESS_KEY` | *(required when archive enabled)* | MinIO secret key |

### TiDB

| Variable | Default | Description |
|---|---|---|
| `TIDB_ENABLED` | `false` | Enable TiDB (required for runtime) |
| `TIDB_HOST` | `127.0.0.1` | TiDB host; must be non-loopback when enabled |
| `TIDB_PORT` | `4000` | TiDB port |
| `TIDB_DATABASE` | `coordinator` | TiDB database |
| `TIDB_USER` | `root` | Disabled-mode default; a least-privilege non-root account is required when enabled |
| `TIDB_PASSWORD` | *(empty)* | Required when enabled |
| `TIDB_POOL_SIZE` | `4` | Connection pool size |
| `TIDB_CONNECTION_TIMEOUT_MS` | `5000` | Pool connection timeout |
| `TIDB_STATEMENT_TIMEOUT_SECS` | `30` | Statement/network timeout |
| `TIDB_WARN_ONLY` | `false` | Schema validation mode; must remain `false` when enabled |
| `TIDB_SSL_MODE` | `VERIFY_IDENTITY` | TLS mode; must remain `VERIFY_IDENTITY` when enabled |
| `TIDB_SSL_CA_PATH` | *(empty)* | CA bundle path; required when enabled |
| `TIDB_SSL_SERVER_NAME` | *(empty)* | TLS server name; must equal `TIDB_HOST` when enabled |
| `TIDB_LOCAL_DEV_ALLOW_PUBLIC_KEY_RETRIEVAL` | `false` | Explicit opt-in for non-TLS local development only |

### Observability

| Variable | Default | Description |
|---|---|---|
| `HTTP_PORT` | `8081` | http4s health/metrics port |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP trace endpoint |
| `LOG_LEVEL` | `info` | Log level |

---

## Building

### Prerequisites

- JDK 21 (GraalVM or compatible)
- sbt 1.x

### Build the Assembly

```sh
sbt assembly
```

This compiles, runs tests, and produces a fat JAR at `target/scala-3.*/octopus-assembly-*.jar`.

### Run Tests

```sh
sbt test
```

Tests include:
- Unit tests for all packages
- MUnit and MUnit Cats Effect tests for effectful code
- SQL contract tests that validate stored procedure call signatures

---

## Running

### Local Development

```sh
# Start the local non-database dependencies. Use an externally reachable
# development TiDB endpoint because enabled TiDB rejects loopback hosts.
docker compose up -d redpanda minio

# Set required env vars and run
export TIDB_ENABLED=true
export TIDB_HOST=tidb.dev.internal
export TIDB_PORT=4000
export TIDB_DATABASE=octopus_core
export TIDB_USER=octopus_runtime
export TIDB_PASSWORD=...
export TIDB_SSL_MODE=VERIFY_IDENTITY
export TIDB_SSL_CA_PATH=/path/to/tidb-ca.crt
export TIDB_SSL_SERVER_NAME=tidb.dev.internal
export SYNC_REDPANDA_BOOTSTRAP_SERVERS=localhost:9092
export MINIO_ACCESS_KEY_ID=...
export MINIO_SECRET_ACCESS_KEY=...

sbt run
```

### Docker

```sh
docker run -d \
  -p 8081:8081 \
  -v /path/to/tidb-ca.crt:/etc/tidb-tls/ca.crt:ro \
  -e TIDB_ENABLED=true \
  -e TIDB_HOST=tidb.dev.internal \
  -e TIDB_PORT=4000 \
  -e TIDB_DATABASE=octopus_core \
  -e TIDB_USER=octopus_runtime \
  -e TIDB_PASSWORD=... \
  -e TIDB_SSL_MODE=VERIFY_IDENTITY \
  -e TIDB_SSL_CA_PATH=/etc/tidb-tls/ca.crt \
  -e TIDB_SSL_SERVER_NAME=tidb.dev.internal \
  -e SYNC_REDPANDA_BOOTSTRAP_SERVERS=host.docker.internal:9092 \
  ssl-proxy/octopus:latest
```

### Kubernetes / Helm

The coordinator is deployed as part of the `ssl-proxy` Helm chart (located in the parent repository at `helm/ssl-proxy/`). A typical values override:

```yaml
global:
  shared:
    tidb:
      host: tidb.example.internal
      port: 4000
      tls:
        serverName: tidb.example.internal
        caSecret:
          name: tidb-client-ca
          key: ca.crt
      accounts:
        octopus:
          database: octopus_core
          user: octopus_runtime
          passwordSecret:
            name: tidb-octopus
            key: password

javaCoordinator:
  enabled: true
  image:
    repository: ssl-proxy/octopus
    tag: latest
  tidb:
    enabled: true
    sslMode: VERIFY_IDENTITY
```

---

## Adding a New Redpanda Topic

Here is the complete workflow for wiring a new Redpanda topic into the coordinator.

### 1. Bootstrap the Topic

Add the topic to the `topics.manifest` file in the parent repo (`docker/redpanda/topics.manifest`), then run the bootstrap script:

```sh
export SYNC_REDPANDA_BOOTSTRAP_SERVERS=redpanda:9092
./setup-wireless-redpanda.sh
```

### 2. Add Configuration

In the config package (`src/main/scala/com/sslproxy/coordinator/config/`), add topic and consumer group fields to the relevant case class.

### 3. Add Environment Variable Defaults

In `src/main/resources/reference.conf`:

```
new-topic-name = ${NEW_TOPIC_NAME}
new-topic-consumer = ${NEW_TOPIC_CONSUMER}
```

### 4. Create the FS2 Stream

Create a new consumer stream in the `kafka/` package:

```scala
def newTopicStream(consumer: KafkaConsumer[IO, String, String], db: DatabaseService): Stream[IO, Unit] =
  consumer.stream
    .evalMap { committable =>
      db.processPayload(committable.record.value)
        .as(committable)
    }
    .through(commitBatchWithin(100, 5.seconds))
```

Wire it into the coordinator's resource management.

### 5. For Request/Reply Patterns

If the new topic follows a request/reply pattern, add a reply topic configuration and follow the pattern in the wireless handler package.

### 6. Register in the Main Loop (if needed)

If the new topic needs timing parity with the main loop, add a step in the cron cycle.

### 7. Add to Stream Names (if applicable)

If the new topic represents a new event stream that needs cursor tracking, add it to `SYNC_STREAM_NAMES` and/or `SYNC_TIDB_STREAM_NAMES`.

### 8. Update Backpressure (if applicable)

If the new topic contributes to backpressure, it is automatically accounted for in the `pendingLedgerCount()` DB query.

---

## Helm Deployment

The coordinator is deployed via the parent repo's `helm/ssl-proxy/` chart. Key configuration points:

- **ConfigMap**: Environment variables are typically set via a ConfigMap or `values.yaml` env overrides.
- **Secrets**: Database passwords, MinIO credentials, and TiDB passwords should be mounted from Kubernetes Secrets.
- **Readiness/Health**: http4s health endpoint is checked via Kubernetes liveness/readiness probes.
- **Graceful shutdown**: The coordinator handles `SIGTERM` by cancelling all FS2 streams and draining in-flight work.

---

## Metrics & Observability

| Endpoint | Description |
|---|---|
| `/health` | Health check (DB, TiDB, Redpanda) |
| `/metrics` | Prometheus scrape endpoint |

### Key Metrics

| Metric | Type | Description |
|---|---|---|
| `db.client.operation` | Timer | TiDB operation duration for Doobie-based persistence |
| `coordinator.loop.counter` | Counter | Main loop iterations |
| `coordinator.batch.dispatched` | Counter | Successfully dispatched batches |
| `coordinator.backpressure.active` | Gauge | Whether scan consumer is suspended |
| `coordinator.backpressure.pending_count` | Gauge | Current pending ledger count |

### Tracing (OpenTelemetry)

OTLP traces are exported to the endpoint configured by `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4317` gRPC).

### Health Checks

- TiDB connection validation
- Kafka consumer stream health

---

## Testing

```sh
# Run all tests
sbt test

# Run a specific test class
sbt "testOnly com.sslproxy.coordinator.tidb.TiDBSinkSpec"
```

The test suite uses:
- **MUnit** / **MUnit Cats Effect** — test framework
- **Doobie test support** — transactor testing
- **fs2-kafka test support** — consumer/producer test harnesses
- **SQL contract tests** — validate stored procedure call signatures against the root `sql/` files

---

## Project Structure

```text
services/octopus/
├── build.sbt                            # sbt build (Scala 3, Cats Effect, FS2)
├── project/
├── Dockerfile                           # Multi-stage build
├── tidb/init/                           # TiDB schema DDL
├── src/
│   ├── main/scala/com/sslproxy/coordinator/
│   │   ├── Main.scala                   # IOApp entry point
│   │   ├── config/                      # Pureconfig-backed configuration
│   │   ├── tidb/                        # TiDB transforms, checksums, error classification
│   │   ├── kafka/                       # FS2 Kafka consumer/producer streams
│   │   ├── cron/                        # Periodic ingest/batch/dispatch scheduler
│   │   ├── dispatch/                    # Kafka batch dispatch, backpressure
│   │   ├── ingest/                      # Payload audit consumer
│   │   ├── sink/                        # Generic TiDB sink, schema introspection
│   │   ├── cutover/                     # Signed cutover artifact verification
│   │   ├── domain/                      # Domain models
│   │   ├── errors/                      # Dead letter handling
│   │   ├── model/                       # System context
│   │   ├── observability/               # Metrics (Prometheus)
│   │   ├── processor/                   # Processor supervisor
│   │   ├── http/                        # http4s health endpoint
│   │   └── util/                        # SHA-256 utilities
│   └── test/scala/com/sslproxy/coordinator/
│       ├── tidb/
│       ├── kafka/
│       └── cron/
```

---

## License

Proprietary — SSL Proxy internal service.
