# AGENTS.md

## Scope
This file governs `/Users/rcs/git/ssl-proxy/services/octopus`.

## Project Shape
- Scala 3 with Cats Effect 3, FS2, Doobie, http4s, fs2-kafka, Circe, sbt.
- Package roots live under `src/main/scala/com/sslproxy/coordinator/`.
- `config/` owns PureConfig models, environment overrides, and fail-closed
  validation. `application.conf` is the accepted variable list.
- `ingest/` owns scan hydration and `proxy.payload_audit` consumption.
- `kafka/` owns locked scan/load/result consumers and wireless request/reply
  streams.
- `postgres/` and `postgres/sql/` own repositories, transforms, checksums,
  schema preflight, typed sinks, and named parameterized SQL.
- `processor/` owns IDs, catalog contracts, leases, retry, and supervision.
- `cron/` and `dispatch/` own periodic ingest/batch/dispatch and outbox
  publication. `archive/` owns MinIO payload archival. `http/` owns health
  and metrics. `observability/` owns logs, Micrometer, and OTLP.
- `src/test/scala/` includes MUnit and MUnit Cats Effect tests, including
  `DocumentationContractSuite` for README/topic/gate alignment.

## Guardrails
- Own durable ingestion, leases, outbox, and maintained projections. Do not
  move PostgreSQL wiring into the Rust proxy, sensor, or shared crates.
- Atheros Search is a separate PostgreSQL client for search/vectors; keep
  that grant boundary.
- Preserve locked topics `sync.scan.request`, `sync.oracle.load`, and
  `sync.oracle.result`. Keep `proxy.payload_audit` and wireless operational
  topics retry-safe with DLQ parking for poison.
- `OCTOPUS_CONSUMERS_ENABLED` unions `ProcessorId.kafkaConsumers` into the
  enabled set. Do not assume every running consumer ID is listed in
  `OCTOPUS_ENABLED_PROCESSORS`.
- Canonical DDL is `sql/postgres/` only. Octopus verifies manifest checksums
  and must not apply DDL at runtime.
- Keep cursor advancement, batch leasing, dispatch, backlog, and result
  handling idempotent under at-least-once delivery.
- Do not commit sbt caches, IDE state, `.omx/` output, or generated local
  runtime files.

## Commands
- Run tests from this directory: `sbt test`.
- Build: `sbt assembly`.
- Root broad test target also runs coordinator tests: `make test`.

## Verification
- Run focused sbt tests for changed packages when practical, then
  `sbt test` for coordinator-wide changes.
- After README, topic, HTTP route, or runtime-gate edits, run
  `DocumentationContractSuite`.
- For PostgreSQL sink changes, cover schema preflight failure modes, retry
  classification, transform output, and disabled-sink behavior.
