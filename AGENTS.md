# AGENTS.md

## Scope
This file governs `/Users/rcs/git/ssl-proxy/services/octopus`.

## Project Shape
- Scala 3 with Cats Effect 3, FS2, Doobie, http4s, fs2-kafka, Circe, sbt build.
- `src/main/scala/com/sslproxy/coordinator/config/` owns pureconfig-backed
  configuration with environment variable overrides.
- `postgres/` owns PostgreSQL connection setup, transforms, checksums, error classification,
  schema preflight, and sink behavior.
- `kafka/` owns FS2 Kafka consumer/producer streams for scan, load, and result topics.
- `cron/` owns the periodic ingest/batch/dispatch scheduler loop.
- `dispatch/` owns Kafka batch dispatch from PostgreSQL backlog.
- `http/` owns the http4s health endpoint.
- `src/test/scala/` includes MUnit and MUnit Cats Effect tests.

## Guardrails
- Keep this service the only PostgreSQL/PostgreSQL owner. PostgreSQL wiring should not migrate
  into Rust proxy, sensor, search, or shared crates.
- Preserve the locked topics and consumer roles for `sync.scan.request`,
  `sync.oracle.load`, and `sync.oracle.result`.
- Keep coordinator operations retry-safe and idempotent: cursor advancement,
  batch leasing, dispatch release/failure, backlog handling, and result
  processing must tolerate duplicate delivery.
- Keep PostgreSQL SQL DDL in `postgres/init/` and shared schema in root `sql/postgres/`.
- Do not commit sbt caches, IDE state, `.omx/` output, or generated local
  runtime files.

## Commands
- Run tests from this directory: `sbt test`.
- Build: `sbt assembly`.
- Root broad test target also runs coordinator tests: `make test`.

## Verification
- Run focused sbt tests for changed packages when practical, then
  `sbt test` for coordinator-wide changes.
- For PostgreSQL sink changes, cover schema preflight failure modes, retry
  classification, transform output, and disabled-sink behavior.
