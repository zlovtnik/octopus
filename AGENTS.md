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
- `wiring/` assembles resources, services, workloads, HTTP, and runtime streams;
  business packages retain their implementations.
- `cron/` and `dispatch/` own periodic ingest/batch/dispatch and outbox
  publication. `archive/` owns MinIO payload archival. `http/` owns health
  and metrics. `observability/` owns logs, Micrometer, and OTLP.
- Snapshot materialization lives in sibling `../octopus-metrics/`; the internal
  `/internal/metrics/live` route supplies only in-process live readings.
- `src/test/scala/` includes MUnit, MUnit Cats Effect, Cucumber glue in `bdd/`,
  `DocumentationContractSuite`, and `FeatureContractSuite`; features are one per `ProcessorFamily`.

## Guardrails
- Keep `proxy.payload_audit` and wireless operational topics retry-safe with
  DLQ parking for poison.
- `OCTOPUS_CONSUMERS_ENABLED` unions `ProcessorId.kafkaConsumers` into the
  enabled set. Do not assume every running consumer ID is listed in
  `OCTOPUS_ENABLED_PROCESSORS`.
- Every Octopus-owned `ProcessorId` needs a tagged scenario in its family feature; `FeatureContractSuite` rejects missing, unknown, and Atheros Search-owned tags.
- Do not lower `coverage-policy.json` floors for `persistence`, `processor`, `postgres`, `dispatch`, or `config` without explicit justification and a fresh Docker-enabled report.

## Commands
- Build: `sbt assembly`.
- Coverage: `sbt jacoco`; inspect `target/scala-3.3.8/jacoco/report/html/index.html`.
- BDD only: `sbt "testOnly com.sslproxy.coordinator.bdd.RunCucumberTest"`.

## Verification
- Run focused sbt tests for changed packages when practical, then
  `sbt test` for coordinator-wide changes.
- After README, topic, HTTP route, or runtime-gate edits, run
  `DocumentationContractSuite`.
- Update the matching Cucumber feature for Octopus processor behavior changes,
  then run `FeatureContractSuite`.
- For PostgreSQL sink changes, cover schema preflight failure modes, retry
  classification, transform output, and disabled-sink behavior.
