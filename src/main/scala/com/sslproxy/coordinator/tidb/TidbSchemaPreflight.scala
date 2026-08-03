package com.sslproxy.coordinator.tidb

import cats.effect.IO
import com.sslproxy.coordinator.config.TiDbConfig
import com.sslproxy.coordinator.observability.StructuredLogger

class TidbSchemaPreflight(transactor: TidbTransactor, config: TiDbConfig):
  import TidbSchemaPreflight.log

  private val allRequiredTables: List[String] = List(
    "sync_events",
    "sync_batches",
    "proxy_events",
    "proxy_blocked_host_rollups",
    "proxy_payload_audit",
    "wireless_sensors",
    "wireless_audit_frames",
    "wireless_bandwidth_windows",
    "wireless_alerts",
    "wireless_client_inventory",
    "wireless_probe_requests"
  )

  private val requiredColumnTypes: List[((String, String), String)] = List(
    ("sync_events" -> "payload_ref") -> "mediumtext",
    ("sync_batches" -> "payload_ref") -> "mediumtext"
  )

  def validate(): IO[Unit] =
    if !config.enabled then IO.unit
    else
      for
        missingTables <- transactor.preflightCheck(allRequiredTables)
        invalidColumns <- transactor.preflightCheckColumnTypes(requiredColumnTypes)
        readiness <- transactor.schemaReadiness("octopus_core")
        manifestProblems = readiness.toList.flatMap { row =>
          val expected = config.manifestSha256.toLowerCase(java.util.Locale.ROOT)
          List(
            Option.when(!row.ready)("schema_readiness.ready is false"),
            Option.when(row.appliedVersion.forall(_ != row.requiredVersion))(
              s"applied version ${row.appliedVersion.getOrElse("missing")} does not match required ${row.requiredVersion}"
            ),
            Option.when(row.requiredChecksum.toLowerCase(java.util.Locale.ROOT) != expected)(
              s"required checksum ${row.requiredChecksum} does not match configured $expected"
            ),
            Option.when(row.appliedChecksum.forall(value => !value.equalsIgnoreCase(expected)))(
              s"applied checksum ${row.appliedChecksum.getOrElse("missing")} does not match configured $expected"
            )
          ).flatten
        } ++ Option.when(readiness.isEmpty)("missing octopus_core schema_readiness row").toList
        _ <-
          if missingTables.isEmpty && invalidColumns.isEmpty && manifestProblems.isEmpty then
            IO(log.info(
              "tidb_schema_preflight",
              "status" -> "ok",
              "tables" -> allRequiredTables.size.toString,
              "column_types" -> requiredColumnTypes.size.toString,
              "manifest_sha256" -> config.manifestSha256
            ))
          else
            val problems = List(
              Option.when(missingTables.nonEmpty)(s"missing tables=[${missingTables.mkString(", ")}]"),
              Option.when(invalidColumns.nonEmpty)(s"invalid columns=[${invalidColumns.mkString(", ")}]"),
              Option.when(manifestProblems.nonEmpty)(s"manifest=[${manifestProblems.mkString(", ")}]")
            ).flatten.mkString("; ")
            val msg = s"TiDB schema preflight failed for database '${config.database}': $problems; " +
              "apply the canonical sql/tidb/octopus_core manifest with the tidb-runtime-schema executor"
            if config.warnOnly then
              IO(log.warn("tidb_schema_preflight", "status" -> "warn_only", "error" -> msg))
            else
              IO.raiseError(IllegalStateException(msg))
      yield ()

object TidbSchemaPreflight:
  private val log = StructuredLogger(getClass)
