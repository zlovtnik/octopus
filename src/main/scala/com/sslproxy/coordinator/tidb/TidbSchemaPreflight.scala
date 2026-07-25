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
        _ <-
          if missingTables.isEmpty && invalidColumns.isEmpty then
            IO(log.info(
              "tidb_schema_preflight",
              "status" -> "ok",
              "tables" -> allRequiredTables.size.toString,
              "column_types" -> requiredColumnTypes.size.toString
            ))
          else
            val problems = List(
              Option.when(missingTables.nonEmpty)(s"missing tables=[${missingTables.mkString(", ")}]"),
              Option.when(invalidColumns.nonEmpty)(s"invalid columns=[${invalidColumns.mkString(", ")}]")
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
