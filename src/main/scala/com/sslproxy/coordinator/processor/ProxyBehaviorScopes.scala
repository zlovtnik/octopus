package com.sslproxy.coordinator.processor

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.Json
import io.circe.syntax.*
import java.nio.charset.StandardCharsets
import java.time.{Instant, OffsetDateTime}
import scala.util.Try

/** Observed registration epochs and process peers, never physical ownership. */
object ProxyBehaviorScopes:
  final case class Attribution(
      kind: String,
      key: Option[String],
      method: String,
      validity: String,
      peer: Option[String],
      mapping: Option[String],
      registration: Option[String],
      from: Option[Instant],
      until: Option[Instant]
  )

  def text(row: Json, key: String): Option[String] =
    row.hcursor.get[String](key).toOption
  def instant(value: String): Option[Instant] = Try(
    OffsetDateTime.parse(value).toInstant
  ).toOption
  private def opaque(parts: String*): String =
    Sha256Utils.sha256Hex(
      parts.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
    )

  def peerKey(deployment: String, reference: String): String =
    opaque(deployment, "peer", reference)

  def attribution(deployment: String, row: Json, at: Instant): Attribution =
    val c = row.hcursor.downField("attribution")
    val supported =
      text(row, "evidence_version").contains(ProxyEvidence.Version) &&
        c.get[String]("version").contains("proxy-attribution-v1")
    val peer = c
      .get[String]("peer_reference")
      .toOption
      .filter(_.matches("[0-9a-f]{64}"))
      .filter(_ =>
        supported && c
          .get[String]("reference_scope")
          .contains("process_wireguard_peer") &&
          c.get[String]("reference_rotation")
            .contains("process_restart_or_key_change") &&
          c.get[String]("reference_expiry").contains("process_lifetime")
      )
    val method = c
      .get[String]("method")
      .toOption
      .filter(Set("device_token", "peer_claim", "unknown"))
      .filter(_ => supported)
      .getOrElse("legacy_unknown")
    val from = c.get[String]("valid_from").toOption.flatMap(instant)
    val until = c.get[String]("valid_until").toOption.flatMap(instant)
    val mapping = c.get[String]("mapping_revision").toOption.flatMap(instant)
    val registration = text(row, "device_id").filter(
      _.matches(
        "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
      )
    )
    val valid = supported && Set("device_token", "peer_claim")(method) &&
      text(row, "identity_source").contains(
        "registered"
      ) && registration.isDefined && peer.isDefined &&
      c.get[String]("ownership").contains("registration_claim_only") &&
      from
        .zip(until)
        .exists((a, b) => a.isBefore(b) && !at.isBefore(a) && at.isBefore(b)) &&
      mapping.zip(from).exists((revision, start) => revision == start)
    val key = if valid then
      Some(
        opaque(
          deployment,
          "registration",
          registration.get,
          peer.get,
          from.get.toString,
          until.get.toString,
          mapping.get.toString
        )
      )
    else peer.map(p => peerKey(deployment, p))
    Attribution(
      if valid then "registration"
      else if peer.isDefined then "peer"
      else "unknown",
      key,
      method,
      if valid then "valid"
      else if !supported then "unsupported"
      else "invalid_or_unavailable",
      peer,
      mapping.map(_.toString),
      registration,
      from,
      until
    )

  def invalidate(deployment: String, a: Attribution): Attribution =
    a.copy(
      kind = if a.peer.isDefined then "peer" else "unknown",
      key = a.peer.map(p => peerKey(deployment, p)),
      validity = "mapping_conflict"
    )

  def coverage(rows: List[Attribution]): Json =
    val total = rows.size
    val registered = rows.count(_.kind == "registration")
    val peers = rows.count(_.kind == "peer")
    val unknown = total - registered - peers
    Json.obj(
      "denominator" -> total.asJson,
      "unit" -> "unique_observations".asJson,
      "supported_registration" -> registered.asJson,
      "scoped_unattributed_peer" -> peers.asJson,
      "unknown" -> unknown.asJson,
      "registration_rate" -> Option
        .when(total > 0)(registered.toDouble / total)
        .asJson,
      "unavailable_rate" -> Option
        .when(total > 0)((peers + unknown).toDouble / total)
        .asJson,
      "method_counts" -> Json.obj(
        rows
          .groupBy(_.method)
          .toList
          .sortBy(_._1)
          .map((k, v) => k -> v.size.asJson)*
      ),
      "validity_counts" -> Json.obj(
        rows
          .groupBy(_.validity)
          .toList
          .sortBy(_._1)
          .map((k, v) => k -> v.size.asJson)*
      ),
      "mapping_epoch_count" -> rows
        .filter(_.kind == "registration")
        .flatMap(_.key)
        .distinct
        .size
        .asJson,
      "ownership" -> "registration_claim_only".asJson,
      "peer_scope" -> "process_restart_or_key_change".asJson
    )

  def friction(rows: List[Json], start: Instant, end: Instant): Json =
    val ordered = rows.sortBy(row =>
      (
        text(row, "time").flatMap(instant).getOrElse(Instant.MIN),
        text(row, "event_id").getOrElse("")
      )
    )
    def blocked(row: Json): Boolean = row.hcursor
      .get[Boolean]("blocked")
      .getOrElse(
        text(row, "type")
          .exists(Set("block", "http_blocked", "session.blocked"))
      )
    val blocks = ordered.count(blocked)
    val errors = ordered.count(row => text(row, "type").contains("http_error"))
    val frictionRows = ordered.filter(row =>
      blocked(row) || text(row, "type").contains("http_error")
    )
    val allowedRows = ordered.filterNot(row =>
      blocked(row) || text(row, "type").contains("http_error")
    )
    val selectedFriction =
      frictionRows.take(math.max(24, 32 - allowedRows.size))
    val timeline =
      (selectedFriction ++ allowedRows.take(32 - selectedFriction.size))
        .sortBy(row =>
          (
            text(row, "time").flatMap(instant).getOrElse(Instant.MIN),
            text(row, "event_id").getOrElse("")
          )
        )
        .map { row =>
          Json.obj(
            "event_id" -> text(row, "event_id").asJson,
            "time" -> text(row, "time").asJson,
            "event_type" -> text(row, "type").asJson,
            "outcome" -> (if blocked(row) then "blocked"
                          else if text(row, "type").contains("http_error") then
                            "error"
                          else "allowed") .asJson
          )
        }
    Json.obj(
      "disposition" -> (if blocks >= 2 || errors >= 2 then
                          "repeated_policy_friction_observations"
                        else "minimum_repetition_not_met").asJson,
      "blocked_observations" -> blocks.asJson,
      "error_observations" -> errors.asJson,
      "related_allowed_observations" -> ordered
        .count(row =>
          !blocked(row) && !text(row, "type").contains("http_error")
        )
        .asJson,
      "window_start" -> start.toString.asJson,
      "window_end" -> end.toString.asJson,
      "timeline" -> timeline.asJson,
      "timeline_limit" -> 32.asJson,
      "timeline_omitted" -> (ordered.size - timeline.size).asJson,
      "association" -> "same_scope_and_window_temporal_association".asJson,
      "causality" -> "unavailable".asJson,
      "retry_count" -> Json.Null,
      "retry_disposition" -> "observations_do_not_identify_application_attempts".asJson,
      "coalescing" -> "suppressed_and_final_unreported_windows_may_hide_observations".asJson
    )
