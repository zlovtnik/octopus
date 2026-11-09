package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.util.Sha256Utils
import doobie.*
import doobie.implicits.*
import io.circe.{Json, Printer}
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant
import scala.util.Try

/** Reconcile the complete bounded signal set before the rotating output page.
  * Search uses the expiry-aware view rather than this historical state table.
  */
object ProxySignalSql:
  private val Canonical = Printer.noSpaces.copy(sortKeys = true)
  private val MaximumSignals = 256
  private val SnapshotFields = Set("feed_id", "snapshot_version", "snapshot_sha256", "feed_provenance",
    "feed_license", "feed_generated_at", "feed_retrieved_at", "feed_expires_at")
  private final case class Signal(id: String, key: String, start: Instant,
      end: Instant, state: String, until: Instant, evidence: Json)

  private def decode(value: Json): Option[Signal] =
    val c = value.hcursor
    def time(key: String): Option[Instant] =
      c.get[String](key).toOption.flatMap(v => Try(Instant.parse(v)).toOption)
    for
      id <- c.get[String]("signal_id").toOption.filter(_.matches("[0-9a-f]{64}"))
      key <- c.get[String]("window_key").toOption.filter(_.getBytes(StandardCharsets.UTF_8).length <= 4096)
      start <- time("window_start")
      end <- time("window_end").filter(_.isAfter(start))
      state <- c.get[String]("state").toOption.filter(Set("active", "candidate"))
      until <- time("valid_until")
      _ <- Option.when(value.noSpaces.getBytes(StandardCharsets.UTF_8).length <= 524288)(())
    yield Signal(id, key, start, end, state, until, value)

  def reconcile(scope: String, values: List[Json], policyFingerprint: String,
      policyExpires: Instant, lookbackStart: Instant, asOf: Instant): ConnectionIO[Unit] =
    val candidates = values.traverse(decode)
      .filter(rows => rows.size <= MaximumSignals && rows.map(_.id).distinct.size == rows.size)
      .getOrElse(Nil)
      .filter(s => s.until.isAfter(asOf) && policyExpires.isAfter(asOf))
    for
      decoded <- candidates.traverseFilter { signal =>
        val feed = signal.evidence.hcursor.get[String]("feed_id").getOrElse("")
        val version = signal.evidence.hcursor.get[String]("snapshot_version").getOrElse("")
        val identity = signal.evidence.mapObject(_.filter((key, _) => SnapshotFields(key)))
        val fingerprint = Sha256Utils.sha256Hex(Canonical.print(identity).getBytes(StandardCharsets.UTF_8))
        if feed.isEmpty || feed.length > 128 || version.isEmpty || version.length > 128 then
          Option.empty[Signal].pure[ConnectionIO]
        else for
          _ <- sql"""INSERT INTO atheros_search.proxy_feed_snapshot_binding(feed_id,snapshot_version,
            snapshot_fingerprint,first_observed_at) VALUES ($feed,$version,$fingerprint,${Timestamp.from(asOf)})
            ON CONFLICT(feed_id,snapshot_version) DO NOTHING""".update.run
          recorded <- sql"""SELECT snapshot_fingerprint FROM atheros_search.proxy_feed_snapshot_binding
            WHERE feed_id=$feed AND snapshot_version=$version""".query[String].unique
        yield Option.when(recorded == fingerprint)(signal)
      }
      retained = decoded.map(_.id).toNel.fold(Fragment.const("false"))(Fragments.in(fr"signal_id", _))
      // At most 256 live signals can have been admitted per complete snapshot.
      // The expiry-aware view protects readers even if old tombstones need more pages.
      _ <- (fr"""WITH withdrawn AS MATERIALIZED (
        SELECT deployment_scope,signal_id FROM atheros_search.proxy_signal_state
        WHERE state <> 'retracted' AND (deployment_scope <> $scope
          OR valid_until <= ${Timestamp.from(asOf)} OR NOT (""" ++ retained ++ fr"""))
        ORDER BY deployment_scope,valid_until,signal_id LIMIT 500 FOR UPDATE)
        UPDATE atheros_search.proxy_signal_state target SET state='retracted',
          retraction_reason=CASE WHEN target.valid_until <= ${Timestamp.from(asOf)}
            THEN 'expired' ELSE 'source_or_snapshot_withdrawn' END,
          projection_revision=nextval('atheros_search.proxy_behavior_revision_seq'),
          calculated_at=${Timestamp.from(asOf)}
        FROM withdrawn WHERE target.deployment_scope=withdrawn.deployment_scope
          AND target.signal_id=withdrawn.signal_id""").update.run
      _ <- decoded.traverse_ { signal =>
        val until = if signal.until.isBefore(policyExpires) then signal.until else policyExpires
        val fingerprint = Sha256Utils.sha256Hex(Canonical.print(signal.evidence).getBytes(StandardCharsets.UTF_8))
        sql"""INSERT INTO atheros_search.proxy_signal_state(deployment_scope,signal_id,window_key,
          window_start,window_end,evidence,state,valid_until,policy_fingerprint,source_fingerprint,
          projection_revision,calculated_at)
          VALUES ($scope,${signal.id},${signal.key},${Timestamp.from(signal.start)},${Timestamp.from(signal.end)},
            ${signal.evidence.noSpaces}::jsonb,${signal.state},${Timestamp.from(until)},$policyFingerprint,
            $fingerprint,nextval('atheros_search.proxy_behavior_revision_seq'),${Timestamp.from(asOf)})
          ON CONFLICT(deployment_scope,signal_id) DO UPDATE SET evidence=EXCLUDED.evidence,
            state=EXCLUDED.state,valid_until=EXCLUDED.valid_until,policy_fingerprint=EXCLUDED.policy_fingerprint,
            source_fingerprint=EXCLUDED.source_fingerprint,projection_revision=EXCLUDED.projection_revision,
            retraction_reason=NULL,calculated_at=EXCLUDED.calculated_at
          WHERE proxy_signal_state.source_fingerprint IS DISTINCT FROM EXCLUDED.source_fingerprint
            OR proxy_signal_state.policy_fingerprint IS DISTINCT FROM EXCLUDED.policy_fingerprint
            OR proxy_signal_state.state IS DISTINCT FROM EXCLUDED.state
            OR proxy_signal_state.valid_until IS DISTINCT FROM EXCLUDED.valid_until""".update.run.void
      }
      _ <- sql"""WITH old AS MATERIALIZED (
        SELECT deployment_scope,signal_id FROM atheros_search.proxy_signal_state
        WHERE state='retracted' AND window_end < ${Timestamp.from(lookbackStart)}
        ORDER BY deployment_scope,window_start,signal_id LIMIT 500 FOR UPDATE)
        DELETE FROM atheros_search.proxy_signal_state target USING old
        WHERE target.deployment_scope=old.deployment_scope AND target.signal_id=old.signal_id""".update.run
    yield ()
