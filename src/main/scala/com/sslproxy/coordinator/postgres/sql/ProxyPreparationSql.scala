package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.processor.*
import doobie.*
import doobie.implicits.*
import doobie.free.connection as FC
import io.circe.{Json, parser}
import io.circe.syntax.*
import java.sql.Timestamp
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

/** One bounded proxy-only transaction. Never invokes a model or queries wireless
  * sources. The source, current pointer, job and checkpoint share the live fence.
  */
object ProxyPreparationSql:
  val Processor = "proxy-search-preparer"
  private val Scope = "proxy-event-v2"
  private val MaxWindowRows = 500
  private val PreparationGeneration = 2L
  private case class WindowObservation(
    eventId: String, normalizedHost: Option[String], deviceKey: String, normalizationVersion: Option[String],
    revision: Long, host: String, eventType: String, classification: String, statusCode: Option[Int],
    bytesUp: Long, bytesDown: Long, observedAt: Timestamp
  )

  def guard(lease: Lease): ConnectionIO[Unit] =
    sql"""SELECT 1 FROM octopus_core.work_leases
      WHERE resource_type = 'processor' AND resource_id = $Processor
        AND owner_id = ${lease.ownerId} AND lease_token = ${lease.token}
        AND fence = ${lease.fence} AND lease_expires_at > clock_timestamp()
      FOR UPDATE""".query[Int].option.flatMap {
      case Some(1) => FC.unit
      case _ => FC.raiseError(IllegalStateException("proxy preparation ownership lost"))
    }

  def tick(lease: Lease, requested: Int, model: String, inputRevision: String, highWater: Int): ConnectionIO[Int] =
    val limit = requested.max(1).min(500)
    for
      _ <- sql"SELECT set_config('statement_timeout', '5000', true)".query[String].unique
      _ <- sql"SELECT set_config('lock_timeout', '1000', true)".query[String].unique
      _ <- sql"SELECT set_config('TimeZone', 'UTC', true)".query[String].unique
      _ <- guard(lease)
      // Published drilldown has a seven-day publication lifetime. Document
      // retention keeps its existing independent policy. Never scan core
      // history from the Search runtime to refill an expired publication.
      _ <- sql"""WITH expired AS (
        SELECT event_id FROM atheros_search.proxy_event_evidence
        WHERE updated_at < clock_timestamp() - interval '7 days'
        ORDER BY updated_at, event_id LIMIT $limit
      ) DELETE FROM atheros_search.proxy_event_evidence evidence USING expired
        WHERE evidence.event_id = expired.event_id""".update.run
      _ <- sql"""INSERT INTO octopus_core.proxy_preparation_progress(scope) VALUES ($Scope)
        ON CONFLICT(scope) DO NOTHING""".update.run
      after <- sql"""SELECT after_revision FROM octopus_core.proxy_preparation_progress
        WHERE scope = $Scope FOR UPDATE""".query[Long].unique
      // Count only the bounded prefix necessary to establish scoped headroom.
      pending <- sql"""SELECT count(*) FROM (
        SELECT job.job_id FROM atheros_search.embedding_jobs job
        JOIN atheros_search.search_documents doc ON doc.document_id = job.document_id
        WHERE doc.source_kind IN ('proxy_event', 'proxy_blocked_host_window')
          AND job.status IN ('pending', 'leased')
        LIMIT ${highWater.max(1)}
      ) bounded""".query[Long].unique
      budget = (highWater.toLong - pending).max(0L).min(limit.toLong).toInt
      changed <- if budget == 0 then 0.pure[ConnectionIO] else
        for
          sourceBudget <- (budget - 2).max(1).pure[ConnectionIO]
          // A half-page is reserved for history; dirty notifications cover
          // commits below that cursor and late/corrected event-time records.
          history <- sql"""SELECT event_id, observation_revision FROM octopus_core.proxy_events
            WHERE observation_revision > $after ORDER BY observation_revision LIMIT ${(sourceBudget + 1) / 2}"""
            .query[(String, Long)].to[List]
          dirty <- sql"""SELECT event_id, source_revision FROM octopus_core.proxy_preparation_dirty
            ORDER BY source_revision, event_id LIMIT ${sourceBudget - history.size} FOR UPDATE"""
            .query[(String, Long)].to[List]
          _ <- (history ++ dirty).groupMapReduce(_._1)(_._2)(_.max(_)).toList.sortBy(_._2).traverse_ { case (id, _) =>
            sql"""SELECT observation_revision, event_time, payload::text, registered_device_id::text FROM octopus_core.proxy_events
              WHERE event_id = $id FOR UPDATE""".query[(Long, Timestamp, String, Option[String])].option.flatMap {
              case None => sql"DELETE FROM octopus_core.proxy_preparation_dirty WHERE event_id = $id".update.run.void
              case Some((revision, observedAt, raw, deviceId)) =>
                for
                  json <- parser.parse(raw).fold(_ => FC.raiseError[Json](IllegalArgumentException("invalid retained proxy JSON")), _.pure[ConnectionIO])
                  evidence = ProxyEvidence.normalized(json)
                  _ <- sql"UPDATE octopus_core.proxy_events SET evidence = ${evidence.noSpaces}::jsonb WHERE event_id = $id".update.run
                  _ <- publish(id, revision, observedAt, evidence)
                  _ <- ProxyEvidence.source(json, id, revision, observedAt) match
                    case None => FC.unit
                    case Some(source) => persist(lease, source.copy(inputRevision = inputRevision, proxyDeviceId = deviceId), model)
                  host = evidence.hcursor.get[String]("host_normalized").toOption
                  _ <- if host.isDefined && ProxyEvidence.AllowedEvents(json.hcursor.get[String]("type").getOrElse("")) && json.hcursor.get[Boolean]("blocked").getOrElse(false) then
                    sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start, host, device_key)
                      SELECT date_trunc('hour', event_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC', host, ${deviceId.getOrElse("")}
                      FROM octopus_core.proxy_events WHERE event_id = $id
                      ON CONFLICT(window_start, host, device_key) DO UPDATE
                      SET revision = nextval('octopus_core.proxy_window_revision_seq'), disposition = 'pending', observed_rows_lower_bound = NULL""".update.run.void
                  else FC.unit
                  _ <- sql"DELETE FROM octopus_core.proxy_preparation_dirty WHERE event_id = $id AND source_revision <= $revision".update.run
                yield ()
            }
          }
          _ <- history.lastOption.traverse_ { case (_, cursor) =>
            if cursor <= after then FC.raiseError[Unit](IllegalStateException("nonadvancing proxy cursor"))
            else sql"UPDATE octopus_core.proxy_preparation_progress SET after_revision = $cursor, updated_at = clock_timestamp() WHERE scope = $Scope".update.run.void
          }
          // Remaining budget is shared across events/windows/jobs, never LIMIT
          // multiplied independently by every source kind.
          windows <- prepareWindows(lease, (budget - (history ++ dirty).map(_._1).distinct.size).max(0).min(1), model, inputRevision)
          repaired <- reconcile(lease, (budget - (history ++ dirty).map(_._1).distinct.size - windows).max(0), model, inputRevision)
          _ <- guard(lease)
        yield (history ++ dirty).map(_._1).distinct.size + windows + repaired
      _ <- guard(lease)
    yield changed

  private def publish(id: String, revision: Long, observedAt: Timestamp, evidence: Json): ConnectionIO[Unit] =
    val c = evidence.hcursor
    sql"""INSERT INTO atheros_search.proxy_event_evidence(event_id, source_revision, observed_at,
      host_normalized, port, authority_normalized, registrable_domain, normalization_version,
      parse_status, authority_source, sanitization_status, evidence_version, evidence)
      VALUES ($id, $revision, $observedAt, ${c.get[String]("host_normalized").toOption},
        ${c.get[Int]("port").toOption}, ${c.get[String]("authority_normalized").toOption},
        ${c.get[String]("registrable_domain").toOption}, ${ProxyAuthority.Version},
        ${c.get[String]("parse_status").getOrElse("invalid")}, ${c.get[String]("authority_source").getOrElse("legacy_host")},
        ${c.get[String]("sanitization_status").getOrElse("unknown")}, ${ProxyEvidence.Version},
        ${ProxyEvidence.publicEvidence(evidence).noSpaces}::jsonb)
      ON CONFLICT(event_id) DO UPDATE SET source_revision = EXCLUDED.source_revision,
        host_normalized = EXCLUDED.host_normalized, port = EXCLUDED.port,
        authority_normalized = EXCLUDED.authority_normalized, registrable_domain = EXCLUDED.registrable_domain,
        normalization_version = EXCLUDED.normalization_version, parse_status = EXCLUDED.parse_status,
        authority_source = EXCLUDED.authority_source, sanitization_status = EXCLUDED.sanitization_status,
        evidence_version = EXCLUDED.evidence_version, evidence = EXCLUDED.evidence, updated_at = clock_timestamp()
      WHERE proxy_event_evidence.source_revision <= EXCLUDED.source_revision""".update.run.void

  def persist(lease: Lease, source: SearchDocumentSource, model: String): ConnectionIO[Unit] =
    val revision = source.evidenceRevision.getOrElse(throw IllegalArgumentException("proxy source revision required"))
    val document = SearchDocumentPreparation.prepare(source).fold(e => throw IllegalArgumentException(e), identity)
    val table = source.kind.sourceTable
    for
      _ <- guard(lease)
      _ <- sql"""INSERT INTO atheros_search.proxy_document_state(source_table, source_key, source_revision,
        template_version, normalization_version, disposition, preparation_generation)
        VALUES ($table, ${source.sourceKey}, $revision, ${source.templateVersion}, ${source.normalizationVersion}, 'preparing', $PreparationGeneration)
        ON CONFLICT(source_table, source_key) DO NOTHING""".update.run
      current <- sql"""SELECT source_revision, document_id::text, preparation_generation FROM atheros_search.proxy_document_state
        WHERE source_table = $table AND source_key = ${source.sourceKey} FOR UPDATE""".query[(Long, Option[String], Long)].unique
      _ <- if current._1 > revision || current._3 > PreparationGeneration ||
          (current._1 == revision && current._3 == PreparationGeneration && current._2.exists(_ != document.documentId)) then FC.unit else
        for
          _ <- SearchPreparationSql.persistFencedProxy(document)
          _ <- sql"""UPDATE atheros_search.proxy_document_state SET source_revision = $revision,
            preparation_generation = $PreparationGeneration,
            document_id = ${document.documentId}::uuid, template_version = ${source.templateVersion},
            normalization_version = ${source.normalizationVersion}, disposition = 'prepared', updated_at = clock_timestamp()
            WHERE source_table = $table AND source_key = ${source.sourceKey}""".update.run
          id = UUID.nameUUIDFromBytes(List(document.documentId, model, source.inputRevision, document.normalizedSha256)
            .map(p => s"${p.length}:$p").mkString.getBytes(UTF_8)).toString
          _ <- sql"""UPDATE atheros_search.embedding_jobs SET status = 'cancelled', owner_id = NULL,
            lease_token = NULL, lease_expires_at = NULL, updated_at = clock_timestamp()
            WHERE document_id = ${document.documentId}::uuid AND embedding_model = $model AND status IN ('pending', 'leased')
              AND (input_revision <> ${source.inputRevision} OR content_sha256 <> ${document.normalizedSha256})""".update.run
          _ <- sql"""INSERT INTO atheros_search.embedding_jobs(job_id, document_id, embedding_kind, embedding_model,
            content_sha256, input_revision) VALUES ($id::uuid, ${document.documentId}::uuid, 'event', $model,
            ${document.normalizedSha256}, ${source.inputRevision}) ON CONFLICT DO NOTHING""".update.run
        yield ()
      _ <- guard(lease)
    yield ()

  private def reconcile(lease: Lease, budget: Int, model: String, inputRevision: String): ConnectionIO[Int] =
    if budget <= 0 then 0.pure[ConnectionIO] else
      val scope = "proxy-jobs:" + model + ":" + inputRevision
      for
        _ <- guard(lease)
        _ <- sql"INSERT INTO octopus_core.proxy_preparation_progress(scope) VALUES ($scope) ON CONFLICT DO NOTHING".update.run
        after <- sql"SELECT after_key FROM octopus_core.proxy_preparation_progress WHERE scope = $scope FOR UPDATE".query[String].unique
        documents <- (fr"""SELECT document_id::text, normalized_sha256 FROM atheros_search.search_documents
          WHERE source_kind IN ('proxy_event', 'proxy_blocked_host_window') AND status = 'active'
            AND input_revision = $inputRevision""" ++
          (if after.isEmpty then Fragment.empty else fr" AND document_id > $after::uuid") ++
          fr"ORDER BY document_id LIMIT $budget FOR UPDATE").query[(String, String)].to[List]
        _ <- documents.traverse_ { case (documentId, checksum) =>
          val id = UUID.nameUUIDFromBytes(List(documentId, model, inputRevision, checksum).map(p => s"${p.length}:$p").mkString.getBytes(UTF_8)).toString
          for
            _ <- sql"""INSERT INTO atheros_search.embedding_jobs(job_id, document_id, embedding_kind, embedding_model, content_sha256, input_revision)
              VALUES ($id::uuid, $documentId::uuid, 'event', $model, $checksum, $inputRevision) ON CONFLICT DO NOTHING""".update.run
            _ <- sql"""WITH repair AS (
              SELECT job.job_id FROM atheros_search.embedding_jobs job
              WHERE job.document_id = $documentId::uuid AND job.embedding_model = $model AND job.embedding_kind = 'event'
                AND job.content_sha256 = $checksum AND job.input_revision = $inputRevision
                AND job.status = 'completed' AND job.attempt_count < job.max_attempts
                AND NOT EXISTS (
                  SELECT 1 FROM atheros_search.embeddings unified
                  JOIN atheros_search.search_vectors_event per_kind ON per_kind.document_id = unified.document_id
                    AND per_kind.embedding_model = unified.embedding_model AND per_kind.input_revision = unified.input_revision
                    AND per_kind.content_sha256 = unified.content_sha256 AND per_kind.embedding::text = unified.embedding::text
                  WHERE unified.document_id = job.document_id AND unified.embedding_model = $model
                    AND unified.embedding_kind = 'event' AND unified.input_revision = $inputRevision AND unified.content_sha256 = $checksum
                    AND unified.embedding OPERATOR(public.<#>) unified.embedding < 0)
              FOR UPDATE
            ), receipt AS (
              INSERT INTO atheros_search.embedding_repair_receipts(receipt_id, job_id, previous_status, attempt_count, previous_error, reason)
              SELECT gen_random_uuid(), job.job_id, job.status, job.attempt_count, job.last_error, 'completed_missing_current_vector'
              FROM atheros_search.embedding_jobs job JOIN repair USING(job_id) RETURNING job_id
            ) UPDATE atheros_search.embedding_jobs job SET status = 'pending', completed_at = NULL,
              next_attempt_at = clock_timestamp(), updated_at = clock_timestamp()
              FROM receipt WHERE receipt.job_id = job.job_id""".update.run
          yield ()
        }
        cursor = documents.lastOption.map(_._1).getOrElse("")
        _ <- sql"UPDATE octopus_core.proxy_preparation_progress SET after_key = $cursor, updated_at = clock_timestamp() WHERE scope = $scope".update.run
        _ <- guard(lease)
      yield documents.size

  private def prepareWindows(lease: Lease, limit: Int, model: String, inputRevision: String): ConnectionIO[Int] =
    if limit == 0 then 0.pure[ConnectionIO] else
      sql"""SELECT window_start, host, device_key, revision FROM octopus_core.proxy_window_dirty
        WHERE window_start < date_trunc('hour', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
          AND disposition = 'pending'
        ORDER BY window_start, host, device_key LIMIT $limit FOR UPDATE""".query[(Timestamp, String, String, Long)].to[List].flatMap { windows =>
        windows.traverse_ { case (start, rawHost, deviceKey, revision) =>
          val end = Timestamp.from(start.toInstant.plusSeconds(3600))
          for
            // Preserve the existing kind's source key. Normalized host windows
            // are a separate stage-6 kind; they cannot rename this identity.
            key <- sql"""SELECT CONCAT('proxy-window:', md5(jsonb_build_array(CAST($rawHost AS text), CAST($deviceKey AS text), CAST($start AS timestamptz))::text))""".query[String].unique
            rows <- sql"""SELECT event_id, evidence->>'host_normalized', COALESCE(registered_device_id::text, ''),
                evidence->>'normalization_version', observation_revision, host, event_type, classification,
                status_code, bytes_up, bytes_down, event_time FROM octopus_core.proxy_events
              WHERE blocked AND event_time >= $start AND event_time < $end
                AND event_type IN ('block', 'http_blocked', 'session.blocked', 'http_proxied', 'http_error', 'tunnel_open', 'tunnel_close')
              ORDER BY event_time, event_id LIMIT ${MaxWindowRows + 1}""".query[WindowObservation].to[List]
            incomplete = rows.exists(_.normalizationVersion != Some(ProxyAuthority.Version))
            members = rows.filter(row => row.host == rawHost && row.deviceKey == deviceKey)
            hosts = members.flatMap(_.normalizedHost).distinct
            ambiguous = hosts.size != 1 || members.exists(_.normalizedHost.isEmpty)
            _ <- if rows.size > MaxWindowRows || incomplete || ambiguous then
              // Preserve the dirty window for an explicitly reviewed larger
              // budget. It cannot acquire a misleading partial denominator.
              sql"""UPDATE octopus_core.proxy_window_dirty SET disposition = ${if incomplete then "source_coverage_pending" else if rows.size > MaxWindowRows then "input_limit" else "ambiguous_normalization"}, observed_rows_lower_bound = ${rows.size}
                WHERE window_start = $start AND host = $rawHost AND device_key = $deviceKey AND revision = $revision""".update.run.void
            else
              val host = hosts.head
              val detail = Json.obj("host" -> rawHost.asJson, "host_normalized" -> host.asJson, "window_start" -> start.toInstant.toString.asJson,
                "window_end" -> end.toInstant.toString.asJson, "observed_blocked_events" -> members.size.asJson,
                "proxy_device_id" -> Option.when(deviceKey.nonEmpty)(deviceKey).asJson,
                "blocked_attempts" -> members.size.asJson, "blocked_attempts_semantics" -> "retained_observations_not_attempts".asJson,
                "total_bytes" -> members.map(row => BigInt(row.bytesUp) + BigInt(row.bytesDown)).sum.asJson,
                "total_bytes_semantics" -> "legacy_counter_sum_not_terminal_transfer".asJson,
                "event_type_counts" -> members.groupMapReduce(_.eventType)(_ => 1L)(_ + _).asJson,
                "classification_counts" -> members.groupMapReduce(_.classification)(_ => 1L)(_ + _).asJson,
                "status_code_distribution" -> members.groupMapReduce(_.statusCode.fold("none")(_.toString))(_ => 1L)(_ + _).asJson,
                "first_event_time" -> members.headOption.map(_.observedAt.toInstant.toString).asJson,
                "last_event_time" -> members.lastOption.map(_.observedAt.toInstant.toString).asJson,
                "source_watermark" -> rows.map(_.revision).maxOption.asJson, "source_evidence_revision" -> revision.asJson,
                "attempts_status" -> "unknown_coalesced".asJson, "terminal_bytes_status" -> "unavailable".asJson)
              val source = SearchDocumentSource(SearchDocumentKind.ProxyBlockedHostWindow, key, None, None, None, end,
                None, None, None, 0, false, s"blocked proxy host window $host blocked observations attempts unknown",
                detail.noSpaces, host = Some(rawHost), proxyDeviceId = Option.when(deviceKey.nonEmpty)(deviceKey), blocked = Some(true), windowStart = Some(start), windowEnd = Some(end),
                evidenceRevision = Some(revision), templateVersion = "proxy-blocked-window-v2", normalizationVersion = ProxyAuthority.Version, inputRevision = inputRevision)
              persist(lease, source, model) *>
                sql"DELETE FROM octopus_core.proxy_window_dirty WHERE window_start = $start AND host = $rawHost AND device_key = $deviceKey AND revision = $revision".update.run.void
          yield ()
        }.as(windows.size)
      }
