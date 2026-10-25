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

/** One bounded proxy-only transaction. Never invokes a model or queries
  * wireless sources. The source, current pointer, job and checkpoint share the
  * live fence.
  */
object ProxyPreparationSql:
  val Processor = "proxy-search-preparer"
  private val Scope = "proxy-event-v2"
  private val MaxWindowRows = 500
  private val PreparationGeneration = 2L
  private val ParkedWindowError = "proxy window parked; embedding job cancelled"
  private val ObsoleteInputError = "proxy input revision changed; embedding job cancelled"
  private case class WindowObservation(
      eventId: String,
      normalizedHost: Option[String],
      deviceKey: String,
      normalizationVersion: Option[String],
      revision: Long,
      host: String,
      eventType: String,
      classification: String,
      statusCode: Option[Int],
      bytesUp: Long,
      bytesDown: Long,
      observedAt: Timestamp,
      sanitized: Boolean
  )

  def guard(lease: Lease): ConnectionIO[Unit] =
    sql"""SELECT 1 FROM octopus_core.work_leases
      WHERE resource_type = 'processor' AND resource_id = $Processor
        AND owner_id = ${lease.ownerId} AND lease_token = ${lease.token}
        AND fence = ${lease.fence} AND lease_expires_at > clock_timestamp()
      FOR UPDATE""".query[Int].option.flatMap {
      case Some(1) => FC.unit
      case _       =>
        FC.raiseError(IllegalStateException("proxy preparation ownership lost"))
    }

  def tick(
      lease: Lease,
      requested: Int,
      model: String,
      inputRevision: String,
      highWater: Int
  ): ConnectionIO[Int] =
    val limit = requested.max(1).min(500)
    for
      _ <- sql"SELECT set_config('statement_timeout', '5000', true)"
        .query[String]
        .unique
      _ <- sql"SELECT set_config('lock_timeout', '1000', true)"
        .query[String]
        .unique
      _ <- sql"SELECT set_config('TimeZone', 'UTC', true)".query[String].unique
      _ <- guard(lease)
      _ <-
        sql"SELECT pg_advisory_xact_lock(hashtextextended('proxy-embedding-queue', 0)) IS NULL"
          .query[Boolean]
          .unique
      _ <-
        sql"""INSERT INTO atheros_search.proxy_preparation_policy(singleton, pending_high_water, expires_at)
        VALUES (TRUE, ${highWater.max(1)}, ${Timestamp.from(lease.expiresAt)})
        ON CONFLICT(singleton) DO UPDATE SET pending_high_water = EXCLUDED.pending_high_water,
          expires_at = EXCLUDED.expires_at, updated_at = clock_timestamp()""".update.run
      // Published drilldown has a seven-day publication lifetime. Document
      // retention keeps its existing independent policy. Never scan core
      // history from the Search runtime to refill an expired publication.
      _ <- sql"""WITH expired AS (
        SELECT event_id FROM atheros_search.proxy_event_evidence
        WHERE updated_at < clock_timestamp() - interval '7 days'
        ORDER BY updated_at, event_id LIMIT $limit
      ) DELETE FROM atheros_search.proxy_event_evidence evidence USING expired
        WHERE evidence.event_id = expired.event_id""".update.run
      _ <-
        sql"""INSERT INTO octopus_core.proxy_preparation_progress(scope) VALUES ($Scope)
        ON CONFLICT(scope) DO NOTHING""".update.run
      after <-
        sql"""SELECT after_revision FROM octopus_core.proxy_preparation_progress
        WHERE scope = $Scope FOR UPDATE""".query[Long].unique
      // Old-revision jobs cannot be claimed by the replacement worker fleet.
      // Retire a bounded page before headroom checks so a full old queue drains.
      cancelled <- cancelObsoleteInputJobs(limit, inputRevision)
      // Count only the bounded prefix necessary to establish scoped headroom.
      pending <- sql"""SELECT count(*) FROM (
        SELECT job.job_id FROM atheros_search.embedding_jobs job
        JOIN atheros_search.search_documents doc ON doc.document_id = job.document_id
        WHERE doc.source_kind IN ('proxy_event', 'proxy_blocked_host_window')
          AND job.status IN ('pending', 'leased')
        LIMIT ${highWater.max(1)}
      ) bounded""".query[Long].unique
      eventValidationFirst <-
        if pending >= highWater.max(1) && limit - cancelled == 1 then
          // A one-item budget still alternates both validation lanes. Neither
          // valid windows nor valid dirty events may starve the other lane.
          sql"""INSERT INTO octopus_core.proxy_preparation_progress(scope,after_revision)
            VALUES ('proxy-full-queue-validation-turn',1)
            ON CONFLICT(scope) DO UPDATE SET after_revision=1-proxy_preparation_progress.after_revision
            RETURNING after_revision""".query[Long].unique.map(_ == 1)
        else false.pure[ConnectionIO]
      // A full queue prevents new work, but must not keep invalidated windows
      // searchable. This bounded validation pass never publishes/enqueues.
      firstWindowWork <-
        if pending >= highWater.max(1) && !eventValidationFirst then
          prepareWindows(lease, (limit - cancelled).max(0).min(1), model, inputRevision, allowPublish = false)
        else 0.pure[ConnectionIO]
      validatedEvents <-
        if pending >= highWater.max(1) then
          validateDirtyEvents(lease, (limit - cancelled - firstWindowWork).max(0).min(1), model, inputRevision)
        else 0.pure[ConnectionIO]
      fallbackWindowWork <-
        if eventValidationFirst && validatedEvents == 0 then
          prepareWindows(lease, (limit - cancelled).max(0).min(1), model, inputRevision, allowPublish = false)
        else 0.pure[ConnectionIO]
      retiredWindows = firstWindowWork + fallbackWindowWork
      budget = (highWater.toLong - pending).max(0L).min((limit - cancelled - retiredWindows - validatedEvents).toLong).toInt
      changed <-
        if budget == 0 then 0.pure[ConnectionIO]
        else
          for
            sourceBudget <- (budget - 2).max(1).pure[ConnectionIO]
            // A half-page is reserved for history; dirty notifications cover
            // commits below that cursor and late/corrected event-time records.
            history <-
              sql"""SELECT event_id, observation_revision FROM octopus_core.proxy_events
            WHERE observation_revision > $after ORDER BY observation_revision LIMIT ${(sourceBudget + 1) / 2}"""
                .query[(String, Long)]
                .to[List]
            dirty <-
              sql"""SELECT event_id, source_revision FROM octopus_core.proxy_preparation_dirty
            ORDER BY source_revision, event_id LIMIT ${sourceBudget - history.size} FOR UPDATE"""
                .query[(String, Long)]
                .to[List]
            _ <- (history ++ dirty)
              .groupMapReduce(_._1)(_._2)(_.max(_))
              .toList
              .sortBy(_._2)
              .traverse_ { case (id, revision) =>
                prepareEvent(lease, id, revision, model, inputRevision)
              }
            _ <- history.lastOption.traverse_ { case (_, cursor) =>
              if cursor <= after then
                FC.raiseError[Unit](
                  IllegalStateException("nonadvancing proxy cursor")
                )
              else
                sql"UPDATE octopus_core.proxy_preparation_progress SET after_revision = $cursor, updated_at = clock_timestamp() WHERE scope = $Scope".update.run.void
            }
            // Remaining budget is shared across events/windows/jobs, never LIMIT
            // multiplied independently by every source kind.
            windows <- prepareWindows(
              lease,
              (budget - (history ++ dirty).map(_._1).distinct.size)
                .max(0)
                .min(1),
              model,
              inputRevision
            )
            migrated <- migrateInputRevisions(
              (budget - (history ++ dirty).map(_._1).distinct.size - windows).max(0),
              model,
              inputRevision
            )
            repaired <- reconcile(
              lease,
              (budget - (history ++ dirty).map(_._1).distinct.size - windows - migrated)
                .max(0),
              model,
              inputRevision
            )
            _ <- guard(lease)
          yield (history ++ dirty).map(_._1).distinct.size + windows + migrated + repaired
      _ <- guard(lease)
    yield changed + cancelled + retiredWindows + validatedEvents

  private def validateDirtyEvents(
      lease: Lease,
      limit: Int,
      model: String,
      inputRevision: String
  ): ConnectionIO[Int] =
    if limit == 0 then 0.pure[ConnectionIO]
    else
      val scope = "proxy-event-validation-v2"
      for
        _ <- sql"INSERT INTO octopus_core.proxy_preparation_progress(scope) VALUES ($scope) ON CONFLICT DO NOTHING".update.run
        after <- sql"SELECT after_revision,after_key FROM octopus_core.proxy_preparation_progress WHERE scope=$scope FOR UPDATE"
          .query[(Long,String)].unique
        notifications <- sql"""SELECT event_id,source_revision FROM octopus_core.proxy_preparation_dirty
          WHERE (source_revision,event_id) > (${after._1},${after._2})
          ORDER BY source_revision,event_id LIMIT $limit FOR UPDATE""".query[(String,Long)].to[List]
        _ <- notifications.traverse_ { case (id,revision) =>
          prepareEvent(lease,id,revision,model,inputRevision,allowPublish=false)
        }
        cursor = notifications.lastOption.getOrElse(("",0L))
        _ <- sql"""UPDATE octopus_core.proxy_preparation_progress SET after_revision=${cursor._2},
          after_key=${cursor._1},updated_at=clock_timestamp() WHERE scope=$scope""".update.run
      yield notifications.size

  private def prepareEvent(
      lease: Lease,
      id: String,
      notificationRevision: Long,
      model: String,
      inputRevision: String,
      allowPublish: Boolean = true
  ): ConnectionIO[Unit] =
    sql"""SELECT observation_revision,event_time,payload::text,registered_device_id::text FROM octopus_core.proxy_events
      WHERE event_id=$id FOR UPDATE""".query[(Long,Timestamp,String,Option[String])].option.flatMap {
      case None =>
        retireEvent(lease,id,notificationRevision,"source_missing") *>
          sql"DELETE FROM octopus_core.proxy_preparation_dirty WHERE event_id=$id AND source_revision <= $notificationRevision".update.run.void
      case Some((revision,observedAt,raw,deviceId)) =>
        for
          json <- parser.parse(raw).fold(
            _ => FC.raiseError[Json](IllegalArgumentException("invalid retained proxy JSON")), _.pure[ConnectionIO])
          source = ProxyEvidence.source(json,id,revision,observedAt)
          _ <- if !allowPublish && source.isDefined then
            for
              _ <- guardGeneration(SearchDocumentKind.ProxyEvent.sourceTable,id)
              previous <- sql"""SELECT source_revision,document_id IS NOT NULL FROM atheros_search.proxy_document_state
                WHERE source_table=${SearchDocumentKind.ProxyEvent.sourceTable} AND source_key=$id FOR UPDATE""".query[(Long,Boolean)].option
              // Retire stale evidence immediately without spending headroom on
              // its replacement. The notification remains for later publication.
              _ <- if previous.exists { case (oldRevision,hasDocument) => oldRevision < revision && hasDocument } then
                retireEvent(lease,id,revision,"deferred")
                else FC.unit
            yield ()
            else for
              _ <- guardGeneration(SearchDocumentKind.ProxyEvent.sourceTable,id)
              current <- sql"""SELECT source_revision FROM atheros_search.proxy_document_state
                WHERE source_table=${SearchDocumentKind.ProxyEvent.sourceTable} AND source_key=$id FOR UPDATE""".query[Long].option
              _ <- if current.exists(_ > revision) then FC.unit
                else for
                  // Remember the old grouping before replacing current metadata.
                  // Host, device, hour and blocked-state corrections must invalidate it.
                  _ <- if source.isDefined then dirtyFormerWindow(id,revision) else FC.unit
                  evidence = ProxyEvidence.normalized(json)
                  _ <- sql"UPDATE octopus_core.proxy_events SET evidence=${evidence.noSpaces}::jsonb WHERE event_id=$id".update.run
                  _ <- publish(id,revision,observedAt,evidence)
                  _ <- source match
                    case None => retireEvent(lease,id,revision,"excluded")
                    case Some(value) =>
                      for
                        windowKey <- sql"""SELECT CONCAT('proxy-window:',md5(jsonb_build_array(host,
                          COALESCE(registered_device_id::text,''),date_trunc('hour',event_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')::text))
                          FROM octopus_core.proxy_events WHERE event_id=$id""".query[String].unique
                        detail <- parser.parse(value.detailJson).fold(FC.raiseError[Json],_.pure[ConnectionIO])
                        _ <- persist(lease,value.copy(inputRevision=inputRevision,proxyDeviceId=deviceId,
                          detailJson=detail.deepMerge(Json.obj("blocked_window_source_key" -> windowKey.asJson)).noSpaces),model)
                      yield ()
                  _ <- if source.exists(_.blocked.contains(true)) && evidence.hcursor.get[String]("host_normalized").isRight then
                    sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key)
                      SELECT date_trunc('hour',event_time AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',host,${deviceId.getOrElse("")}
                      FROM octopus_core.proxy_events WHERE event_id=$id
                      ON CONFLICT(window_start,host,device_key) DO UPDATE
                      SET revision=nextval('octopus_core.proxy_window_revision_seq'),disposition='pending',observed_rows_lower_bound=NULL""".update.run.void
                    else FC.unit
                yield ()
              _ <- sql"DELETE FROM octopus_core.proxy_preparation_dirty WHERE event_id=$id AND source_revision <= $revision".update.run
            yield ()
        yield ()
    }

  private def dirtyFormerWindow(id: String,revision: Long,missing: Boolean = false): ConnectionIO[Unit] =
    sql"""SELECT date_trunc('hour',observed_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',host,
      COALESCE(proxy_device_id::text,''),detail_json->>'blocked_window_source_key'
      FROM atheros_search.search_documents WHERE source_table=${SearchDocumentKind.ProxyEvent.sourceTable} AND source_key=$id
        AND source_kind='proxy_event' AND status='active' AND blocked AND observed_at IS NOT NULL
        AND ($missing OR evidence_revision IS NULL OR evidence_revision < $revision)"""
      .query[(Timestamp,Option[String],String,Option[String])].to[List].flatMap { previous =>
        previous.traverse_ { case (start,host,device,key) =>
          def queue(rawHost: String,deviceKey: String): ConnectionIO[Unit] =
            sql"""INSERT INTO octopus_core.proxy_window_dirty(window_start,host,device_key)
              VALUES ($start,$rawHost,$deviceKey) ON CONFLICT(window_start,host,device_key) DO UPDATE
              SET revision=nextval('octopus_core.proxy_window_revision_seq'),disposition='pending',observed_rows_lower_bound=NULL""".update.run.void
          for
            // The opaque source key survives host privacy filtering. Never
            // leave the former aggregate searchable while awaiting recompute.
            _ <- key.traverse_(retireWindow)
            _ <- host match
              case Some(rawHost) => queue(rawHost,device)
              case None => key.traverse_ { windowKey =>
                val end = Timestamp.from(start.toInstant.plusSeconds(3600))
                for
                  candidates <- sql"""SELECT host,COALESCE(registered_device_id::text,''),
                    CONCAT('proxy-window:',md5(jsonb_build_array(host,COALESCE(registered_device_id::text,''),$start::timestamptz)::text))
                    FROM octopus_core.proxy_events WHERE blocked AND event_time >= $start AND event_time < $end
                    ORDER BY event_time,event_id LIMIT ${MaxWindowRows+1}""".query[(String,String,String)].to[List]
                  _ <- if candidates.size <= MaxWindowRows then
                    candidates.find(_._3 == windowKey).traverse_ { case (rawHost,deviceKey,_) => queue(rawHost,deviceKey) }
                    else FC.unit
                  _ <- sql"""UPDATE atheros_search.proxy_document_state SET disposition=${
                    if candidates.size > MaxWindowRows then "retired_reconciliation_required" else "retired_source_correction"},updated_at=clock_timestamp()
                    WHERE source_table=${SearchDocumentKind.ProxyBlockedHostWindow.sourceTable} AND source_key=$windowKey""".update.run
                yield ()
              }
          yield ()
        }
      }

  private def retireEvent(lease: Lease,id: String,revision: Long,disposition: String): ConnectionIO[Unit] =
    val table = SearchDocumentKind.ProxyEvent.sourceTable
    for
      _ <- guard(lease)
      _ <- sql"""INSERT INTO atheros_search.proxy_document_state(source_table,source_key,source_revision,
        template_version,normalization_version,disposition,preparation_generation)
        VALUES ($table,$id,$revision,${ProxyEvidence.TemplateVersion},${ProxyAuthority.Version},$disposition,$PreparationGeneration)
        ON CONFLICT(source_table,source_key) DO NOTHING""".update.run
      _ <- guardGeneration(table,id)
      current <- sql"SELECT source_revision FROM atheros_search.proxy_document_state WHERE source_table=$table AND source_key=$id FOR UPDATE"
        .query[Long].unique
      _ <- if current > revision then FC.unit
        else for
          _ <- sql"""SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array(CAST($table AS text),CAST($id AS text))::text,0)) IS NULL"""
            .query[Boolean].unique
          _ <- dirtyFormerWindow(id,revision,missing=disposition == "source_missing")
          ids <- sql"""UPDATE atheros_search.search_documents SET status='superseded',updated_at=clock_timestamp()
            WHERE source_table=$table AND source_key=$id AND source_kind='proxy_event' AND status='active'
            RETURNING document_id::text""".query[String].to[List]
          _ <- ids.traverse_(documentId => sql"""UPDATE atheros_search.embedding_jobs SET status='cancelled',owner_id=NULL,
            lease_token=NULL,lease_expires_at=NULL,last_error=${SearchPreparationSql.SupersededDocumentError},updated_at=clock_timestamp()
            WHERE document_id=$documentId::uuid AND status IN ('pending','leased')""".update.run)
          _ <- sql"""UPDATE atheros_search.proxy_document_state SET source_revision=$revision,document_id=NULL,
            disposition=$disposition,preparation_generation=$PreparationGeneration,updated_at=clock_timestamp()
            WHERE source_table=$table AND source_key=$id""".update.run
          _ <- if disposition == "source_missing" then
            sql"DELETE FROM atheros_search.proxy_event_evidence WHERE event_id=$id AND source_revision <= $revision".update.run.void
            else FC.unit
        yield ()
      _ <- guard(lease)
    yield ()

  private def cancelObsoleteInputJobs(limit: Int, inputRevision: String): ConnectionIO[Int] =
    sql"""SELECT job.job_id::text, doc.source_table, doc.source_key
      FROM atheros_search.embedding_jobs job
      JOIN atheros_search.search_documents doc ON doc.document_id = job.document_id
      WHERE doc.source_kind IN ('proxy_event', 'proxy_blocked_host_window')
        AND job.input_revision <> $inputRevision AND job.status IN ('pending', 'leased')
      ORDER BY job.job_id LIMIT $limit""".query[(String, String, String)].to[List].flatMap { jobs =>
      jobs.traverse { case (id, table, key) =>
        for
          _ <- guardGeneration(table, key)
          _ <- sql"""SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array(CAST($table AS text), CAST($key AS text))::text, 0)) IS NULL"""
            .query[Boolean].unique
          count <- sql"""UPDATE atheros_search.embedding_jobs SET status = 'cancelled', owner_id = NULL,
            lease_token = NULL, lease_expires_at = NULL, last_error = $ObsoleteInputError, updated_at = clock_timestamp()
            WHERE job_id = $id::uuid AND input_revision <> $inputRevision AND status IN ('pending', 'leased')""".update.run
        yield count
      }.map(_.sum)
    }

  private def guardGeneration(table: String, key: String): ConnectionIO[Unit] =
    sql"""SELECT preparation_generation FROM atheros_search.proxy_document_state
      WHERE source_table = $table AND source_key = $key FOR UPDATE""".query[Long].option.flatMap {
      case Some(generation) if generation > PreparationGeneration =>
        FC.raiseError(IllegalStateException("proxy preparation generation is stale"))
      case _ => FC.unit
    }

  private def publish(
      id: String,
      revision: Long,
      observedAt: Timestamp,
      evidence: Json
  ): ConnectionIO[Unit] =
    val c = evidence.hcursor
    sql"""INSERT INTO atheros_search.proxy_event_evidence(event_id, source_revision, observed_at,
      host_normalized, port, authority_normalized, registrable_domain, normalization_version,
      parse_status, authority_source, sanitization_status, evidence_version, evidence)
      VALUES ($id, $revision, $observedAt, ${c
        .get[String]("host_normalized")
        .toOption},
        ${c
        .get[Int]("port")
        .toOption}, ${c.get[String]("authority_normalized").toOption},
        ${c
        .get[String]("registrable_domain")
        .toOption}, ${ProxyAuthority.Version},
        ${c.get[String]("parse_status").getOrElse("invalid")}, ${c
        .get[String]("authority_source")
        .getOrElse("legacy_host")},
        ${c
        .get[String]("sanitization_status")
        .getOrElse("unknown")}, ${ProxyEvidence.Version},
        ${ProxyEvidence.publicEvidence(evidence).noSpaces}::jsonb)
      ON CONFLICT(event_id) DO UPDATE SET source_revision = EXCLUDED.source_revision,
        observed_at = EXCLUDED.observed_at,
        host_normalized = EXCLUDED.host_normalized, port = EXCLUDED.port,
        authority_normalized = EXCLUDED.authority_normalized, registrable_domain = EXCLUDED.registrable_domain,
        normalization_version = EXCLUDED.normalization_version, parse_status = EXCLUDED.parse_status,
        authority_source = EXCLUDED.authority_source, sanitization_status = EXCLUDED.sanitization_status,
        evidence_version = EXCLUDED.evidence_version, evidence = EXCLUDED.evidence, updated_at = clock_timestamp()
      WHERE proxy_event_evidence.source_revision <= EXCLUDED.source_revision""".update.run.void

  def persist(
      lease: Lease,
      source: SearchDocumentSource,
      model: String
  ): ConnectionIO[Unit] =
    val revision = source.evidenceRevision.getOrElse(
      throw IllegalArgumentException("proxy source revision required")
    )
    val document = SearchDocumentPreparation
      .prepare(source)
      .fold(e => throw IllegalArgumentException(e), identity)
    val table = source.kind.sourceTable
    for
      _ <- guard(lease)
      _ <-
        sql"""INSERT INTO atheros_search.proxy_document_state(source_table, source_key, source_revision,
        template_version, normalization_version, disposition, preparation_generation)
        VALUES ($table, ${source.sourceKey}, $revision, ${source.templateVersion}, ${source.normalizationVersion}, 'preparing', $PreparationGeneration)
        ON CONFLICT(source_table, source_key) DO NOTHING""".update.run
      current <-
        sql"""SELECT source_revision, document_id::text, preparation_generation, disposition FROM atheros_search.proxy_document_state
        WHERE source_table = $table AND source_key = ${source.sourceKey} FOR UPDATE"""
          .query[(Long, Option[String], Long, String)]
          .unique
      _ <-
        if current._3 > PreparationGeneration then
          FC.raiseError(
            IllegalStateException("proxy preparation generation is stale")
          )
        else if current._1 > revision ||
          (current._1 == revision && current._3 == PreparationGeneration &&
            (Set("excluded","source_missing")(current._4) || current._2.exists(_ != document.documentId)))
        then FC.unit
        else
          for
            _ <- SearchPreparationSql.persistFencedProxy(document)
            _ <-
              sql"""UPDATE atheros_search.proxy_document_state SET source_revision = $revision,
            preparation_generation = $PreparationGeneration,
            document_id = ${document.documentId}::uuid, template_version = ${source.templateVersion},
            normalization_version = ${source.normalizationVersion}, disposition = 'prepared', updated_at = clock_timestamp()
            WHERE source_table = $table AND source_key = ${source.sourceKey}""".update.run
            _ <-
              sql"""UPDATE atheros_search.embedding_jobs SET status = 'cancelled', owner_id = NULL,
            lease_token = NULL, lease_expires_at = NULL,
            last_error = CASE WHEN input_revision <> ${source.inputRevision} THEN $ObsoleteInputError ELSE last_error END,
            updated_at = clock_timestamp()
            WHERE document_id = ${document.documentId}::uuid AND embedding_model = $model AND status IN ('pending', 'leased')
              AND (input_revision <> ${source.inputRevision} OR content_sha256 <> ${document.normalizedSha256})""".update.run
            _ <- enqueueJob(document.documentId, document.normalizedSha256, model, source.inputRevision,
              source.kind == SearchDocumentKind.ProxyBlockedHostWindow)
          yield ()
      _ <- guard(lease)
    yield ()

  private def enqueueJob(
      documentId: String,
      checksum: String,
      model: String,
      inputRevision: String,
      resumeParked: Boolean = false
  ): ConnectionIO[Unit] =
    val id = UUID.nameUUIDFromBytes(List(documentId, model, inputRevision, checksum)
      .map(p => s"${p.length}:$p").mkString.getBytes(UTF_8)).toString
    for
      _ <- sql"""INSERT INTO atheros_search.embedding_jobs(job_id, document_id, embedding_kind, embedding_model,
        content_sha256, input_revision) VALUES ($id::uuid, $documentId::uuid, 'event', $model,
        $checksum, $inputRevision) ON CONFLICT DO NOTHING""".update.run
      // Revision rollbacks and metadata corrections retain job identity.
      // Resume only preparer cancellations, preserving terminal outcomes.
      _ <- sql"""UPDATE atheros_search.embedding_jobs SET status = 'pending', last_error = NULL,
        next_attempt_at = clock_timestamp(), updated_at = clock_timestamp()
        WHERE document_id = $documentId::uuid AND embedding_kind = 'event' AND embedding_model = $model
          AND content_sha256 = $checksum AND input_revision = $inputRevision AND status = 'cancelled'
          AND (last_error = $ObsoleteInputError OR last_error = ${SearchPreparationSql.SupersededDocumentError}
            OR ($resumeParked AND last_error = $ParkedWindowError))
          AND attempt_count < max_attempts""".update.run
    yield ()

  private def migrateInputRevisions(budget: Int, model: String, inputRevision: String): ConnectionIO[Int] =
    if budget <= 0 then 0.pure[ConnectionIO]
    else
      sql"""SELECT document_id::text, source_table, source_key FROM atheros_search.search_documents
        WHERE source_kind IN ('proxy_event', 'proxy_blocked_host_window') AND status = 'active'
          AND input_revision <> $inputRevision
        ORDER BY document_id LIMIT $budget""".query[(String, String, String)].to[List].flatMap { documents =>
        documents.traverse { case (id, table, key) =>
          for
            _ <- guardGeneration(table, key)
            _ <- sql"""SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array(CAST($table AS text), CAST($key AS text))::text, 0)) IS NULL"""
              .query[Boolean].unique
            // Recheck after taking the same source lock as worker completion.
            current <- sql"""SELECT normalized_sha256, host, detail_json::text, proxy_event_type, tags::text
              FROM atheros_search.search_documents
              WHERE document_id = $id::uuid AND status = 'active' AND input_revision <> $inputRevision
              FOR UPDATE""".query[(String, Option[String], String, Option[String], String)].option
            _ <- current.traverse_ { case (checksum, rawHost, rawDetail, eventType, rawTags) =>
              for
                detail <- parser.parse(rawDetail).fold(FC.raiseError[Json], _.pure[ConnectionIO])
                tags <- parser.parse(rawTags).flatMap(json => json.asArray.toRight(
                  io.circe.DecodingFailure("proxy tags must be an array", Nil))).fold(FC.raiseError[Vector[Json]], _.pure[ConnectionIO])
                host = ProxyEvidence.safeHost(rawHost, detail.hcursor.get[String]("sanitization_status").contains("sanitized"))
                safeDetail = ProxyEvidence.publicEvidence(detail).deepMerge(Json.obj("host" -> host.asJson)).noSpaces
                titleParts = List(eventType, host).flatten.map(_.trim).filter(_.nonEmpty).distinct
                title = Option.when(titleParts.nonEmpty)(titleParts.mkString(" ").take(512))
                tagHost = host.filter(_.length <= 255)
                safeTags = (tags.filterNot(_.hcursor.get[String]("type").contains("host")) ++ tagHost.map(value =>
                  Json.obj("type" -> "host".asJson, "value" -> value.asJson))).asJson.noSpaces
                _ <- sql"""UPDATE atheros_search.search_documents SET input_revision = $inputRevision,
                  host = $host, detail_json = $safeDetail::jsonb, title = $title, tags = $safeTags::jsonb,
                  updated_at = clock_timestamp() WHERE document_id = $id::uuid""".update.run
                _ <- sql"DELETE FROM atheros_search.search_document_tags WHERE document_id = $id::uuid AND tag_type = 'host'".update.run
                _ <- tagHost.traverse_(value => sql"""INSERT INTO atheros_search.search_document_tags(document_id,tag_type,tag_value)
                  VALUES ($id::uuid,'host',$value)""".update.run)
                _ <- enqueueJob(id, checksum, model, inputRevision)
              yield ()
            }
          yield current.fold(0)(_ => 1)
        }.map(_.sum)
      }

  private def reconcile(
      lease: Lease,
      budget: Int,
      model: String,
      inputRevision: String
  ): ConnectionIO[Int] =
    if budget <= 0 then 0.pure[ConnectionIO]
    else
      val scope = "proxy-jobs:" + model + ":" + inputRevision
      for
        _ <- guard(lease)
        _ <-
          sql"INSERT INTO octopus_core.proxy_preparation_progress(scope) VALUES ($scope) ON CONFLICT DO NOTHING".update.run
        after <-
          sql"SELECT after_key FROM octopus_core.proxy_preparation_progress WHERE scope = $scope FOR UPDATE"
            .query[String]
            .unique
        documents <-
          (fr"""SELECT document_id::text, normalized_sha256 FROM atheros_search.search_documents
          WHERE source_kind IN ('proxy_event', 'proxy_blocked_host_window') AND status = 'active'
            AND input_revision = $inputRevision""" ++
            (if after.isEmpty then Fragment.empty
             else fr" AND document_id > $after::uuid") ++
            fr"ORDER BY document_id LIMIT $budget FOR UPDATE")
            .query[(String, String)]
            .to[List]
        _ <- documents.traverse_ { case (documentId, checksum) =>
          for
            _ <- enqueueJob(documentId, checksum, model, inputRevision)
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
        _ <-
          sql"UPDATE octopus_core.proxy_preparation_progress SET after_key = $cursor, updated_at = clock_timestamp() WHERE scope = $scope".update.run
        _ <- guard(lease)
      yield documents.size

  private def retireWindow(key: String): ConnectionIO[Unit] =
    val table = SearchDocumentKind.ProxyBlockedHostWindow.sourceTable
    for
      _ <- guardGeneration(table, key)
      // Serialize retirement against both publication and worker completion.
      _ <- sql"""SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array(CAST($table AS text), CAST($key AS text))::text, 0)) IS NULL"""
        .query[Boolean].unique
      ids <- sql"""UPDATE atheros_search.search_documents SET status = 'superseded', updated_at = clock_timestamp()
        WHERE source_table = $table AND source_key = $key AND source_kind = 'proxy_blocked_host_window'
          AND status = 'active' RETURNING document_id::text""".query[String].to[List]
      _ <- ids.traverse_ { id =>
        sql"""UPDATE atheros_search.embedding_jobs SET status = 'cancelled', owner_id = NULL,
          lease_token = NULL, lease_expires_at = NULL, last_error = $ParkedWindowError,
          updated_at = clock_timestamp()
          WHERE document_id = $id::uuid AND status IN ('pending', 'leased')""".update.run.void
      }
    yield ()

  private def prepareWindows(
      lease: Lease,
      limit: Int,
      model: String,
      inputRevision: String,
      allowPublish: Boolean = true
  ): ConnectionIO[Int] =
    if limit == 0 then 0.pure[ConnectionIO]
    else
      (fr"""SELECT window_start, host, device_key, revision FROM octopus_core.proxy_window_dirty
        WHERE window_start < date_trunc('hour', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
          AND disposition = 'pending'""" ++
        (if allowPublish then fr"ORDER BY window_start, host, device_key"
         else fr"ORDER BY revision, window_start, host, device_key") ++
        fr"LIMIT $limit FOR UPDATE")
        .query[(Timestamp, String, String, Long)]
        .to[List]
        .flatMap { windows =>
          windows
            .traverse_ { case (start, rawHost, deviceKey, revision) =>
              val end = Timestamp.from(start.toInstant.plusSeconds(3600))
              for
                // Preserve the existing kind's source key. Normalized host windows
                // are a separate stage-6 kind; they cannot rename this identity.
                key <-
                  sql"""SELECT CONCAT('proxy-window:', md5(jsonb_build_array(CAST($rawHost AS text), CAST($deviceKey AS text), CAST($start AS timestamptz))::text))"""
                    .query[String]
                    .unique
                rows <- sql"""SELECT event_id, evidence->>'host_normalized', COALESCE(registered_device_id::text, ''),
                evidence->>'normalization_version', observation_revision, host, event_type, classification,
                status_code, bytes_up, bytes_down, event_time,
                COALESCE(evidence->>'sanitization_status', '') = 'sanitized' FROM octopus_core.proxy_events
              WHERE blocked AND event_time >= $start AND event_time < $end
                AND event_type IN ('block', 'http_blocked', 'session.blocked', 'http_proxied', 'http_error', 'tunnel_open', 'tunnel_close')
              ORDER BY event_time, event_id LIMIT ${MaxWindowRows + 1}"""
                  .query[WindowObservation]
                  .to[List]
                memberRows <-
                  sql"""SELECT event_id, evidence->>'host_normalized', COALESCE(registered_device_id::text, ''),
                evidence->>'normalization_version', observation_revision, host, event_type, classification,
                status_code, bytes_up, bytes_down, event_time,
                COALESCE(evidence->>'sanitization_status', '') = 'sanitized' FROM octopus_core.proxy_events
              WHERE blocked AND event_time >= $start AND event_time < $end
                AND event_type IN ('block', 'http_blocked', 'session.blocked', 'http_proxied', 'http_error', 'tunnel_open', 'tunnel_close')
                AND host = $rawHost AND COALESCE(registered_device_id::text, '') = $deviceKey
              ORDER BY event_time, event_id LIMIT ${MaxWindowRows + 1}"""
                    .query[WindowObservation]
                    .to[List]
                incomplete = memberRows.exists(
                  _.normalizationVersion != Some(ProxyAuthority.Version)
                )
                members = memberRows
                hosts = members.flatMap(_.normalizedHost).distinct
                ambiguous = hosts.size != 1 || members.exists(
                  _.normalizedHost.isEmpty
                )
                _ <-
                  if memberRows.size > MaxWindowRows || incomplete || ambiguous then
                    // Preserve the dirty window for an explicitly reviewed larger
                    // budget. It cannot acquire a misleading partial denominator.
                    sql"""UPDATE octopus_core.proxy_window_dirty SET disposition = ${
                        if incomplete then "source_coverage_pending"
                        else if memberRows.size > MaxWindowRows then "input_limit"
                        else "ambiguous_normalization"
                      }, observed_rows_lower_bound = ${memberRows.size}
                WHERE window_start = $start AND host = $rawHost AND device_key = $deviceKey AND revision = $revision""".update.run.void *>
                      retireWindow(key)
                  else if !allowPublish then
                    // Rotate validated work behind the remaining notifications.
                    // A full queue must not pin every pass to one valid window.
                    sql"""UPDATE octopus_core.proxy_window_dirty
                      SET revision = nextval('octopus_core.proxy_window_revision_seq')
                      WHERE window_start = $start AND host = $rawHost AND device_key = $deviceKey AND revision = $revision""".update.run.void
                  else
                    val host = hosts.head
                    val legacyHost = ProxyEvidence.safeHost(Some(rawHost), members.exists(_.sanitized))
                    val detail = Json.obj(
                      "host" -> legacyHost.asJson,
                      "host_normalized" -> host.asJson,
                      "window_start" -> start.toInstant.toString.asJson,
                      "window_end" -> end.toInstant.toString.asJson,
                      "observed_blocked_events" -> members.size.asJson,
                      "proxy_device_id" -> Option
                        .when(deviceKey.nonEmpty)(deviceKey)
                        .asJson,
                      "blocked_attempts" -> members.size.asJson,
                      "blocked_attempts_semantics" -> "retained_observations_not_attempts".asJson,
                      "total_bytes" -> members
                        .map(row => BigInt(row.bytesUp) + BigInt(row.bytesDown))
                        .sum
                        .asJson,
                      "total_bytes_semantics" -> "legacy_counter_sum_not_terminal_transfer".asJson,
                      "event_type_counts" -> members
                        .groupMapReduce(_.eventType)(_ => 1L)(_ + _)
                        .asJson,
                      "classification_counts" -> members
                        .groupMapReduce(_.classification)(_ => 1L)(_ + _)
                        .asJson,
                      "status_code_distribution" -> members
                        .groupMapReduce(_.statusCode.fold("none")(_.toString))(
                          _ => 1L
                        )(_ + _)
                        .asJson,
                      "first_event_time" -> members.headOption
                        .map(_.observedAt.toInstant.toString)
                        .asJson,
                      "last_event_time" -> members.lastOption
                        .map(_.observedAt.toInstant.toString)
                        .asJson,
                      "source_watermark" -> rows
                        .map(_.revision)
                        .maxOption
                        .asJson,
                      "source_evidence_revision" -> revision.asJson,
                      "attempts_status" -> "unknown_coalesced".asJson,
                      "terminal_bytes_status" -> "unavailable".asJson
                    )
                    val source = SearchDocumentSource(
                      SearchDocumentKind.ProxyBlockedHostWindow,
                      key,
                      None,
                      None,
                      None,
                      end,
                      None,
                      None,
                      None,
                      0,
                      false,
                      s"blocked proxy host window $host blocked observations attempts unknown",
                      detail.noSpaces,
                      host = legacyHost,
                      proxyDeviceId =
                        Option.when(deviceKey.nonEmpty)(deviceKey),
                      blocked = Some(true),
                      windowStart = Some(start),
                      windowEnd = Some(end),
                      evidenceRevision = Some(revision),
                      templateVersion = "proxy-blocked-window-v2",
                      normalizationVersion = ProxyAuthority.Version,
                      inputRevision = inputRevision
                    )
                    persist(lease, source, model) *>
                      sql"DELETE FROM octopus_core.proxy_window_dirty WHERE window_start = $start AND host = $rawHost AND device_key = $deviceKey AND revision = $revision".update.run.void
              yield ()
            }
            .as(windows.size)
        }
