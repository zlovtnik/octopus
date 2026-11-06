package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.processor.*
import doobie.*
import doobie.implicits.*
import doobie.free.connection as FC
import io.circe.{Json, parser}
import io.circe.syntax.*
import java.sql.Timestamp
import java.time.{Duration, Instant}

object ProxyBehaviorSql:
  private val Empty = ProxyBehavior.Result(Nil,Nil,0)
  private case class Settings(
      scope: String, version: String, coverageStart: Timestamp, coverageEnd: Timestamp,
      coverageAttested: Boolean, conflictsAttested: Boolean, expiresAt: Timestamp,
      horizon: Int, window: Int, lookback: Int, maxRows: Int, maxWindows: Int
  )

  def tick(lease: Lease, requested: Int, model: String, inputRevision: String,
      headroom: Int, asOf: Instant): ConnectionIO[Int] =
    for
      _ <- ProxyPreparationSql.guard(lease)
      _ <- sql"SELECT set_config('statement_timeout','5000',true)".query[String].unique
      _ <- sql"SELECT set_config('TimeZone','UTC',true)".query[String].unique
      _ <- sql"SELECT pg_advisory_xact_lock(hashtextextended('proxy-embedding-queue',0)) IS NULL".query[Boolean].unique
      settings <- sql"""SELECT deployment_scope,policy_version,coverage_start,coverage_end,
        coverage_attested,conflicts_attested,expires_at,pairing_horizon_seconds,window_seconds,
        lookback_seconds,max_source_rows,max_windows
        FROM atheros_search.proxy_behavior_policy WHERE singleton""".query[Settings].option
      work <- settings match
        case None =>
          sql"""UPDATE atheros_search.proxy_behavior_status SET status='disabled',
            calculated_at=${Timestamp.from(asOf)} WHERE singleton""".update.run *>
            retireMissing(lease,"",Empty,requested.max(0).min(500))
        case Some(s) if !s.expiresAt.toInstant.isAfter(asOf) =>
          status(s,"policy_expired",Json.obj(),asOf) *>
            retireMissing(lease,s.scope,Empty,requested.max(0).min(500))
        case Some(s) => prepare(lease,s,requested.max(0).min(500),model,inputRevision,headroom.max(0),asOf)
      _ <- ProxyPreparationSql.guard(lease)
    yield work

  private def status(s: Settings,state: String,detail: Json,asOf: Instant): ConnectionIO[Unit] =
    sql"""INSERT INTO atheros_search.proxy_behavior_status(singleton,status,detail,deployment_scope,policy_version,calculated_at)
      VALUES (true,$state,${detail.noSpaces}::jsonb,${s.scope},${s.version},${Timestamp.from(asOf)})
      ON CONFLICT(singleton) DO UPDATE SET status=EXCLUDED.status,detail=EXCLUDED.detail,
        deployment_scope=EXCLUDED.deployment_scope,policy_version=EXCLUDED.policy_version,calculated_at=EXCLUDED.calculated_at""".update.run.void

  private def prepare(lease: Lease,s: Settings,budget: Int,model: String,
      inputRevision: String,headroom: Int,asOf: Instant): ConnectionIO[Int] =
    val start = asOf.minusSeconds(s.lookback.toLong)
    val lookupStart = Timestamp.from(start.minusSeconds(s.horizon.toLong))
    val lookupEnd = Timestamp.from(asOf)
    val maxRows = s.maxRows + 1
    for
      // Both bounded lanes use one statement snapshot. A conflict can name a
      // flow outside its canonical row's time/correlation.
      snapshot <- sql"""WITH sources AS MATERIALIZED (
        SELECT payload::text,observation_revision,source_conflicts::text
        FROM octopus_core.proxy_events WHERE event_time >= $lookupStart AND event_time <= $lookupEnd
        ORDER BY event_time,event_id LIMIT $maxRows
      ), conflicts AS MATERIALIZED (
        SELECT payload::text,observation_revision,source_conflicts::text
        FROM octopus_core.proxy_events WHERE source_conflicts <> '{}'::jsonb
        ORDER BY observation_revision LIMIT $maxRows
      ) SELECT 'source',* FROM sources UNION ALL SELECT 'conflict',* FROM conflicts"""
        .query[(String,String,Long,String)].to[List]
      rows = snapshot.collect { case ("source",raw,revision,markers) => (raw,revision,markers) }
      conflicts = snapshot.collect { case ("conflict",raw,revision,markers) => (raw,revision,markers) }
      result <-
        if rows.size > s.maxRows || conflicts.size > s.maxRows then
          status(s,"input_overflow",Json.obj("source_rows_lower_bound" -> rows.size.asJson,
            "conflict_rows_lower_bound" -> conflicts.size.asJson),asOf) *>
            retireMissing(lease,s.scope,Empty,budget)
        else
          val sources = (rows ++ conflicts).distinct.traverse { case (raw,revision,markers) =>
            for
              row <- parser.parse(raw).leftMap(_ => "invalid retained proxy payload")
              trusted <- parser.parse(markers).leftMap(_ => "invalid trusted proxy conflicts")
            yield ProxyBehavior.Source(row,revision,trusted)
          }
          // Future counterparts have not been collected, even if a policy
          // attests a wider interval. The actual read ends at asOf.
          val coveredEnd = if s.coverageEnd.toInstant.isBefore(asOf) then s.coverageEnd.toInstant else asOf
          val policy = ProxyBehavior.Policy(s.scope,s.version,Duration.ofSeconds(s.horizon.toLong),
            s.window,s.coverageStart.toInstant,coveredEnd,s.coverageAttested,
            s.conflictsAttested,asOf,start,s.maxRows,s.maxWindows)
          sources.flatMap(ProxyBehavior.build(policy,_)) match
            case Left(error) => status(s,"unavailable",Json.obj("reason" -> error.asJson),asOf) *>
              retireMissing(lease,s.scope,Empty,budget)
            case Right(built) =>
              for
                retired <- retireMissing(lease,s.scope,built,budget)
                _ <- sql"""INSERT INTO octopus_core.proxy_preparation_progress(scope)
                  VALUES ('proxy-behavior-output') ON CONFLICT DO NOTHING""".update.run
                cursor <- sql"""SELECT after_key FROM octopus_core.proxy_preparation_progress
                  WHERE scope='proxy-behavior-output' FOR UPDATE""".query[String].unique
                outputs = (built.flows.map(p => ("flow:" + p.key,true,p)) ++
                  built.windows.map(p => ("window:" + p.key,false,p))).sortBy(_._1)
                ordered = outputs.filter(_._1 > cursor) ++ outputs.filter(_._1 <= cursor)
                counts <- ordered.foldM((retired,0,cursor)) { case ((work,jobs,last),(key,isFlow,p)) =>
                  if work >= budget then (work,jobs,last).pure[ConnectionIO]
                  else
                    val change = if isFlow then persistFlow(lease,s,p,model,inputRevision,headroom > jobs,asOf)
                      else persistWindow(s,p,asOf).map(changed => (changed,0))
                    change.map { case (changed,enqueued) => (work + changed,jobs + enqueued,key) }
                }
                _ <- sql"""UPDATE octopus_core.proxy_preparation_progress SET after_key=${counts._3},
                  updated_at=clock_timestamp() WHERE scope='proxy-behavior-output'""".update.run
                _ <- status(s,if counts._1 >= budget then "recomputing" else "ready",
                  Json.obj("excluded_observations" -> built.excludedObservations.asJson),asOf)
              yield counts._1
    yield result

  private def persistFlow(lease: Lease,s: Settings,p: ProxyBehavior.Projection,model: String,
      inputRevision: String,allowJob: Boolean,asOf: Instant): ConnectionIO[(Int,Int)] =
    val correlation = p.evidence.hcursor.get[String]("correlation_id").toOption.get
    for
      changed <- sql"""INSERT INTO atheros_search.proxy_flow(deployment_scope,correlation_id,
        evidence,disposition,source_watermark,projection_revision,policy_version,calculation_version,calculated_at,source_fingerprint)
        VALUES (${s.scope},$correlation,${p.evidence.noSpaces}::jsonb,${p.disposition},${p.sourceWatermark},
          nextval('atheros_search.proxy_behavior_revision_seq'),${s.version},${ProxyBehavior.CalculationVersion},
          ${Timestamp.from(asOf)},${p.fingerprint})
        ON CONFLICT(deployment_scope,correlation_id) DO UPDATE SET evidence=EXCLUDED.evidence,
          disposition=EXCLUDED.disposition,source_watermark=EXCLUDED.source_watermark,
          projection_revision=EXCLUDED.projection_revision,policy_version=EXCLUDED.policy_version,
          calculation_version=EXCLUDED.calculation_version,calculated_at=EXCLUDED.calculated_at,
          source_fingerprint=EXCLUDED.source_fingerprint
        WHERE proxy_flow.source_fingerprint IS DISTINCT FROM EXCLUDED.source_fingerprint
        RETURNING projection_revision""".query[Long].option
      current <- sql"""SELECT projection_revision FROM atheros_search.proxy_flow
        WHERE deployment_scope=${s.scope} AND correlation_id=$correlation""".query[Long].unique
      document <- sql"""SELECT source_revision,document_id IS NOT NULL FROM atheros_search.proxy_document_state
        WHERE source_table='proxy_flows' AND source_key=${p.key}""".query[(Long,Boolean)].option
      needsDocument = document.forall { case (revision,present) => revision != current || !present }
      publish = needsDocument && allowJob
      _ <- if publish then
        ProxyPreparationSql.persist(lease,SearchDocumentSource(
          SearchDocumentKind.ProxyFlow,p.key,None,None,None,Timestamp.from(p.observedAt),
          None,None,None,0,false,p.semanticText,p.evidence.noSpaces,
          host=p.evidence.hcursor.get[String]("host_normalized").toOption,
          evidenceRevision=Some(current),templateVersion="proxy-flow-v1",
          normalizationVersion=ProxyAuthority.Version,inputRevision=inputRevision),model)
        else if needsDocument && document.exists(_._2) then retireDocument(p.key) else FC.unit
    yield (if changed.isDefined || publish || (needsDocument && document.exists(_._2)) then 1 else 0,
      if publish then 1 else 0)

  private def persistWindow(s: Settings,p: ProxyBehavior.Projection,asOf: Instant): ConnectionIO[Int] =
    val c = p.evidence.hcursor
    val endpoint = c.get[String]("endpoint_key").toOption.get
    val start = Timestamp.from(Instant.parse(c.get[String]("window_start").toOption.get))
    val end = Timestamp.from(Instant.parse(c.get[String]("window_end").toOption.get))
    for
      changed <- sql"""INSERT INTO atheros_search.proxy_host_window(deployment_scope,endpoint_key,
        window_start,window_end,evidence,source_watermark,projection_revision,policy_version,calculation_version,calculated_at,source_fingerprint)
        VALUES (${s.scope},$endpoint,$start,$end,${p.evidence.noSpaces}::jsonb,${p.sourceWatermark},
          nextval('atheros_search.proxy_behavior_revision_seq'),${s.version},${ProxyBehavior.CalculationVersion},
          ${Timestamp.from(asOf)},${p.fingerprint})
        ON CONFLICT(deployment_scope,endpoint_key,window_start) DO UPDATE SET window_end=EXCLUDED.window_end,
          evidence=EXCLUDED.evidence,source_watermark=EXCLUDED.source_watermark,projection_revision=EXCLUDED.projection_revision,
          policy_version=EXCLUDED.policy_version,calculation_version=EXCLUDED.calculation_version,
          calculated_at=EXCLUDED.calculated_at,source_fingerprint=EXCLUDED.source_fingerprint
        WHERE proxy_host_window.source_fingerprint IS DISTINCT FROM EXCLUDED.source_fingerprint""".update.run
      _ <- sql"""INSERT INTO atheros_search.proxy_host_current(deployment_scope,endpoint_key,latest_window_start)
        VALUES (${s.scope},$endpoint,$start) ON CONFLICT(deployment_scope,endpoint_key) DO UPDATE
        SET latest_window_start=EXCLUDED.latest_window_start
        WHERE proxy_host_current.latest_window_start <= EXCLUDED.latest_window_start""".update.run
    yield changed

  private def retireDocument(key: String): ConnectionIO[Unit] =
    for
      _ <- sql"""SELECT pg_advisory_xact_lock(hashtextextended(
        jsonb_build_array('proxy_flows'::text,$key::text)::text,0)) IS NULL""".query[Boolean].unique
      _ <- sql"""UPDATE atheros_search.search_documents SET status='superseded',updated_at=clock_timestamp()
        WHERE source_table='proxy_flows' AND source_key=$key AND status='active'""".update.run
      _ <- sql"""UPDATE atheros_search.embedding_jobs job SET status='cancelled',
        last_error=${SearchPreparationSql.SupersededDocumentError},owner_id=NULL,
        lease_token=NULL,lease_expires_at=NULL,updated_at=clock_timestamp()
        FROM atheros_search.search_documents document
        WHERE job.document_id=document.document_id AND document.source_table='proxy_flows'
          AND document.source_key=$key AND job.status IN ('pending','leased')""".update.run
      _ <- sql"""UPDATE atheros_search.proxy_document_state SET document_id=NULL,disposition='deferred',
        updated_at=clock_timestamp() WHERE source_table='proxy_flows' AND source_key=$key""".update.run
    yield ()

  private def retireMissing(lease: Lease,scope: String,built: ProxyBehavior.Result,budget: Int): ConnectionIO[Int] =
    val keys = built.flows.map(_.evidence.hcursor.get[String]("correlation_id").toOption.get)
    val retained = keys.toNel.fold(Fragment.const("false"))(Fragments.in(fr"correlation_id",_))
    for
      removed <- (fr"""SELECT deployment_scope,correlation_id FROM atheros_search.proxy_flow
        WHERE deployment_scope <> $scope OR NOT (""" ++ retained ++ fr""")
        ORDER BY deployment_scope,correlation_id LIMIT $budget FOR UPDATE""").query[(String,String)].to[List]
      _ <- removed.traverse_ { case (scope,correlation) =>
        val key = Json.arr(scope.asJson,correlation.asJson).noSpaces
        retireDocument(key) *> sql"""DELETE FROM atheros_search.proxy_flow
          WHERE deployment_scope=$scope AND correlation_id=$correlation""".update.run.void
      }
      windowPredicates = built.windows.map { p =>
        val endpoint = p.evidence.hcursor.get[String]("endpoint_key").toOption.get
        val start = Timestamp.from(Instant.parse(p.evidence.hcursor.get[String]("window_start").toOption.get))
        fr"(endpoint_key=$endpoint AND window_start=$start)"
      }
      retainedWindows = windowPredicates.toNel.fold(Fragment.const("false"))(Fragments.or(_))
      remaining = (budget-removed.size).max(0)
      oldWindows <- (fr"""SELECT deployment_scope,endpoint_key,window_start FROM atheros_search.proxy_host_window
        WHERE deployment_scope <> $scope OR NOT (""" ++ retainedWindows ++ fr""")
        ORDER BY deployment_scope,window_start,endpoint_key LIMIT $remaining FOR UPDATE""")
        .query[(String,String,Timestamp)].to[List]
      _ <- oldWindows.traverse_ { case (scope,endpoint,start) =>
        sql"""DELETE FROM atheros_search.proxy_host_current WHERE deployment_scope=$scope
          AND endpoint_key=$endpoint AND latest_window_start=$start""".update.run *>
        sql"""DELETE FROM atheros_search.proxy_host_window WHERE deployment_scope=$scope
          AND endpoint_key=$endpoint AND window_start=$start""".update.run.void
      }
      _ <- ProxyPreparationSql.guard(lease)
    yield removed.size + oldWindows.size
