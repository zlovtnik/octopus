package com.sslproxy.coordinator.postgres.sql

import cats.syntax.all.*
import com.sslproxy.coordinator.dq.*
import doobie.{ConnectionIO, Fragment}
import doobie.implicits.*
import io.circe.Json
import io.circe.parser.parse
import java.util.UUID

/** Atomic authoritative DQ transitions. All identifiers and documents are bound. */
final class DqControlSql(val settings: DqSettings):
  import DqJson.*
  private def id: String = UUID.randomUUID().toString
  private def decoded(raw: String): Json = parse(raw).fold(throw _, identity)
  private def json(query: doobie.Query0[String]): ConnectionIO[Json] = query.unique.map(decoded)
  private def conflict[A](code: String): ConnectionIO[A] = doobie.free.connection.raiseError(DqError(409, code))
  private def lockRun(runId: String): ConnectionIO[Unit] =
    sql"SELECT run_id FROM octopus_core.dq_runs WHERE run_id=$runId FOR UPDATE".query[String].option.flatMap {
      case Some(_) => ().pure[ConnectionIO]
      case None => doobie.free.connection.raiseError(DqError(404, "run_not_found"))
    }
  def run(runId: String): ConnectionIO[Json] =
    json(sql"""SELECT (r.document || jsonb_build_object(
      'run_id',r.run_id,'status',r.status,'state_revision',r.state_revision,
      'input_refs',r.input_refs,'output_refs',r.output_refs,'source_revision_digest',r.source_revision_digest,
      'execution_mode',r.execution_mode,'created_at',r.created_at,'deadline_at',r.deadline_at,
      'completed_at',r.completed_at,'projection_applied',r.projection_applied,'error_code',r.error_code,
      'summary',r.summary,'current_attempt',(SELECT to_jsonb(a) FROM octopus_core.dq_attempts a
        WHERE a.attempt_id=r.current_attempt_id)))::TEXT
      FROM octopus_core.dq_runs r WHERE r.run_id=$runId""".query[String])
  private def checkedRun(principal: DqPrincipal, runId: String): ConnectionIO[Json] =
    run(runId).map { value =>
      scoped(principal, value, settings)
      if !principal.permits(scope(field(value, "parameters"))) then throw DqError(403, "forbidden_scope")
      if principal.roles.contains("service") then value
      else value.mapObject { obj =>
        obj("current_attempt").filterNot(_.isNull).fold(obj) { attempt =>
          obj.add("current_attempt",attempt.mapObject(_.remove("lease_token")))
        }
      }
    }
  def getRun(principal: DqPrincipal, runId: String): ConnectionIO[Json] =
    principal.requireRole(Set("viewer","operator","admin","automation","service"))
    checkedRun(principal, runId)
  def invocation(principal: DqPrincipal, key: String): ConnectionIO[Json] =
    principal.requireRole(Set("operator","admin","automation"))
    sql"""SELECT run_id FROM octopus_core.dq_runs WHERE installation_id=${settings.installationId}
      AND namespace=${settings.namespace} AND idempotency_key=$key""".query[String].option.flatMap {
      case Some(runId) => checkedRun(principal,runId)
      case None => doobie.free.connection.raiseError(DqError(404,"invocation_not_found"))
    }
  private def audit(actor: String, action: String, runId: Option[String], details: Json): ConnectionIO[Unit] =
    val auditId = id
    val document = canonical(details)
    sql"""INSERT INTO octopus_core.dq_audit(audit_id,installation_id,namespace,actor,action,run_id,details)
      VALUES ($auditId,${settings.installationId},${settings.namespace},$actor,$action,$runId,$document::JSONB)""".update.run.void *>
      enqueue("dq.audit.v1", auditId, Json.obj("schema_version" -> Json.fromInt(1), "event_id" -> Json.fromString(auditId),
        "event_type" -> Json.fromString(action), "installation_id" -> Json.fromString(settings.installationId),
        "namespace" -> Json.fromString(settings.namespace), "actor" -> Json.fromString(actor),
        "run_id" -> runId.fold(Json.Null)(Json.fromString), "details" -> details))
  private def enqueue(topic: String, key: String, payload: Json): ConnectionIO[Unit] =
    bounded(payload)
    val outboxId = id
    val raw = canonical(payload)
    sql"""INSERT INTO octopus_core.dq_outbox(outbox_id,topic,message_key,payload)
      VALUES ($outboxId,$topic,$key,$raw::JSONB)""".update.run.void
  private def event(runId: String, eventType: String, topic: String = "dq.events.v1"): ConnectionIO[Unit] =
    run(runId).flatMap { r =>
      val attempt = r.hcursor.downField("current_attempt")
      val envelope = Json.obj(
        "schema_version" -> Json.fromInt(1), "event_id" -> Json.fromString(id), "event_type" -> Json.fromString(eventType),
        "installation_id" -> Json.fromString(settings.installationId), "namespace" -> Json.fromString(settings.namespace),
        "run_id" -> Json.fromString(runId), "processor_id" -> field(r,"processor_id"),
        "processor_version" -> field(r,"processor_version"), "state_revision" -> field(r,"state_revision"),
        "attempt_id" -> attempt.get[String]("attempt_id").toOption.fold(Json.Null)(Json.fromString),
        "fence" -> attempt.get[Long]("fence").toOption.fold(Json.Null)(Json.fromLong),
        "occurred_at" -> Json.fromString(java.time.Instant.now().toString), "correlation_id" -> Json.fromString(runId),
        "source_revision_digest" -> field(r,"source_revision_digest"), "parameters" -> field(r,"parameters"),
        "artifacts" -> field(r,"input_refs"), "traceparent" -> r.hcursor.downField("traceparent").focus.getOrElse(Json.Null),
        "tracestate" -> r.hcursor.downField("tracestate").focus.getOrElse(Json.Null),
        "error_code" -> r.hcursor.downField("error_code").focus.getOrElse(Json.Null))
      enqueue(topic, settings.installationId + "/" + settings.namespace + "/" + runId, envelope)
    }
  def catalog(principal: DqPrincipal): ConnectionIO[Json] =
    principal.requireRole(Set("viewer","operator","admin","automation"))
    sql"""SELECT v.manifest::TEXT FROM octopus_core.dq_processors p JOIN octopus_core.dq_processor_versions v
      USING (installation_id,namespace,processor_id)
      WHERE p.installation_id=${settings.installationId} AND p.namespace=${settings.namespace}
        AND p.default_version=v.version ORDER BY v.processor_id""".query[String].to[List]
      .map(values => Json.obj("items" -> Json.fromValues(values.map(decoded))))
  def manifest(processorId: String, version: String): ConnectionIO[Json] =
    sql"""SELECT manifest::TEXT FROM octopus_core.dq_processor_versions WHERE
      installation_id=${settings.installationId} AND namespace=${settings.namespace}
      AND processor_id=$processorId AND version=$version""".query[String].option.flatMap {
      case Some(raw) => decoded(raw).pure[ConnectionIO]
      case None => doobie.free.connection.raiseError(DqError(404, "processor_version_not_found"))
    }
  def register(principal: DqPrincipal, value: Json): ConnectionIO[Json] =
    principal.requireRole(Set("processor_owner","admin"))
    scoped(principal,value,settings)
    val processorId = text(value,"processor_id")
    val version = text(value,"version")
    val digest = text(value,"implementation_digest")
    val manifestHash = text(value,"manifest_sha256")
    val schema = field(value,"parameter_schema")
    checkSchema(schema)
    if !identifier(processorId) || !identifier(version) || !settings.approvedDigests.contains(digest) ||
      manifestHash != sha256(value.mapObject(_.remove("manifest_sha256"))) ||
      text(value,"parameter_schema_sha256") != sha256(schema) || text(value,"policy_version") != settings.policyVersion ||
      text(value,"protocol_version") != "dq.v1" then throw DqError(400,"unapproved_manifest")
    val resources = field(value,"resources")
    if int(resources,"concurrency") < 1 || int(resources,"concurrency") > 256 ||
      int(resources,"timeout_seconds") < 1 || int(resources,"timeout_seconds") > 3600 ||
      long(resources,"max_input_bytes") < 1 || long(resources,"max_output_bytes") < 1 then
      throw DqError(400,"invalid_resources")
    bounded(value)
    val raw = canonical(value)
    for
      inserted <- sql"""INSERT INTO octopus_core.dq_processor_versions(installation_id,namespace,processor_id,version,
        manifest,manifest_sha256,implementation_digest,parameter_schema_sha256,policy_version,registered_by)
        VALUES (${settings.installationId},${settings.namespace},$processorId,$version,$raw::JSONB,$manifestHash,
        $digest,${text(value,"parameter_schema_sha256")},${settings.policyVersion},${principal.actor})
        ON CONFLICT DO NOTHING""".update.run
      existing <- manifest(processorId,version)
      _ <- if existing != value then conflict("immutable_version_conflict") else ().pure[ConnectionIO]
      _ <- sql"""INSERT INTO octopus_core.dq_processors(installation_id,namespace,processor_id,default_version)
        VALUES (${settings.installationId},${settings.namespace},$processorId,$version) ON CONFLICT DO NOTHING""".update.run
      _ <- if inserted == 1 then audit(principal.actor,"register",None,Json.obj("manifest_sha256" -> Json.fromString(manifestHash))) *>
        enqueue("dq.registry.v1",settings.installationId + "/" + settings.namespace + "/" + processorId + "/" + version,value)
        else ().pure[ConnectionIO]
    yield existing
  def admit(principal: DqPrincipal, request: Json): ConnectionIO[Json] =
    principal.requireRole(Set("operator","admin","automation","service"))
    scoped(principal,request,settings)
    bounded(request)
    val allowed = Set("installation_id","namespace","processor_id","processor_version","operation","parameters",
      "idempotency_key","deadline_at","traceparent","tracestate")
    if request.asObject.exists(obj => !obj.keys.forall(allowed)) then throw DqError(400,"unsupported_admission_field")
    val processorId = text(request,"processor_id")
    val parameters = field(request,"parameters")
    val authorizedScope = scope(parameters)
    val key = text(request,"idempotency_key")
    val operation = text(request,"operation")
    if !identifier(processorId) || key.isBlank || key.length > 256 ||
      !Set("prepare_candidates","reconcile_decision").contains(operation) then throw DqError(400,"invalid_admission")
    val fingerprint = sha256(request.mapObject(_.filter { case (k,_) => !Set("traceparent","tracestate").contains(k) }))
    val lockKey = settings.installationId + "/" + settings.namespace
    for
      _ <- sql"SELECT pg_advisory_xact_lock(hashtextextended($lockKey,0))".query[Unit].unique
      prior <- sql"""SELECT run_id,request_fingerprint FROM octopus_core.dq_runs WHERE
        installation_id=${settings.installationId} AND namespace=${settings.namespace} AND idempotency_key=$key"""
        .query[(String,String)].option
      result <- prior match
        case Some((runId,hash)) => if hash == fingerprint then checkedRun(principal,runId) else conflict("idempotency_conflict")
        case None =>
          val mode = settings.mode(authorizedScope).getOrElse(throw DqError(403,"beta_disabled"))
          for
            active <- sql"""SELECT EXISTS(SELECT 1 FROM octopus_core.dq_scope_policies WHERE installation_id=${settings.installationId}
              AND namespace=${settings.namespace} AND authorized_scope=$authorizedScope AND mode=$mode
              AND policy_version=${settings.policyVersion})""".query[Boolean].unique
            _ <- if active then ().pure[ConnectionIO] else doobie.free.connection.raiseError[Unit](DqError(403,"beta_disabled"))
            version <- optional(request,"processor_version") match
              case Some(v) => v.pure[ConnectionIO]
              case None => sql"""SELECT default_version FROM octopus_core.dq_processors WHERE
                installation_id=${settings.installationId} AND namespace=${settings.namespace} AND processor_id=$processorId"""
                .query[String].option.flatMap(_.fold(doobie.free.connection.raiseError[String](DqError(404,"processor_not_found")))(_.pure[ConnectionIO]))
            pinned <- manifest(processorId,version)
            _ <- doobie.free.connection.delay {
              if !settings.approvedDigests.contains(text(pinned,"implementation_digest")) || text(pinned,"policy_version") != settings.policyVersion then
                throw DqError(403,"unapproved_manifest")
              validate(field(pinned,"parameter_schema"),parameters)
              if processorId == "atheros.dedup" && (text(parameters,"policy_version") != settings.policyVersion ||
                int(parameters,"max_candidates") < 1 || int(parameters,"max_candidates") > 1000) then throw DqError(400,"invalid_dedup_parameters")
            }
            count <- sql"""SELECT COUNT(*) FROM octopus_core.dq_runs WHERE installation_id=${settings.installationId}
              AND namespace=${settings.namespace} AND status NOT IN ('succeeded','failed','cancelled')""".query[Long].unique
            _ <- if count >= settings.maxQueued then doobie.free.connection.raiseError[Unit](DqError(429,"admission_capacity")) else ().pure[ConnectionIO]
            runId = id
            until <- doobie.free.connection.delay {
              val at = deadline(request)
              if !at.isAfter(java.time.Instant.now()) || at.isAfter(java.time.Instant.now().plusSeconds(86400)) then
                throw DqError(400,"invalid_deadline")
              at.toString
            }
            doc = request.deepMerge(Json.obj("processor_version" -> Json.fromString(version),
              "implementation_digest" -> field(pinned,"implementation_digest"), "parameters_sha256" -> Json.fromString(sha256(parameters)),
              "policy_version" -> Json.fromString(settings.policyVersion), "release_channel" -> Json.fromString("beta")))
            _ <- sql"""INSERT INTO octopus_core.dq_runs(run_id,installation_id,namespace,processor_id,processor_version,
              authorized_scope,operation,idempotency_key,request_fingerprint,document,manifest,policy_version,execution_mode,status,deadline_at)
              VALUES ($runId,${settings.installationId},${settings.namespace},$processorId,$version,$authorizedScope,$operation,$key,
              $fingerprint,${canonical(doc)}::JSONB,${canonical(pinned)}::JSONB,${settings.policyVersion},$mode,'queued',$until::TIMESTAMPTZ)""".update.run
            _ <- audit(principal.actor,"invoke",Some(runId),Json.obj("request_fingerprint" -> Json.fromString(fingerprint)))
            result <- run(runId)
          yield result
    yield result
  def cancel(principal: DqPrincipal, runId: String): ConnectionIO[Json] =
    principal.requireRole(Set("operator","admin"))
    for
      _ <- lockRun(runId)
      r <- checkedRun(principal,runId)
      changed <- sql"""UPDATE octopus_core.dq_runs SET status='cancelled',fence=fence+1,state_revision=state_revision+1,
        completed_at=CURRENT_TIMESTAMP,work_owner=NULL,work_expires_at=NULL WHERE run_id=$runId
        AND status NOT IN ('succeeded','failed','cancelled')""".update.run
      _ <- if changed == 1 then
        sql"""UPDATE octopus_core.dq_attempts SET status='cancelled',finished_at=CURRENT_TIMESTAMP
          WHERE run_id=$runId AND status IN ('reserved','accepted','running','applying')""".update.run.void *>
          audit(principal.actor,"cancel",Some(runId),Json.obj()) *> event(runId,"cancel","dq.commands.v1") *> event(runId,"cancelled")
        else ().pure[ConnectionIO]
      result <- if changed == 0 then r.pure[ConnectionIO] else checkedRun(principal,runId)
    yield result
  def replay(principal: DqPrincipal, runId: String, request: Json): ConnectionIO[Json] =
    principal.requireRole(Set("operator","admin"))
    for
      r <- checkedRun(principal,runId)
      _ <- if !Set("failed","cancelled").contains(text(r,"status")) then conflict("run_not_replayable") else ().pure[ConnectionIO]
      key = text(request,"idempotency_key")
      _ <- if key == text(r,"idempotency_key") then conflict("replay_requires_new_key") else ().pure[ConnectionIO]
      replayRequest = Json.obj("installation_id" -> field(r,"installation_id"),"namespace" -> field(r,"namespace"),
        "processor_id" -> field(r,"processor_id"),"processor_version" -> field(r,"processor_version"),
        "operation" -> field(r,"operation"),"parameters" -> field(r,"parameters"),"idempotency_key" -> Json.fromString(key))
      result <- admit(principal,replayRequest)
      _ <- audit(principal.actor,"replay",Some(text(result,"run_id")),Json.obj("source_run_id" -> Json.fromString(runId)))
    yield result
  def heartbeat(request: Json): ConnectionIO[Json] =
    scoped(DqPrincipal.internal,request,settings)
    val worker = text(request,"worker_instance_id")
    val processorId = text(request,"processor_id")
    val version = text(request,"processor_version")
    val address = text(request,"address")
    val concurrency = int(request,"concurrency")
    val digest = text(request,"implementation_digest")
    val ready = request.hcursor.get[Boolean]("ready").fold(_ => throw DqError(400,"invalid_ready"),identity)
    if !identifier(worker) || !address.matches("[A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}") || concurrency < 1 || concurrency > 256 then
      throw DqError(400,"invalid_worker")
    for
      m <- manifest(processorId,version)
      _ <- if text(m,"implementation_digest") != digest || !settings.approvedDigests.contains(digest) then conflict("worker_digest_mismatch") else ().pure[ConnectionIO]
      changed <- sql"""INSERT INTO octopus_core.dq_workers(worker_instance_id,installation_id,namespace,processor_id,
        processor_version,implementation_digest,address,concurrency,ready)
        VALUES ($worker,${settings.installationId},${settings.namespace},$processorId,$version,$digest,$address,$concurrency,$ready)
        ON CONFLICT (worker_instance_id) DO UPDATE SET ready=EXCLUDED.ready,last_seen_at=CURRENT_TIMESTAMP
        WHERE dq_workers.installation_id=EXCLUDED.installation_id AND dq_workers.namespace=EXCLUDED.namespace
          AND dq_workers.processor_id=EXCLUDED.processor_id AND dq_workers.processor_version=EXCLUDED.processor_version
          AND dq_workers.implementation_digest=EXCLUDED.implementation_digest AND dq_workers.address=EXCLUDED.address
          AND dq_workers.concurrency=EXCLUDED.concurrency""".update.run
      _ <- if changed != 1 then conflict("immutable_worker_identity") else ().pure[ConnectionIO]
    yield Json.obj("accepted" -> Json.True)
  def dispatches: ConnectionIO[Json] =
    sql"""SELECT run_id FROM octopus_core.dq_runs r WHERE installation_id=${settings.installationId}
      AND namespace=${settings.namespace} AND deadline_at>CURRENT_TIMESTAMP AND
      ((status IN ('preparing','retrying') AND jsonb_array_length(input_refs)>0 AND next_attempt_at<=CURRENT_TIMESTAMP AND work_owner IS NULL)
        OR (status='running' AND EXISTS(SELECT 1 FROM octopus_core.dq_attempts a WHERE a.attempt_id=r.current_attempt_id
          AND a.status='reserved' AND a.lease_expires_at>CURRENT_TIMESTAMP)))
      ORDER BY created_at,run_id LIMIT 100""".query[String].to[List].flatMap(_.traverse { runId =>
      run(runId).flatMap(r => manifest(text(r,"processor_id"),text(r,"processor_version")).map(m =>
        Json.obj("run" -> r,"manifest" -> m,"attempt" -> r.hcursor.downField("current_attempt").focus.getOrElse(Json.Null))))
    }).map(items => Json.obj("items" -> Json.fromValues(items)))
  private def policy(r: Json): Boolean = settings.mode(scope(field(r,"parameters"))).contains(text(r,"execution_mode")) &&
    text(r,"policy_version") == settings.policyVersion && settings.approvedDigests.contains(text(r,"implementation_digest"))
  private def currentPolicy(r: Json): ConnectionIO[Boolean] =
    sql"""SELECT EXISTS(SELECT 1 FROM octopus_core.dq_scope_policies WHERE installation_id=${settings.installationId}
      AND namespace=${settings.namespace} AND authorized_scope=${scope(field(r,"parameters"))}
      AND mode=${text(r,"execution_mode")} AND policy_version=${settings.policyVersion})""".query[Boolean].unique.map(_ && policy(r))
  def reserve(runId: String, request: Json): ConnectionIO[Json] =
    scoped(DqPrincipal.internal,request,settings)
    val worker = text(request,"worker_instance_id")
    val slot = int(request,"worker_slot")
    for
      _ <- lockRun(runId)
      r <- run(runId)
      permitted <- currentPolicy(r)
      _ <- if !permitted then conflict("policy_changed") else ().pure[ConnectionIO]
      prior <- sql"""SELECT attempt_id FROM octopus_core.dq_attempts WHERE run_id=$runId AND
        status IN ('reserved','accepted','running','applying') AND lease_expires_at>CURRENT_TIMESTAMP""".query[String].option
      _ <- prior match
        case Some(_) => ().pure[ConnectionIO]
        case None =>
          for
            w <- sql"""SELECT processor_id,processor_version,implementation_digest,address,concurrency,ready,
              last_seen_at>CURRENT_TIMESTAMP-INTERVAL '30 seconds' FROM octopus_core.dq_workers
              WHERE worker_instance_id=$worker AND installation_id=${settings.installationId} AND namespace=${settings.namespace}
              FOR UPDATE""".query[(String,String,String,String,Int,Boolean,Boolean)].option
            _ <- w match
              case Some((p,v,d,_,credits,true,true)) if p==text(r,"processor_id") && v==text(r,"processor_version") &&
                d==text(r,"implementation_digest") && slot>=0 && slot<credits => ().pure[ConnectionIO]
              case _ => conflict("worker_unavailable")
            occupied <- sql"""SELECT EXISTS(SELECT 1 FROM octopus_core.dq_attempts WHERE worker_instance_id=$worker AND worker_slot=$slot
              AND status IN ('reserved','accepted','running'))""".query[Boolean].unique
            _ <- if occupied then conflict("worker_slot_busy") else ().pure[ConnectionIO]
            changed <- sql"""UPDATE octopus_core.dq_runs SET status='running',fence=fence+1,attempt_count=attempt_count+1,
              state_revision=state_revision+1 WHERE run_id=$runId AND status IN ('preparing','retrying') AND
              jsonb_array_length(input_refs)>0 AND attempt_count<5 AND next_attempt_at<=CURRENT_TIMESTAMP
              AND deadline_at>CURRENT_TIMESTAMP AND work_owner IS NULL""".update.run
            _ <- if changed!=1 then conflict("run_not_dispatchable") else ().pure[ConnectionIO]
            attemptId = id
            token = id
            _ <- sql"""INSERT INTO octopus_core.dq_attempts(attempt_id,run_id,attempt_number,worker_instance_id,worker_slot,
              lease_token,fence,lease_expires_at,status) SELECT $attemptId,run_id,attempt_count,$worker,$slot,$token,fence,
              LEAST(deadline_at,CURRENT_TIMESTAMP+${settings.leaseSeconds}*INTERVAL '1 second'),'reserved'
              FROM octopus_core.dq_runs WHERE run_id=$runId""".update.run
            _ <- sql"UPDATE octopus_core.dq_runs SET current_attempt_id=$attemptId WHERE run_id=$runId".update.run
          yield ()
      result <- run(runId)
      m <- manifest(text(result,"processor_id"),text(result,"processor_version"))
      address <- sql"""SELECT w.address FROM octopus_core.dq_workers w JOIN octopus_core.dq_attempts a
        ON a.worker_instance_id=w.worker_instance_id WHERE a.attempt_id=${text(field(result,"current_attempt"),"attempt_id")}""".query[String].unique
    yield Json.obj("run" -> result,"attempt" -> field(result,"current_attempt"),"manifest" -> m,
      "worker_address" -> Json.fromString(address))
  def lease(attemptId: String, request: Json, accept: Boolean): ConnectionIO[Json] =
    scoped(DqPrincipal.internal,request,settings)
    val runId = text(request,"run_id")
    for
      _ <- lockRun(runId)
      r <- run(runId)
      permitted <- currentPolicy(r)
      _ <- if !permitted then conflict("policy_changed") else ().pure[ConnectionIO]
      changed <- sql"""UPDATE octopus_core.dq_attempts SET
        status=CASE WHEN $accept AND status='reserved' THEN 'accepted' ELSE status END,
        lease_expires_at=LEAST(${text(r,"deadline_at")}::TIMESTAMPTZ,CURRENT_TIMESTAMP+${settings.leaseSeconds}*INTERVAL '1 second')
        WHERE attempt_id=$attemptId AND run_id=$runId AND worker_instance_id=${text(request,"worker_instance_id")}
        AND lease_token=${text(request,"lease_token")} AND fence=${long(request,"fence")}
        AND status IN ('reserved','accepted','running','applying') AND lease_expires_at>CURRENT_TIMESTAMP
        AND EXISTS(SELECT 1 FROM octopus_core.dq_runs WHERE run_id=$runId AND current_attempt_id=$attemptId
          AND fence=${long(request,"fence")} AND status IN ('running','applying'))""".update.run
      _ <- if changed!=1 then conflict("stale_lease") else ().pure[ConnectionIO]
      result <- run(runId)
    yield field(result,"current_attempt")

  def claimPreparation(owner: String, leaseSeconds: Int, limit: Int): ConnectionIO[List[Json]] =
    sql"""WITH selected AS (SELECT run_id FROM octopus_core.dq_runs WHERE
      installation_id=${settings.installationId} AND namespace=${settings.namespace}
      AND status IN ('queued','preparing','retrying') AND jsonb_array_length(input_refs)=0
      AND (work_owner IS NULL OR work_expires_at<=CURRENT_TIMESTAMP) AND next_attempt_at<=CURRENT_TIMESTAMP
      AND deadline_at>CURRENT_TIMESTAMP AND preparation_attempts<5 ORDER BY created_at,run_id
      FOR UPDATE SKIP LOCKED LIMIT ${limit.max(1).min(100)})
      UPDATE octopus_core.dq_runs r SET status='preparing',work_owner=$owner,
        work_expires_at=CURRENT_TIMESTAMP+${leaseSeconds.max(1)}*INTERVAL '1 second',preparation_attempts=preparation_attempts+1
      FROM selected WHERE selected.run_id=r.run_id RETURNING r.run_id""".query[String].to[List].flatMap(_.traverse(run))
  private def artifacts(runId: String, refs: Json): ConnectionIO[Unit] =
    refs.asArray.getOrElse(Vector.empty).toList.traverse_ { ref =>
      sql"""INSERT INTO octopus_core.dq_artifacts(artifact_id,run_id,reference,installation_id,namespace)
        VALUES (${text(ref,"artifact_id")},$runId,${canonical(ref)}::JSONB,${settings.installationId},${settings.namespace})
        ON CONFLICT (artifact_id) DO NOTHING""".update.run.void *>
        sql"SELECT reference::TEXT FROM octopus_core.dq_artifacts WHERE artifact_id=${text(ref,"artifact_id")}".query[String].unique
          .flatMap(raw => if decoded(raw)==ref then ().pure[ConnectionIO] else conflict("artifact_identity_conflict"))
    }
  def completePreparation(runId: String, owner: String, refs: Json, revision: String): ConnectionIO[Boolean] =
    validateArtifacts(refs,settings,runId,None,true)
    if refs.asArray.forall(_.isEmpty) || !hash(revision) then throw DqError(400,"invalid_preparation")
    for
      _ <- lockRun(runId)
      r <- run(runId)
      permitted <- currentPolicy(r)
      changed <- if permitted then sql"""UPDATE octopus_core.dq_runs SET input_refs=${canonical(refs)}::JSONB,
        source_revision_digest=$revision,state_revision=state_revision+1,work_owner=NULL,work_expires_at=NULL
        WHERE run_id=$runId AND status='preparing' AND work_owner=$owner AND work_expires_at>CURRENT_TIMESTAMP
        AND deadline_at>CURRENT_TIMESTAMP""".update.run else 0.pure[ConnectionIO]
      _ <- if changed==1 then artifacts(runId,refs) *> event(runId,"invoke","dq.commands.v1") else ().pure[ConnectionIO]
    yield changed==1
  def failWork(runId: String, owner: String, code: String, permanent: Boolean, needsPreparation: Boolean): ConnectionIO[Unit] =
    for
      _ <- lockRun(runId)
      changed <- sql"""UPDATE octopus_core.dq_runs SET status=CASE
        WHEN $permanent OR ($needsPreparation AND (preparation_attempts>=5 OR attempt_count>=5)) OR (NOT $needsPreparation AND application_attempts>=5)
        THEN 'failed' WHEN NOT $needsPreparation AND status='applying' THEN 'applying' ELSE 'retrying' END,
        error_code=$code,fence=CASE WHEN $needsPreparation OR $permanent THEN fence+1 ELSE fence END,state_revision=state_revision+1,
        completed_at=CASE WHEN $permanent OR ($needsPreparation AND (preparation_attempts>=5 OR attempt_count>=5)) OR
          (NOT $needsPreparation AND application_attempts>=5) THEN CURRENT_TIMESTAMP ELSE NULL END,
        input_refs=CASE WHEN $needsPreparation THEN '[]'::JSONB ELSE input_refs END,
        output_refs=CASE WHEN $needsPreparation THEN '[]'::JSONB ELSE output_refs END,
        current_attempt_id=CASE WHEN $needsPreparation OR $permanent THEN NULL ELSE current_attempt_id END,
        work_owner=NULL,work_expires_at=NULL,
        next_attempt_at=CURRENT_TIMESTAMP+(random()*LEAST(60,POWER(2,GREATEST(application_attempts,preparation_attempts)-1)))*INTERVAL '1 second'
        WHERE run_id=$runId AND status IN ('preparing','applying') AND work_owner=$owner AND work_expires_at>CURRENT_TIMESTAMP""".update.run
      _ <- if changed==1 then
        sql"""UPDATE octopus_core.dq_attempts SET status='failed',finished_at=CURRENT_TIMESTAMP WHERE run_id=$runId
          AND status IN ('reserved','accepted','running','applying') AND ($needsPreparation OR $permanent OR
            EXISTS(SELECT 1 FROM octopus_core.dq_runs WHERE run_id=$runId AND status='failed'))""".update.run.void *> terminalOrRetry(runId)
        else ().pure[ConnectionIO]
    yield ()
  private def terminalOrRetry(runId: String): ConnectionIO[Unit] = run(runId).flatMap { r =>
    if text(r,"status")=="failed" then event(runId,"failed") *> event(runId,"failed","dq.dlq.v1")
    else if field(r,"input_refs").asArray.exists(_.nonEmpty) then event(runId,"retry_dispatch","dq.commands.v1")
    else ().pure[ConnectionIO]
  }
  def claimOutput(owner: String, leaseSeconds: Int, limit: Int): ConnectionIO[List[Json]] =
    sql"""WITH selected AS (SELECT run_id FROM octopus_core.dq_runs WHERE status='applying'
      AND installation_id=${settings.installationId} AND namespace=${settings.namespace}
      AND (work_owner IS NULL OR work_expires_at<=CURRENT_TIMESTAMP) AND deadline_at>CURRENT_TIMESTAMP
      AND next_attempt_at<=CURRENT_TIMESTAMP AND application_attempts<5
      ORDER BY created_at,run_id FOR UPDATE SKIP LOCKED LIMIT ${limit.max(1).min(100)})
      UPDATE octopus_core.dq_runs r SET work_owner=$owner,application_attempts=application_attempts+1,
        work_expires_at=CURRENT_TIMESTAMP+${leaseSeconds.max(1)}*INTERVAL '1 second'
      FROM selected WHERE r.run_id=selected.run_id RETURNING r.run_id""".query[String].to[List].flatMap(_.traverse(run))
  def guardOutput(runId: String, attemptId: String, fence: Long, owner: String, revision: String): ConnectionIO[Boolean] =
    for
      _ <- lockRun(runId)
      r <- run(runId)
      valid <- sql"""SELECT EXISTS(SELECT 1 FROM octopus_core.dq_runs r JOIN octopus_core.dq_attempts a
        ON a.attempt_id=r.current_attempt_id WHERE r.run_id=$runId AND r.status='applying' AND r.work_owner=$owner
        AND r.work_expires_at>CURRENT_TIMESTAMP AND r.deadline_at>CURRENT_TIMESTAMP AND r.source_revision_digest=$revision
        AND a.attempt_id=$attemptId AND a.fence=$fence AND r.fence=$fence AND a.status='applying'
        )""".query[Boolean].unique
      permitted <- currentPolicy(r)
    yield valid && permitted
  def completeOutput(runId: String, attemptId: String, fence: Long, owner: String, revision: String,
    applied: Boolean, summary: Json): ConnectionIO[Boolean] =
    for
      valid <- guardOutput(runId,attemptId,fence,owner,revision)
      _ <- if valid then
        sql"""UPDATE octopus_core.dq_runs SET status='succeeded',state_revision=state_revision+1,
          projection_applied=$applied,summary=${canonical(summary)}::JSONB,completed_at=CURRENT_TIMESTAMP,
          work_owner=NULL,work_expires_at=NULL WHERE run_id=$runId""".update.run.void *>
        sql"""UPDATE octopus_core.dq_attempts SET status='succeeded',finished_at=CURRENT_TIMESTAMP
          WHERE attempt_id=$attemptId""".update.run.void *>
        sql"""UPDATE octopus_core.dq_decision_sources SET completed_at=CURRENT_TIMESTAMP WHERE run_id=$runId""".update.run.void *>
        event(runId,"succeeded") else ().pure[ConnectionIO]
    yield valid
  def expireAttempts: ConnectionIO[Int] =
    sql"""SELECT run_id FROM octopus_core.dq_runs WHERE installation_id=${settings.installationId} AND namespace=${settings.namespace}
      AND status NOT IN ('succeeded','failed','cancelled') AND
      (deadline_at<=CURRENT_TIMESTAMP OR (work_expires_at<=CURRENT_TIMESTAMP AND
        ((status='applying' AND application_attempts>=5) OR (status='preparing' AND preparation_attempts>=5)))
        OR EXISTS(SELECT 1 FROM octopus_core.dq_attempts a WHERE a.attempt_id=current_attempt_id
        AND a.status IN ('reserved','accepted','running') AND a.lease_expires_at<=CURRENT_TIMESTAMP))
      ORDER BY run_id LIMIT 100""".query[String].to[List].flatMap(_.traverse { runId =>
      for
        _ <- lockRun(runId)
        changed <- sql"""UPDATE octopus_core.dq_runs SET
          status=CASE WHEN attempt_count>=5 OR preparation_attempts>=5 OR application_attempts>=5 OR deadline_at<=CURRENT_TIMESTAMP THEN 'failed' ELSE 'retrying' END,
          fence=fence+1,state_revision=state_revision+1,current_attempt_id=NULL,work_owner=NULL,work_expires_at=NULL,
          error_code='lease_expired',output_refs='[]'::JSONB,
          completed_at=CASE WHEN attempt_count>=5 OR preparation_attempts>=5 OR application_attempts>=5 OR deadline_at<=CURRENT_TIMESTAMP THEN CURRENT_TIMESTAMP ELSE NULL END,
          next_attempt_at=CURRENT_TIMESTAMP+(random()*LEAST(60,POWER(2,attempt_count-1)))*INTERVAL '1 second'
          WHERE run_id=$runId AND status NOT IN ('succeeded','failed','cancelled') AND
          (deadline_at<=CURRENT_TIMESTAMP OR (work_expires_at<=CURRENT_TIMESTAMP AND
            ((status='applying' AND application_attempts>=5) OR (status='preparing' AND preparation_attempts>=5)))
            OR EXISTS(SELECT 1 FROM octopus_core.dq_attempts a WHERE a.attempt_id=current_attempt_id
            AND a.status IN ('reserved','accepted','running') AND a.lease_expires_at<=CURRENT_TIMESTAMP))""".update.run
        _ <- if changed==1 then sql"""UPDATE octopus_core.dq_attempts SET status='expired',finished_at=CURRENT_TIMESTAMP
          WHERE run_id=$runId AND status IN ('reserved','accepted','running','applying')""".update.run.void *> terminalOrRetry(runId)
          else ().pure[ConnectionIO]
      yield changed
    }).map(_.sum)

  def reconcilePolicies: ConnectionIO[Unit] =
    val replacePolicies =
      if settings.installationId.isBlank || settings.namespace.isBlank then
        sql"SELECT EXISTS(SELECT 1 FROM octopus_core.dq_scope_policies WHERE mode='enabled')".query[Boolean].unique.flatMap { active =>
          if active then doobie.free.connection.raiseError[Unit](DqError(503,"rollback_requires_installation_scope")) else ().pure[ConnectionIO]
        }
      else
        sql"""UPDATE octopus_core.dq_scope_policies SET mode='disabled' WHERE
          installation_id=${settings.installationId} AND namespace=${settings.namespace}""".update.run.void *>
        settings.scopeModes.toList.traverse_ { case (scope,mode) =>
          sql"""INSERT INTO octopus_core.dq_scope_policies(installation_id,namespace,authorized_scope,mode,policy_version)
            VALUES (${settings.installationId},${settings.namespace},$scope,$mode,${settings.policyVersion})
            ON CONFLICT (installation_id,namespace,authorized_scope) DO UPDATE SET mode=EXCLUDED.mode,policy_version=EXCLUDED.policy_version""".update.run.void
        }
    replacePolicies *> sql"""SELECT run_id FROM octopus_core.dq_runs WHERE
      installation_id=${settings.installationId} AND namespace=${settings.namespace}
      AND status NOT IN ('succeeded','failed','cancelled') ORDER BY run_id"""
      .query[String].to[List].flatMap(_.traverse_ { runId =>
        lockRun(runId) *> run(runId).flatMap { r =>
          if policy(r) then ().pure[ConnectionIO]
          else cancel(DqPrincipal("policy-reconcile",Set("admin"),Set("*")),runId).void
        }
      })
  def claimOutbox(owner: String, seconds: Int, limit: Int): ConnectionIO[List[DqOutbox]] =
    val token = id
    sql"""WITH selected AS (SELECT outbox_id FROM octopus_core.dq_outbox WHERE
      (status='pending' AND next_attempt_at<=CURRENT_TIMESTAMP) OR (status='leased' AND lease_expires_at<=CURRENT_TIMESTAMP)
      ORDER BY created_at,outbox_id FOR UPDATE SKIP LOCKED LIMIT ${limit.max(1).min(100)})
      UPDATE octopus_core.dq_outbox o SET status='leased',owner=$owner,token=$token,fence=fence+1,
      lease_expires_at=CURRENT_TIMESTAMP+${seconds.max(1)}*INTERVAL '1 second',attempt_count=attempt_count+1
      FROM selected WHERE selected.outbox_id=o.outbox_id RETURNING o.outbox_id,o.topic,o.message_key,o.payload::TEXT,o.fence"""
      .query[(String,String,String,String,Long)].to[List].map(_.map { case (outboxId,topic,key,raw,fence) =>
        DqOutbox(outboxId,topic,key,decoded(raw),owner,token,fence)
      })
  private def outboxLease(item: DqOutbox): Fragment = fr"""outbox_id=${item.id} AND status='leased' AND owner=${item.owner}
    AND token=${item.token} AND fence=${item.fence} AND lease_expires_at>CURRENT_TIMESTAMP"""
  def acknowledgeOutbox(item: DqOutbox): ConnectionIO[Boolean] =
    (fr"""UPDATE octopus_core.dq_outbox SET status='published',published_at=CURRENT_TIMESTAMP,owner=NULL,token=NULL,
      lease_expires_at=NULL WHERE """ ++ outboxLease(item)).update.run.map(_==1)
  def failOutbox(item: DqOutbox): ConnectionIO[Unit] =
    (fr"""UPDATE octopus_core.dq_outbox SET status='pending',owner=NULL,token=NULL,lease_expires_at=NULL,
      next_attempt_at=CURRENT_TIMESTAMP+(random()*LEAST(60,POWER(2,LEAST(6,attempt_count-1))))*INTERVAL '1 second'
      WHERE """ ++ outboxLease(item)).update.run.void

  def artifact(principal: DqPrincipal, artifactId: String, access: Boolean): ConnectionIO[Json] =
    principal.requireRole(Set("viewer","operator","admin"))
    for
      row <- sql"SELECT run_id,reference::TEXT FROM octopus_core.dq_artifacts WHERE artifact_id=$artifactId"
        .query[(String,String)].option.flatMap(_.fold(doobie.free.connection.raiseError[(String,String)](DqError(404,"artifact_not_found")))(_.pure[ConnectionIO]))
      _ <- checkedRun(principal,row._1)
      _ <- if access then audit(principal.actor,"artifact_access",Some(row._1),Json.obj("artifact_id" -> Json.fromString(artifactId)))
        else ().pure[ConnectionIO]
    yield decoded(row._2)

  def receipt(request: Json): ConnectionIO[Json] =
    scoped(DqPrincipal.internal,request,settings)
    val group = text(request,"consumer_group")
    val topic = text(request,"topic")
    val partition = int(request,"partition")
    val offset = long(request,"offset")
    val eventId = text(request,"event_id")
    val payloadHash = text(request,"payload_sha256")
    val envelope = request.hcursor.downField("envelope").focus.filterNot(_.isNull)
    envelope.foreach(bounded)
    val quarantine = optional(request,"quarantine_reason")
    if !Set("dq.commands.v1","dq.worker-reports.v1").contains(topic) || partition<0 || offset<0 || !hash(payloadHash) ||
      eventId.isBlank || eventId.length>256 || group.isBlank || group.length>256 || (envelope.isEmpty && quarantine.isEmpty) then
      throw DqError(400,"invalid_receipt")
    for
      _ <- sql"SELECT pg_advisory_xact_lock(hashtextextended($eventId,0))".query[Unit].unique
      same <- sql"""SELECT payload_sha256 FROM octopus_core.dq_receipts WHERE consumer_group=$group AND topic=$topic
        AND partition_id=$partition AND offset_id=$offset""".query[String].option
      duplicate <- sql"SELECT EXISTS(SELECT 1 FROM octopus_core.dq_receipts WHERE event_id=$eventId AND payload_sha256=$payloadHash)".query[Boolean].unique
      conflictEvent <- sql"SELECT EXISTS(SELECT 1 FROM octopus_core.dq_receipts WHERE event_id=$eventId AND payload_sha256<>$payloadHash)".query[Boolean].unique
      reason = if same.exists(_!=payloadHash) || conflictEvent then Some("receipt_integrity_conflict") else quarantine
      result <- if same.isDefined then
        reason.traverse_(why => audit("broker-consumer","receipt_conflict",None,Json.obj("event_id" -> Json.fromString(eventId),
          "payload_sha256" -> Json.fromString(payloadHash),"error_code" -> Json.fromString(why)))) *>
          Json.obj("duplicate" -> Json.True,"quarantined" -> Json.fromBoolean(reason.isDefined)).pure[ConnectionIO]
        else
          for
            _ <- sql"""INSERT INTO octopus_core.dq_receipts(consumer_group,topic,partition_id,offset_id,event_id,payload_sha256,
              installation_id,namespace,envelope,quarantine_reason) VALUES ($group,$topic,$partition,$offset,$eventId,$payloadHash,
              ${settings.installationId},${settings.namespace},${envelope.map(canonical)}::JSONB,$reason)""".update.run
            applied <- if duplicate || reason.isDefined then false.pure[ConnectionIO]
              else envelope.fold(false.pure[ConnectionIO]) { e =>
                doobie.free.connection.delay(validateEnvelope(e,eventId)).attempt.flatMap {
                  case Left(_) => quarantineReceipt(group,topic,partition,offset,"invalid_envelope").as(false)
                  case Right(_) => if topic=="dq.worker-reports.v1" then report(e).handleErrorWith {
                    case _: DqError => quarantineReceipt(group,topic,partition,offset,"invalid_or_stale_report").as(false)
                    case error => doobie.free.connection.raiseError(error)
                  } else true.pure[ConnectionIO]
                }
              }
            _ <- reason.traverse_(why => quarantineReceipt(group,topic,partition,offset,why))
            parked <- sql"""SELECT quarantine_reason IS NOT NULL FROM octopus_core.dq_receipts
              WHERE consumer_group=$group AND topic=$topic AND partition_id=$partition AND offset_id=$offset""".query[Boolean].unique
          yield Json.obj("duplicate" -> Json.fromBoolean(duplicate),"applied" -> Json.fromBoolean(applied),"quarantined" -> Json.fromBoolean(parked))
    yield result
  private def quarantineReceipt(group: String, topic: String, partition: Int, offset: Long, reason: String): ConnectionIO[Unit] =
    sql"""UPDATE octopus_core.dq_receipts SET quarantine_reason=$reason WHERE consumer_group=$group AND topic=$topic
      AND partition_id=$partition AND offset_id=$offset""".update.run.void *>
    enqueue("dq.dlq.v1",topic + "/" + partition.toString + "/" + offset.toString,
      Json.obj("schema_version" -> Json.fromInt(1),"event_id" -> Json.fromString(id),"event_type" -> Json.fromString("quarantined"),
        "installation_id" -> Json.fromString(settings.installationId),"namespace" -> Json.fromString(settings.namespace),
        "source_topic" -> Json.fromString(topic),"source_partition" -> Json.fromInt(partition),"source_offset" -> Json.fromLong(offset),
        "error_code" -> Json.fromString(reason)))
  private def validateEnvelope(e: Json, eventId: String): Unit =
    scoped(DqPrincipal.internal,e,settings)
    if int(e,"schema_version")!=1 || text(e,"event_id")!=eventId || !identifier(text(e,"run_id")) ||
      scala.util.Try(java.time.Instant.parse(text(e,"occurred_at"))).isFailure then throw DqError(400,"invalid_envelope")
  private def report(e: Json): ConnectionIO[Boolean] =
    val runId = text(e,"run_id")
    val attemptId = text(e,"attempt_id")
    val fence = long(e,"fence")
    val kind = text(e,"event_type")
    val sequence = long(e,"sequence")
    for
      _ <- lockRun(runId)
      r <- run(runId)
      valid <- sql"""SELECT EXISTS(SELECT 1 FROM octopus_core.dq_attempts a JOIN octopus_core.dq_runs r ON r.run_id=a.run_id
        WHERE a.attempt_id=$attemptId AND a.run_id=$runId AND a.fence=$fence AND r.fence=$fence
        AND r.current_attempt_id=$attemptId AND r.status IN ('running','applying') AND r.deadline_at>CURRENT_TIMESTAMP
        AND a.status IN ('reserved','accepted','running','applying') AND a.lease_expires_at>CURRENT_TIMESTAMP
        AND a.report_sequence<$sequence)""".query[Boolean].unique
      permitted <- currentPolicy(r)
      _ <- if !valid || !permitted || text(e,"processor_id")!=text(r,"processor_id") ||
        text(e,"processor_version")!=text(r,"processor_version") then conflict("stale_report") else ().pure[ConnectionIO]
      _ <- if !Set("started","progress","output_ready","attempt_failed").contains(kind) then conflict("unknown_report") else ().pure[ConnectionIO]
      _ <- if Set("output_ready","attempt_failed").contains(kind) && text(r,"status")!="running" then conflict("report_after_output") else ().pure[ConnectionIO]
      _ <- if kind=="output_ready" then
        val refs = field(e,"artifacts")
        validateArtifacts(refs,settings,runId,Some(attemptId),false)
        if refs.asArray.forall(_.size!=1) || optional(e,"source_revision_digest")!=Some(text(r,"source_revision_digest")) then
          throw DqError(400,"invalid_output")
        artifacts(runId,refs) *> sql"""UPDATE octopus_core.dq_runs SET status='applying',output_refs=${canonical(refs)}::JSONB,
          state_revision=state_revision+1 WHERE run_id=$runId AND status='running'""".update.run.void *>
          sql"UPDATE octopus_core.dq_attempts SET status='applying',report_sequence=$sequence WHERE attempt_id=$attemptId".update.run.void
        else if kind=="attempt_failed" then
          val permanent = optional(e,"error_class").contains("permanent")
          sql"""UPDATE octopus_core.dq_runs SET status=CASE WHEN $permanent OR attempt_count>=5 THEN 'failed' ELSE 'retrying' END,
            error_code='worker_failure',current_attempt_id=NULL,fence=fence+1,state_revision=state_revision+1,
            completed_at=CASE WHEN $permanent OR attempt_count>=5 THEN CURRENT_TIMESTAMP ELSE NULL END,
            next_attempt_at=CURRENT_TIMESTAMP+(random()*LEAST(60,POWER(2,attempt_count-1)))*INTERVAL '1 second'
            WHERE run_id=$runId AND status='running'""".update.run.void *>
            sql"""UPDATE octopus_core.dq_attempts SET status='failed',report_sequence=$sequence,finished_at=CURRENT_TIMESTAMP
              WHERE attempt_id=$attemptId""".update.run.void *> terminalOrRetry(runId)
        else sql"""UPDATE octopus_core.dq_attempts SET status=CASE WHEN status='applying' THEN status ELSE 'running' END,
          started_at=COALESCE(started_at,CURRENT_TIMESTAMP),report_sequence=$sequence WHERE attempt_id=$attemptId""".update.run.void *>
          event(runId,kind)
    yield true
