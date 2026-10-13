package com.sslproxy.coordinator.dq

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.postgres.sql.{DqProjectionSql, IdentityGraphSql}
import doobie.Transactor
import doobie.implicits.*
import io.circe.Json
import io.circe.parser.parse
import munit.CatsEffectSuite
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.{Wait, WaitAllStrategy}
import org.testcontainers.utility.DockerImageName
import java.nio.file.{Files, Paths}
import java.sql.DriverManager
import java.util.UUID
import scala.util.Using

/** Mandatory canonical PostgreSQL tests: unavailable Docker fails setup. */
class DqStoreIntegrationSuite extends CatsEffectSuite:
  private class Database extends PostgreSQLContainer[Database](DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
  private val database = new Database().withDatabaseName("octopus_dq_test").withUsername("postgres").withPassword("test-password")
    .waitingFor(new WaitAllStrategy().withStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s",2))
      .withStrategy(Wait.forSuccessfulCommand("pg_isready -h 127.0.0.1 -p 5432 -U postgres -d octopus_dq_test")))
  override def beforeAll(): Unit =
    database.start()
    assert(database.getDatabaseName.startsWith("octopus_dq_test"))
    Using.resource(DriverManager.getConnection(database.getJdbcUrl,database.getUsername,database.getPassword)) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        statement.execute("CREATE EXTENSION IF NOT EXISTS vector"): Unit
        List("octopus_core", "atheros_search").foreach { schema =>
          val root = Paths.get("../../sql/postgres", schema).toAbsolutePath.normalize()
          val manifest = Files.readString(root.resolve("manifest.yaml"))
          val ordered = "(?m)^  - ([^\\n]+\\.sql)$".r.findAllMatchIn(manifest).map(_.group(1)).toList
          if schema == "octopus_core" then assert(ordered.contains("01_tables/018_dq_control.sql"))
          val checksums = Files.readString(root.resolve("checksums.sha256")).linesIterator.map(_.split("  ",2)).map(row => row(1)->row(0)).toMap
          ordered.foreach { relative =>
            val ddl = Files.readString(root.resolve(relative))
            assertEquals(DqJson.digest(ddl),checksums(relative),relative)
            statement.execute(ddl): Unit
          }
        }
        statement.execute("CREATE ROLE dq_octopus_test LOGIN PASSWORD 'synthetic-test-password'"): Unit
        statement.execute("CREATE ROLE dq_atheros_test LOGIN PASSWORD 'synthetic-test-password'"): Unit
        List("octopus_core","atheros_search").foreach { schema =>
          val grants = Files.readString(Paths.get("../../sql/postgres",schema,"grants/least_privilege.sql.tmpl"))
            .replace("{{OCTOPUS_ACCOUNT}}","dq_octopus_test").replace("{{ATHEROS_SEARCH_ACCOUNT}}","dq_atheros_test")
          statement.execute(grants): Unit
        }
      }
    }
  override def afterAll(): Unit = database.stop()
  // Match the runtime's PostgreSQL enum/JSON string binding configuration.
  private lazy val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver",database.getJdbcUrl + "&stringtype=unspecified",
    database.getUsername,database.getPassword,None)
  private lazy val runtimeXa = Transactor.fromDriverManager[IO]("org.postgresql.Driver",database.getJdbcUrl + "&stringtype=unspecified",
    "dq_octopus_test","synthetic-test-password",None)
  private val digest = "sha256:" + "a".repeat(64)
  private val actor = DqPrincipal("a".repeat(64),Set("admin","operator"),Set("north"))
  private def j(raw: String): Json = parse(raw).fold(throw _,identity)
  private def settings(namespace: String): DqSettings = DqSettings("dq-test",namespace,Map("north"->"shadow"),"p1",Set(digest))
  private def manifest(ns: String, version: String = "0.1.0"): Json =
    val schema = j("""{"type":"object","additionalProperties":false,"required":["authorized_scope","policy_version","max_candidates"],
      "properties":{"authorized_scope":{"type":"string"},"policy_version":{"type":"string"},"max_candidates":{"type":"integer","minimum":1,"maximum":1000},"candidate_id":{"type":"string"}}}""")
    val value = Json.obj("installation_id"->Json.fromString("dq-test"),"namespace"->Json.fromString(ns),
      "processor_id"->Json.fromString("atheros.dedup"),"version"->Json.fromString(version),"owner_team"->Json.fromString("wireless"),
      "implementation_digest"->Json.fromString(digest),"worker_service"->Json.fromString("dedup-worker:9090"),
      "protocol_version"->Json.fromString("dq.v1"),"parameter_schema"->schema,"parameter_schema_sha256"->Json.fromString(DqJson.sha256(schema)),
      "inputs"->Json.arr(),"outputs"->Json.arr(),"resources"->j("""{"concurrency":2,"timeout_seconds":60,"max_input_bytes":1000000,"max_output_bytes":1000000}"""),
      "policy_version"->Json.fromString("p1"))
    value.deepMerge(Json.obj("manifest_sha256"->Json.fromString(DqJson.sha256(value))))
  private def ready(ns: String): IO[DqStore] =
    val store = new DqStore(xa,settings(ns))
    store.reconcilePolicies *> store.register(actor,manifest(ns)).as(store)
  private def request(ns: String, key: String): Json = Json.obj(
    "installation_id"->Json.fromString("dq-test"),"namespace"->Json.fromString(ns),"processor_id"->Json.fromString("atheros.dedup"),
    "operation"->Json.fromString("prepare_candidates"),"idempotency_key"->Json.fromString(key),
    "parameters"->j("""{"authorized_scope":"north","policy_version":"p1","max_candidates":10}"""))
  private def reference(ns: String, runId: String, attempt: Option[String] = None): Json =
    val artifactId = UUID.randomUUID().toString
    val prefix = "s3://dq-artifacts/dq-test/" + ns + "/"
    val uri = attempt.fold(prefix+"inputs/"+runId+"/"+"c".repeat(64))(a => prefix+"runs/"+runId+"/attempts/"+a+"/"+artifactId)
    val ref = Json.obj("artifact_id"->Json.fromString(artifactId),"installation_id"->Json.fromString("dq-test"),
      "namespace"->Json.fromString(ns),"uri"->Json.fromString(uri),"version_id"->Json.fromString("version-1"),
      "etag"->Json.fromString("etag"),"sha256"->Json.fromString("c".repeat(64)),
      "schema_id"->Json.fromString(if attempt.isDefined then "atheros.dedup.proposal.v1" else "atheros.dedup.snapshot.v1"),
      "media_type"->Json.fromString("application/json"),"size_bytes"->Json.fromLong(100))
    attempt.fold(ref)(a => ref.deepMerge(Json.obj("producer_run_id"->Json.fromString(runId),"producer_attempt_id"->Json.fromString(a))))
  private def prepared(store: DqStore, key: String): IO[Json] = for
    run <- store.admit(actor,request(store.settings.namespace,key))
    runId = DqJson.text(run,"run_id")
    claimed <- store.claimPreparation("preparer",60,100)
    _ = assert(claimed.exists(_.hcursor.get[String]("run_id").contains(runId)))
    attached <- store.completePreparation(runId,"preparer",Json.arr(reference(store.settings.namespace,runId)),"b".repeat(64))
    _ = assert(attached)
    result <- store.getRun(actor,runId)
  yield result
  private def heartbeat(ns: String, worker: String, address: String = "dedup-worker:9090"): Json = Json.obj(
    "installation_id"->Json.fromString("dq-test"),"namespace"->Json.fromString(ns),"processor_id"->Json.fromString("atheros.dedup"),
    "processor_version"->Json.fromString("0.1.0"),"implementation_digest"->Json.fromString(digest),"worker_instance_id"->Json.fromString(worker),
    "address"->Json.fromString(address),"concurrency"->Json.fromInt(2),"ready"->Json.True)
  private def reservation(ns: String, worker: String, slot: Int = 0): Json = Json.obj("installation_id"->Json.fromString("dq-test"),
    "namespace"->Json.fromString(ns),"worker_instance_id"->Json.fromString(worker),"worker_slot"->Json.fromInt(slot))
  private def leaseBody(ns: String, attempt: Json): Json = attempt.mapObject(_.filter { case (k,_) =>
    Set("run_id","worker_instance_id","lease_token","fence").contains(k)
  }).deepMerge(Json.obj("installation_id"->Json.fromString("dq-test"),"namespace"->Json.fromString(ns)))
  private def receipt(ns: String, envelope: Json, offset: Long, payloadHash: String = "d".repeat(64)): Json = Json.obj(
    "installation_id"->Json.fromString("dq-test"),"namespace"->Json.fromString(ns),"consumer_group"->Json.fromString("octopus-dq"),
    "topic"->Json.fromString("dq.worker-reports.v1"),"partition"->Json.fromInt(0),"offset"->Json.fromLong(offset),
    "event_id"->DqJson.field(envelope,"event_id"),"payload_sha256"->Json.fromString(payloadHash),"envelope"->envelope)
  private def report(ns: String, run: Json, attempt: Json, eventType: String, refs: Json = Json.arr(), revision: String = "b".repeat(64)): Json =
    Json.obj("schema_version"->Json.fromInt(1),"event_id"->Json.fromString(UUID.randomUUID().toString),"event_type"->Json.fromString(eventType),
      "installation_id"->Json.fromString("dq-test"),"namespace"->Json.fromString(ns),"run_id"->DqJson.field(run,"run_id"),
      "processor_id"->DqJson.field(run,"processor_id"),"processor_version"->DqJson.field(run,"processor_version"),
      "attempt_id"->DqJson.field(attempt,"attempt_id"),"fence"->DqJson.field(attempt,"fence"),"sequence"->Json.fromLong(1),
      "occurred_at"->Json.fromString(java.time.Instant.now().toString),"correlation_id"->DqJson.field(run,"run_id"),
      "source_revision_digest"->Json.fromString(revision),"artifacts"->refs)

  test("atomic duplicate admission pins default; content conflict wins before later default resolution") {
    for
      store <- ready("admission")
      catalog <- store.catalog(DqPrincipal.internal)
      _ = assertEquals(DqJson.field(catalog,"items").asArray.map(_.size),Some(1))
      result <- (store.admit(actor,request("admission","duplicate")),store.admit(actor,request("admission","duplicate"))).parTupled
      _ = assertEquals(DqJson.text(result._1,"run_id"),DqJson.text(result._2,"run_id"))
      _ <- store.register(actor,manifest("admission","0.2.0"))
      _ <- sql"UPDATE octopus_core.dq_processors SET default_version='0.2.0' WHERE namespace='admission'".update.run.transact(xa)
      replayed <- store.admit(actor,request("admission","duplicate"))
      _ = assertEquals(DqJson.text(replayed,"processor_version"),"0.1.0")
      changed = request("admission","duplicate").deepMerge(Json.obj("parameters"->j("""{"max_candidates":11}""")))
      conflict <- store.admit(actor,changed).attempt
      _ = assertEquals(conflict.left.toOption.collect { case DqError(_,code) => code },Some("idempotency_conflict"))
      audits <- sql"SELECT COUNT(*) FROM octopus_core.dq_audit WHERE namespace='admission' AND action='invoke'".query[Long].unique.transact(xa)
      _ = assertEquals(audits,1L)
      exponent = request("admission","numeric").deepMerge(Json.obj("parameters" -> j("""{"max_candidates":1e1}""")))
      numeric <- store.admit(actor,exponent)
      sameNumeric <- store.admit(actor,request("admission","numeric"))
      _ = assertEquals(DqJson.text(numeric,"run_id"),DqJson.text(sameNumeric,"run_id"))
      _ = assertEquals(DqJson.text(numeric,"parameters_sha256"),DqJson.sha256(DqJson.field(numeric,"parameters")))
    yield ()
  }
  test("two managers reserve one attempt; lost acknowledgement retains boot worker and immutable endpoint") {
    for
      store <- ready("reservation")
      run <- prepared(store,"one")
      _ <- store.heartbeat(heartbeat("reservation","boot-one"))
      _ <- store.heartbeat(heartbeat("reservation","boot-two"))
      pair <- (store.reserve(DqJson.text(run,"run_id"),reservation("reservation","boot-one")),
        store.reserve(DqJson.text(run,"run_id"),reservation("reservation","boot-two"))).parTupled
      _ = assertEquals(DqJson.field(pair._1,"attempt"),DqJson.field(pair._2,"attempt"))
      a = DqJson.field(pair._1,"attempt")
      first <- store.accepted(DqJson.text(a,"attempt_id"),leaseBody("reservation",a))
      again <- store.accepted(DqJson.text(a,"attempt_id"),leaseBody("reservation",a))
      _ = assertEquals(DqJson.text(first,"attempt_id"),DqJson.text(again,"attempt_id"))
      changed <- store.heartbeat(heartbeat("reservation","boot-one","new-host:9090")).attempt
      _ = assert(changed.isLeft)
      foreignLease = leaseBody("reservation",a).deepMerge(Json.obj("worker_instance_id"->Json.fromString("other-boot")))
      forbidden <- store.renew(DqJson.text(a,"attempt_id"),foreignLease).attempt
      _ = assert(forbidden.isLeft)
    yield ()
  }

  test("admission reserves a fair quota for every authorized scope and releases capacity after cancellation") {
    val namespace = "scope-quota"
    val scopedActor = DqPrincipal("a".repeat(64),Set("admin","operator"),Set("north","south"))
    val store = new DqStore(xa,settings(namespace).copy(scopeModes=Map("north"->"shadow","south"->"shadow"),maxQueued=4))
    def south(key: String): Json = request(namespace,key).deepMerge(Json.obj("parameters" -> Json.obj("authorized_scope" -> Json.fromString("south"))))
    for
      _ <- store.reconcilePolicies
      _ <- store.register(scopedActor,manifest(namespace))
      first <- store.admit(scopedActor,request(namespace,"north-1"))
      _ <- store.admit(scopedActor,request(namespace,"north-2"))
      full <- store.admit(scopedActor,request(namespace,"north-3")).attempt
      _ = assertEquals(full.left.toOption.collect { case DqError(status,code) => (status,code) },Some((429,"admission_capacity")))
      duplicate <- store.admit(scopedActor,request(namespace,"north-1"))
      _ = assertEquals(DqJson.text(duplicate,"run_id"),DqJson.text(first,"run_id"))
      _ <- store.admit(scopedActor,south("south-1"))
      _ <- store.admit(scopedActor,south("south-2"))
      _ <- store.cancel(scopedActor,DqJson.text(first,"run_id"))
      _ <- store.admit(scopedActor,request(namespace,"north-3"))
      count <- sql"SELECT COUNT(*) FROM octopus_core.dq_runs WHERE namespace=$namespace AND status='queued'".query[Long].unique.transact(xa)
      _ = assertEquals(count,4L)
    yield ()
  }
  test("two concurrent runs cannot occupy one worker slot") {
    for
      store <- ready("slot")
      one <- prepared(store,"one")
      two <- prepared(store,"two")
      _ <- store.heartbeat(heartbeat("slot","slot-boot"))
      result <- (store.reserve(DqJson.text(one,"run_id"),reservation("slot","slot-boot")).attempt,
        store.reserve(DqJson.text(two,"run_id"),reservation("slot","slot-boot")).attempt).parTupled
      _ = assertEquals(List(result._1,result._2).count(_.isRight),1)
    yield ()
  }
  test("expired execution leases fence stale renewal and reports; receipt quarantine survives redelivery") {
    for
      store <- ready("expired")
      run <- prepared(store,"one")
      _ <- store.heartbeat(heartbeat("expired","expired-boot"))
      reserved <- store.reserve(DqJson.text(run,"run_id"),reservation("expired","expired-boot"))
      a = DqJson.field(reserved,"attempt")
      _ <- sql"UPDATE octopus_core.dq_attempts SET lease_expires_at=CURRENT_TIMESTAMP-INTERVAL '1 second' WHERE attempt_id=${DqJson.text(a,"attempt_id")}".update.run.transact(xa)
      expired <- store.expireAttempts
      _ = assertEquals(expired,1)
      renewal <- store.renew(DqJson.text(a,"attempt_id"),leaseBody("expired",a)).attempt
      _ = assert(renewal.isLeft)
      e = report("expired",run,a,"started")
      parked <- store.receiveReceipt(receipt("expired",e,101))
      replayed <- store.receiveReceipt(receipt("expired",e,101))
      _ = assert(parked.hcursor.get[Boolean]("quarantined").contains(true))
      _ = assert(replayed.hcursor.get[Boolean]("quarantined").contains(true))
      durable <- sql"SELECT quarantine_reason FROM octopus_core.dq_receipts WHERE topic='dq.worker-reports.v1' AND offset_id=101".query[String].unique.transact(xa)
      _ = assertEquals(durable,"invalid_or_stale_report")
    yield ()
  }
  test("conflicting event hash is durable quarantine; malformed envelopes park before offset commit") {
    for
      store <- ready("receipts")
      malformed = Json.obj("event_id"->Json.fromString("receipt-malformed"))
      first <- store.receiveReceipt(receipt("receipts",malformed,102))
      second <- store.receiveReceipt(receipt("receipts",malformed,103,"e".repeat(64)))
      _ = assert(first.hcursor.get[Boolean]("quarantined").contains(true))
      _ = assert(second.hcursor.get[Boolean]("quarantined").contains(true))
      count <- sql"SELECT COUNT(*) FROM octopus_core.dq_receipts WHERE offset_id IN (102,103) AND quarantine_reason IS NOT NULL".query[Long].unique.transact(xa)
      _ = assertEquals(count,2L)
    yield ()
  }
  test("source mismatch rejects output; valid output survives bounded application retry and expired worker lease") {
    for
      store <- ready("application")
      run <- prepared(store,"one")
      _ <- store.heartbeat(heartbeat("application","apply-boot"))
      reserved <- store.reserve(DqJson.text(run,"run_id"),reservation("application","apply-boot"))
      a = DqJson.field(reserved,"attempt")
      runId = DqJson.text(run,"run_id")
      attemptId = DqJson.text(a,"attempt_id")
      refs = Json.arr(reference("application",runId,Some(attemptId)))
      stale <- store.receiveReceipt(receipt("application",report("application",run,a,"output_ready",refs,"f".repeat(64)),104))
      _ = assert(stale.hcursor.get[Boolean]("quarantined").contains(true))
      accepted <- store.receiveReceipt(receipt("application",report("application",run,a,"output_ready",refs),105))
      _ = assert(accepted.hcursor.get[Boolean]("applied").contains(true))
      _ <- store.claimOutput("applier",60,10)
      _ <- store.failOutput(runId,"applier","storage_unavailable",false,false)
      _ <- sql"UPDATE octopus_core.dq_runs SET next_attempt_at=CURRENT_TIMESTAMP WHERE run_id=$runId".update.run.transact(xa)
      _ <- sql"UPDATE octopus_core.dq_attempts SET lease_expires_at=CURRENT_TIMESTAMP-INTERVAL '1 second' WHERE attempt_id=$attemptId".update.run.transact(xa)
      claimed <- store.claimOutput("retry-applier",60,10)
      _ = assert(claimed.exists(_.hcursor.get[String]("run_id").contains(runId)))
      completed <- store.completeOutputTx(runId,attemptId,DqJson.long(a,"fence"),"retry-applier","b".repeat(64),false,Json.obj("comparison"->Json.True)).transact(xa)
      duplicate <- store.completeOutputTx(runId,attemptId,DqJson.long(a,"fence"),"retry-applier","b".repeat(64),false,Json.obj()).transact(xa)
      _ = assert(completed)
      _ = assert(!duplicate)
      result <- store.getRun(actor,runId)
      _ = assertEquals(DqJson.text(result,"status"),"succeeded")
      _ = assert(result.hcursor.get[Boolean]("projection_applied").contains(false))
    yield ()
  }
  test("scope authorization and rollback fence outstanding work before legacy resumes") {
    for
      store <- ready("rollback")
      disabled <- store.admit(actor,request("rollback","wrong-scope").deepMerge(Json.obj("parameters"->j("""{"authorized_scope":"south"}""")))).attempt
      _ = assert(disabled.isLeft)
      run <- prepared(store,"one")
      _ <- store.heartbeat(heartbeat("rollback","rollback-boot"))
      reserved <- store.reserve(DqJson.text(run,"run_id"),reservation("rollback","rollback-boot"))
      a = DqJson.field(reserved,"attempt")
      rolledBack = new DqStore(xa,settings("rollback").copy(scopeModes=Map.empty))
      _ <- rolledBack.reconcilePolicies
      lease <- store.renew(DqJson.text(a,"attempt_id"),leaseBody("rollback",a)).attempt
      _ = assert(lease.isLeft)
      result <- store.getRun(actor,DqJson.text(run,"run_id"))
      _ = assertEquals(DqJson.text(result,"status"),"cancelled")
      noScope = actor.copy(scopes=Set("south"))
      denied <- store.getRun(noScope,DqJson.text(run,"run_id")).attempt
      _ = assert(denied.isLeft)
      artifactId = DqJson.text(DqJson.field(run,"input_refs").asArray.get.head,"artifact_id")
      artifactDenied <- store.artifact(noScope,artifactId,false).attempt
      _ = assert(artifactDenied.isLeft)
    yield ()
  }

  test("dispatch respects the requested credit window when more than 32 runs are ready") {
    for
      store <- ready("dispatch-limit")
      _ <- (1 to 35).toList.traverse_(number => prepared(store,"backlog-" + number.toString))
      dispatch <- store.dispatches(32)
      small <- store.dispatches(4)
      invalid <- store.dispatches(101).attempt
      _ = assertEquals(DqJson.field(dispatch,"items").asArray.map(_.size),Some(32))
      _ = assertEquals(DqJson.field(small,"items").asArray.map(_.size),Some(4))
      _ = assert(invalid.isLeft)
    yield ()
  }

  test("preparation dependency retries are fenced and terminate after five attempts") {
    for
      store <- ready("preparation-retry")
      run <- store.admit(actor,request("preparation-retry","retry"))
      runId = DqJson.text(run,"run_id")
      _ <- (1 to 5).toList.traverse_ { number =>
        val owner = "preparer-" + number.toString
        for
          _ <- sql"UPDATE octopus_core.dq_runs SET next_attempt_at=CURRENT_TIMESTAMP WHERE run_id=$runId".update.run.transact(xa)
          claimed <- store.claimPreparation(owner,60,1)
          _ = assertEquals(claimed.size,1)
          stale <- store.completePreparation(runId,"wrong-owner",Json.arr(reference("preparation-retry",runId)),"b".repeat(64))
          _ = assert(!stale)
          _ <- store.failPreparation(runId,owner,"storage_unavailable",false)
        yield ()
      }
      result <- store.getRun(actor,runId)
      noMore <- store.claimPreparation("sixth",60,1)
      _ = assertEquals(DqJson.text(result,"status"),"failed")
      _ = assertEquals(noMore.size,0)
    yield ()
  }

  private def seedPair(location: String, left: String, right: String): IO[Unit] =
    val first = UUID.randomUUID().toString
    val second = UUID.randomUUID().toString
    val pair = UUID.randomUUID().toString
    val projection = UUID.randomUUID().toString
    (for
      _ <- List(left,right).traverse_ { mac =>
        sql"""INSERT INTO atheros_search.devices(mac,location_id,first_seen,last_seen)
          VALUES ($mac,$location,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""".update.run.void
      }
      _ <- List((first,left),(second,right)).traverse_ { case (document,mac) =>
        sql"""INSERT INTO atheros_search.search_documents(document_id,source_kind,source_id,source_key,source_table,source_mac,location_id,
          normalized_text,normalized_sha256,search_vector) VALUES ($document::uuid,'device',$mac,$mac,'inventory_devices',$mac,$location,
          'fixture',${"a".repeat(64)},to_tsvector('simple','fixture'))""".update.run.void
      }
      _ <- sql"""INSERT INTO atheros_search.similarity_pairs(pair_id,pair_kind,embedding_model,embedding_kind,
        left_document_id,right_document_id,left_source_table,left_source_key,left_source_mac,
        right_source_table,right_source_key,right_source_mac,cosine_distance,cosine_similarity,evidence,computed_at,projection_run_id)
        VALUES ($pair::uuid,'device_device','fixture','device',$first::uuid,$second::uuid,'devices',$left,$left,
          'devices',$right,$right,0.05,0.95,'{}'::jsonb,CURRENT_TIMESTAMP,$projection::uuid)""".update.run.void
    yield ()).transact(xa)

  test("automation discovers and applies human confirmation to canonical identity clusters with runtime grants and JDBC bindings") {
    val namespace = "automation-decision"
    val scopedActor = DqPrincipal("a".repeat(64),Set("admin","operator"),Set("north","south"))
    val automation = DqPrincipal("octopus-dq-discovery",Set("automation"),Set("north","south"))
    val store = new DqStore(runtimeXa,settings(namespace).copy(scopeModes=Map("north"->"shadow","south"->"enabled")))
    val left = "02:00:00:00:02:01"
    val right = "02:00:00:00:02:02"
    for
      _ <- store.reconcilePolicies
      _ <- store.register(scopedActor,manifest(namespace))
      shadow <- store.admit(automation,request(namespace,"repeated-shadow"))
      sameShadow <- store.admit(automation,request(namespace,"repeated-shadow"))
      _ = assertEquals(DqJson.text(shadow,"run_id"),DqJson.text(sameShadow,"run_id"))
      _ <- seedPair("south",left,right)
      _ <- IdentityGraphSql.persistMergeCandidate(left,right,0.90).transact(xa)
      candidate <- sql"SELECT candidate_id FROM atheros_search.merge_candidates WHERE mac_a=$left AND mac_b=$right".query[String].unique.transact(xa)
      _ <- (sql"INSERT INTO atheros_search.merge_decisions(candidate_id,decision,decided_by) VALUES ($candidate,'merge','synthetic-reviewer')".update.run *>
        sql"UPDATE atheros_search.merge_candidates SET status='approved',updated_at=CURRENT_TIMESTAMP WHERE candidate_id=$candidate".update.run).transact(xa)
      discovered <- DqProjectionSql.decisions("dq-test",namespace,10).to[List].transact(runtimeXa)
      _ = assertEquals(discovered,List((candidate,"merge","south")))
      admission = request(namespace,"decision-reconciliation").deepMerge(Json.obj("processor_version" -> Json.fromString("0.1.0"),
        "operation" -> Json.fromString("reconcile_decision"),"parameters" -> Json.obj("authorized_scope" -> Json.fromString("south"),
          "max_candidates" -> Json.fromInt(1000),"candidate_id" -> Json.fromString(candidate))))
      run <- store.admit(automation,admission)
      ledger <- sql"SELECT run_id FROM octopus_core.dq_decision_sources WHERE candidate_id=$candidate".query[String].unique.transact(runtimeXa)
      _ = assertEquals(ledger,DqJson.text(run,"run_id"))
      remaining <- DqProjectionSql.decisions("dq-test",namespace,10).to[List].transact(runtimeXa)
      _ = assertEquals(remaining,Nil)
      snapshot <- DqSnapshot.prepare(run,0.9).transact(runtimeXa)
      _ = assertEquals(DqJson.field(snapshot,"decisions").asArray.map(_.size),Some(1))
      legacy <- sql"SELECT COUNT(*) FROM octopus_core.devices WHERE mac_id IN ($left,$right)".query[Long].unique.transact(xa)
      _ = assertEquals(legacy,0L)
      _ <- store.cancel(scopedActor,DqJson.text(shadow,"run_id"))
      claimed <- store.claimPreparation("canonical-preparer",60,1)
      runId = DqJson.text(run,"run_id")
      _ = assertEquals(claimed.map(DqJson.text(_,"run_id")),List(runId))
      _ <- store.completePreparation(runId,"canonical-preparer",Json.arr(reference(namespace,runId)),DqSnapshot.revision(snapshot))
      _ <- store.heartbeat(heartbeat(namespace,"canonical-boot"))
      reserved <- store.reserve(runId,reservation(namespace,"canonical-boot"))
      attempt = DqJson.field(reserved,"attempt")
      _ <- store.accepted(DqJson.text(attempt,"attempt_id"),leaseBody(namespace,attempt))
      refs = Json.arr(reference(namespace,runId,Some(DqJson.text(attempt,"attempt_id"))))
      _ <- store.receiveReceipt(receipt(namespace,report(namespace,run,attempt,"output_ready",refs,DqSnapshot.revision(snapshot)),501))
      outputRuns <- store.claimOutput("canonical-applier",60,1)
      applying = outputRuns.head
      proposal = Json.obj("run_id" -> Json.fromString(runId),"attempt_id" -> DqJson.field(attempt,"attempt_id"),
        "fence" -> DqJson.field(attempt,"fence"),"operation" -> Json.fromString("reconcile_decision"),
        "source_revision_digest" -> Json.fromString(DqSnapshot.revision(snapshot)),"policy_version" -> Json.fromString("p1"),
        "candidates" -> Json.arr(),"reconciliations" -> Json.arr(Json.obj("candidate_id" -> Json.fromString(candidate),
          "decision" -> Json.fromString("merge"))),"summary" -> j("""{"candidate_count":0,"reconciliation_count":1}"""))
      applied <- DqApply.transaction(store,"canonical-applier",applying,snapshot,proposal,0.9).transact(runtimeXa)
      _ = assert(applied)
      result <- store.getRun(scopedActor,runId)
      _ = assert(result.hcursor.get[Boolean]("projection_applied").contains(true))
      candidateStatus <- sql"SELECT status FROM atheros_search.merge_candidates WHERE candidate_id=$candidate".query[String].unique.transact(runtimeXa)
      _ = assertEquals(candidateStatus,"confirmed")
      cluster <- sql"""SELECT cluster.cluster_id,cluster.cluster_size FROM atheros_search.identity_clusters cluster
        JOIN atheros_search.identity_cluster_members member USING(cluster_id) WHERE member.mac=$left AND cluster.status='active'"""
        .query[(String,Int)].unique.transact(runtimeXa)
      _ = assertEquals(cluster._2,2)
      members <- sql"SELECT mac FROM atheros_search.identity_cluster_members WHERE cluster_id=${cluster._1} ORDER BY mac".query[String].to[List].transact(runtimeXa)
      _ = assertEquals(members,List(left,right))
    yield ()
  }

  test("source snapshots notice same-timestamp edits and human decisions before apply") {
    val left = "02:00:00:00:01:01"
    val right = "02:00:00:00:01:02"
    for
      store <- ready("source-fence")
      _ <- seedPair("north",left,right)
      run <- store.admit(actor,request("source-fence","snapshot"))
      before <- DqSnapshot.prepare(run,0.9).transact(xa)
      _ <- sql"UPDATE atheros_search.devices SET display_name='changed' WHERE mac=$left".update.run.transact(xa)
      after <- DqSnapshot.prepare(run,0.9).transact(xa)
      _ = assert(DqSnapshot.revision(before)!=DqSnapshot.revision(after))
      _ <- IdentityGraphSql.persistMergeCandidate(left,right,0.90).transact(xa)
      candidate <- sql"SELECT candidate_id FROM atheros_search.merge_candidates WHERE mac_a=$left AND mac_b=$right".query[String].unique.transact(xa)
      snapshot <- DqSnapshot.prepare(run,0.9).transact(xa)
      _ <- sql"""INSERT INTO atheros_search.merge_decisions(candidate_id,decision,decided_at,decided_by)
        VALUES ($candidate,'not_match',CURRENT_TIMESTAMP,'test-operator')""".update.run.transact(xa)
      current <- DqSnapshot.prepare(run,0.9).transact(xa)
      _ = assert(DqSnapshot.revision(snapshot)!=DqSnapshot.revision(current))
      _ = assertEquals(DqJson.field(current,"pairs").asArray.map(_.size),Some(0))
      reconciliationRequest = request("source-fence","decision").deepMerge(Json.obj(
        "operation" -> Json.fromString("reconcile_decision"), "parameters" -> Json.obj("candidate_id" -> Json.fromString(candidate))))
      reconciliation <- store.admit(actor,reconciliationRequest)
      decisionSnapshot <- DqSnapshot.prepare(reconciliation,0.9).transact(xa)
      _ = assertEquals(DqJson.field(decisionSnapshot,"decisions").asArray.map(_.size),Some(1))
      ledger <- sql"SELECT run_id FROM octopus_core.dq_decision_sources WHERE candidate_id=$candidate".query[String].unique.transact(xa)
      _ = assertEquals(ledger,DqJson.text(reconciliation,"run_id"))
      excluded <- DqProjectionSql.decisions("dq-test","source-fence",10).to[List].transact(xa)
      _ = assert(!excluded.exists(_._1==candidate))
      _ <- store.cancel(actor,DqJson.text(reconciliation,"run_id"))
      _ <- sql"DELETE FROM octopus_core.dq_runs WHERE run_id=${DqJson.text(reconciliation,"run_id")}".update.run.transact(xa)
      stillExcluded <- DqProjectionSql.decisions("dq-test","source-fence",10).to[List].transact(xa)
      _ = assert(!stillExcluded.exists(_._1==candidate))
      enabled = new DqStore(xa,settings("source-fence").copy(scopeModes=Map("north" -> "enabled")))
      _ <- enabled.reconcilePolicies
      promoted <- DqProjectionSql.decisions("dq-test","source-fence",10).to[List].transact(xa)
      _ = assert(promoted.exists(_._1==candidate))
      _ <- seedPair("north","02:00:00:00:01:03","02:00:00:00:01:04")
      tailRun = run.deepMerge(Json.obj("parameters" -> Json.obj("max_candidates" -> Json.fromInt(1))))
      tail <- DqSnapshot.prepare(tailRun,0.9).transact(xa)
      _ = assertEquals(DqJson.field(tail,"pairs").asArray.map(_.size),Some(1))
      _ = assertEquals(DqJson.text(DqJson.field(tail,"pairs").asArray.get.head,"mac_a"),"02:00:00:00:01:03")
    yield ()
  }
