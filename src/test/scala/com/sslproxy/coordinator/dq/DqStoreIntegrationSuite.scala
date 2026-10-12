package com.sslproxy.coordinator.dq

import cats.effect.IO
import cats.syntax.all.*
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
  private class Database extends PostgreSQLContainer[Database](DockerImageName.parse("postgres:17"))
  private val database = new Database().withDatabaseName("octopus_dq_test").withUsername("postgres").withPassword("test-password")
    .waitingFor(new WaitAllStrategy().withStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s",2))
      .withStrategy(Wait.forSuccessfulCommand("pg_isready -h 127.0.0.1 -p 5432 -U postgres -d octopus_dq_test")))
  override def beforeAll(): Unit =
    database.start()
    assert(database.getDatabaseName.startsWith("octopus_dq_test"))
    val root = Paths.get("../../sql/postgres/octopus_core").toAbsolutePath.normalize()
    val manifest = Files.readString(root.resolve("manifest.yaml"))
    val ordered = "(?m)^  - ([^\\n]+\\.sql)$".r.findAllMatchIn(manifest).map(_.group(1)).toList
    assert(ordered.contains("01_tables/018_dq_control.sql"))
    val checksums = Files.readString(root.resolve("checksums.sha256")).linesIterator.map(_.split("  ",2)).map(row => row(1)->row(0)).toMap
    Using.resource(DriverManager.getConnection(database.getJdbcUrl,database.getUsername,database.getPassword)) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        ordered.foreach { relative =>
          val ddl = Files.readString(root.resolve(relative))
          assertEquals(DqJson.digest(ddl),checksums(relative),relative)
          statement.execute(ddl): Unit
        }
      }
    }
  override def afterAll(): Unit = database.stop()
  private lazy val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver",database.getJdbcUrl,
    database.getUsername,database.getPassword,None)
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
