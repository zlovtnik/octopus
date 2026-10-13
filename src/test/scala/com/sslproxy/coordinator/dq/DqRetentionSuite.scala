package com.sslproxy.coordinator.dq

import cats.effect.{IO, Resource}
import cats.effect.std.Semaphore
import cats.syntax.all.*
import com.sslproxy.coordinator.config.ArchiveConfig
import com.sslproxy.coordinator.postgres.sql.{DqMetricsSql, DqRetentionSql, IdentityGraphSql}
import doobie.Transactor
import doobie.implicits.*
import io.circe.Json
import munit.CatsEffectSuite
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.{Wait, WaitAllStrategy}
import org.testcontainers.utility.DockerImageName

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.sql.DriverManager
import scala.util.Using

class DqRetentionSuite extends CatsEffectSuite:
  private class Database extends PostgreSQLContainer[Database](DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"))
  private val database = new Database().withDatabaseName("octopus_dq_retention_test")
    .withUsername("postgres").withPassword("test-password")
    .waitingFor(new WaitAllStrategy().withStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s",2))
      .withStrategy(Wait.forSuccessfulCommand("pg_isready -h 127.0.0.1 -U postgres -d octopus_dq_retention_test")))
  override def beforeAll(): Unit =
    database.start()
    assert(database.getDatabaseName.startsWith("octopus_dq_retention_test"))
    Using.resource(DriverManager.getConnection(database.getJdbcUrl,database.getUsername,database.getPassword)) { connection =>
      Using.resource(connection.createStatement()) { statement =>
        statement.execute("CREATE EXTENSION IF NOT EXISTS vector"): Unit
        List("octopus_core","atheros_search").foreach { schema =>
          val root = Paths.get("../../sql/postgres",schema).toAbsolutePath.normalize()
          val ordered = "(?m)^  - ([^\\n]+\\.sql)$".r.findAllMatchIn(Files.readString(root.resolve("manifest.yaml"))).map(_.group(1)).toList
          ordered.foreach(relative => statement.execute(Files.readString(root.resolve(relative))): Unit)
        }
      }
    }
  override def afterAll(): Unit = database.stop()
  private lazy val xa = Transactor.fromDriverManager[IO]("org.postgresql.Driver",database.getJdbcUrl,
    database.getUsername,database.getPassword,None)

  private def emptyObjectStore: Resource[IO,String] =
    Resource.make(IO.blocking {
      val server = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
      server.createContext("/", exchange =>
        val response = """<?xml version="1.0" encoding="UTF-8"?>
          |<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
          |  <Name>dq-artifacts</Name><Prefix></Prefix><KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker>
          |  <MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
          |</ListVersionsResult>""".stripMargin.getBytes(StandardCharsets.UTF_8)
        exchange.getResponseHeaders.set("Content-Type","application/xml")
        exchange.sendResponseHeaders(200,response.length.toLong)
        val body = exchange.getResponseBody
        try body.write(response)
        finally body.close()
      )
      server.start()
      server
    })(server => IO.blocking(server.stop(0))).map(server => s"http://127.0.0.1:${server.getAddress.getPort}")

  private def seed(id: String, namespace: String, status: String, summary: Json, applied: Boolean = false): IO[Unit] =
    (sql"""INSERT INTO octopus_core.dq_runs(run_id,installation_id,namespace,processor_id,processor_version,
      authorized_scope,operation,idempotency_key,request_fingerprint,document,manifest,policy_version,execution_mode,
      status,deadline_at,completed_at,projection_applied,summary)
      VALUES($id,'retention-test',$namespace,'atheros.dedup','0.1.0','north','prepare_candidates',$id,'hash','{}','{}','p1','enabled',
        $status,CURRENT_TIMESTAMP-INTERVAL '100 days',CURRENT_TIMESTAMP-INTERVAL '100 days',$applied,${DqJson.canonical(summary)}::JSONB)""".update.run *>
      sql"""INSERT INTO octopus_core.dq_artifacts(artifact_id,run_id,installation_id,namespace,reference)
        VALUES($id,$id,'retention-test',$namespace,jsonb_build_object('artifact_id',$id,'version_id','exact-v1',
          'uri','s3://dq-artifacts/retention-test/'||$namespace||'/inputs/'||$id||'/sha'))""".update.run).transact(xa).void

  test("pending reviews retain evidence; closure starts a fresh ninety-day artifact window") {
    val namespace = "open-review"
    for
      _ <- IdentityGraphSql.persistMergeCandidate("02:00:00:00:11:01","02:00:00:00:11:02",0.90).transact(xa)
      candidate <- sql"SELECT candidate_id FROM atheros_search.merge_candidates WHERE mac_a='02:00:00:00:11:01'".query[String].unique.transact(xa)
      _ <- seed("open-run",namespace,"succeeded",Json.obj("review_candidate_ids"->Json.arr(Json.fromString(candidate))),true)
      closed <- DqRetentionSql.closeArtifacts("retention-test",namespace,100).transact(xa)
      _ = assertEquals(closed,0)
      _ <- sql"""UPDATE atheros_search.merge_candidates SET status='confirmed',confirmation_source='human',
        confirmed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE candidate_id=$candidate""".update.run.transact(xa)
      changed <- DqRetentionSql.closeArtifacts("retention-test",namespace,100).transact(xa)
      _ = assertEquals(changed,1)
      expired <- DqRetentionSql.expiredArtifacts("retention-test",namespace,100).transact(xa)
      _ = assertEquals(expired,Nil)
      future <- sql"SELECT retained_until>CURRENT_TIMESTAMP+INTERVAL '89 days' FROM octopus_core.dq_artifacts WHERE artifact_id='open-run'".query[Boolean].unique.transact(xa)
      _ = assert(future)
      removed <- DqRetentionSql.pruneRuns("retention-test",namespace,100).transact(xa)
      _ = assertEquals(removed,0)
    yield ()
  }

  test("only expired exact versions are forgotten; metadata waits for artifact deletion and pending outbox") {
    val namespace = "closed"
    for
      _ <- seed("closed-run",namespace,"failed",Json.obj())
      _ <- DqRetentionSql.closeArtifacts("retention-test",namespace,100).transact(xa)
      refs <- DqRetentionSql.expiredArtifacts("retention-test",namespace,100).transact(xa)
      _ = assertEquals(refs.size,1)
      before <- DqRetentionSql.pruneRuns("retention-test",namespace,100).transact(xa)
      _ = assertEquals(before,0)
      wrong <- DqRetentionSql.forgetArtifact("closed-run","other-version").transact(xa)
      _ = assertEquals(wrong,0)
      exact <- DqRetentionSql.forgetArtifact("closed-run","exact-v1").transact(xa)
      _ = assertEquals(exact,1)
      _ <- sql"""INSERT INTO octopus_core.dq_outbox(outbox_id,topic,message_key,payload)
        VALUES('retention-pending','dq.events.v1','run',jsonb_build_object('run_id','closed-run'))""".update.run.transact(xa)
      pending <- DqRetentionSql.pruneRuns("retention-test",namespace,100).transact(xa)
      _ = assertEquals(pending,0)
      _ <- sql"UPDATE octopus_core.dq_outbox SET status='published' WHERE outbox_id='retention-pending'".update.run.transact(xa)
      removed <- DqRetentionSql.pruneRuns("retention-test",namespace,100).transact(xa)
      _ = assertEquals(removed,1)
    yield ()
  }

  test("invalid expired references are discarded so retention can continue pruning") {
    val namespace = "invalid-reference"
    for
      _ <- seed("invalid-reference-run",namespace,"failed",Json.obj())
      _ <- DqRetentionSql.closeArtifacts("retention-test",namespace,100).transact(xa)
      _ <- sql"""UPDATE octopus_core.dq_artifacts SET reference=jsonb_set(reference,'{uri}',to_jsonb('s3://other-bucket/invalid'::TEXT))
        WHERE artifact_id='invalid-reference-run'""".update.run.transact(xa)
      semaphore <- Semaphore[IO](1)
      _ <- emptyObjectStore.use { endpoint =>
        val config = DqConfig(installationId="retention-test",namespace=namespace)
        val store = new DqStore(xa,DqSettings("retention-test",namespace,Map.empty,"p1",Set.empty))
        val storage = ArchiveConfig(endpoint=endpoint,accessKey="access-key",secretKey="secret-key")
        DqRetention.resource(config,storage,store,semaphore).use(_.collect)
      }
      artifactExists <- sql"SELECT EXISTS(SELECT 1 FROM octopus_core.dq_artifacts WHERE artifact_id='invalid-reference-run')".query[Boolean].unique.transact(xa)
      _ = assert(!artifactExists)
      runExists <- sql"SELECT EXISTS(SELECT 1 FROM octopus_core.dq_runs WHERE run_id='invalid-reference-run')".query[Boolean].unique.transact(xa)
      _ = assert(!runExists)
    yield ()
  }

  test("active and legacy applied runs without review references fail closed for retention") {
    for
      _ <- seed("active-run","safe","running",Json.obj())
      _ <- seed("legacy-run","safe","succeeded",Json.obj(),true)
      changed <- DqRetentionSql.closeArtifacts("retention-test","safe",100).transact(xa)
      _ = assertEquals(changed,0)
    yield ()
  }

  test("a reconciled not_match decision closes its review and retains evidence for ninety days") {
    for
      _ <- IdentityGraphSql.persistMergeCandidate("02:00:00:00:12:01","02:00:00:00:12:02",0.90).transact(xa)
      candidate <- sql"SELECT candidate_id FROM atheros_search.merge_candidates WHERE mac_a='02:00:00:00:12:01'".query[String].unique.transact(xa)
      _ <- sql"""INSERT INTO atheros_search.merge_decisions(candidate_id,decision,decided_by)
        VALUES($candidate,'not_match','retention-test')""".update.run.transact(xa)
      _ <- IdentityGraphSql.reconcileMergeConfirmationsFor(List(candidate)).transact(xa)
      status <- sql"SELECT status FROM atheros_search.merge_candidates WHERE candidate_id=$candidate".query[String].unique.transact(xa)
      _ = assertEquals(status,"not_match")
      _ <- seed("not-match-run","rejected-review","succeeded",Json.obj("review_candidate_ids"->Json.arr(Json.fromString(candidate))),true)
      closed <- DqRetentionSql.closeArtifacts("retention-test","rejected-review",100).transact(xa)
      _ = assertEquals(closed,1)
      expired <- DqRetentionSql.expiredArtifacts("retention-test","rejected-review",100).transact(xa)
      _ = assertEquals(expired,Nil)
    yield ()
  }

  test("reopened automatic confirmation blocks expiration and resets the closure window") {
    for
      _ <- IdentityGraphSql.persistMergeCandidate("02:00:00:00:13:01","02:00:00:00:13:02",0.99).transact(xa)
      candidate <- sql"SELECT candidate_id FROM atheros_search.merge_candidates WHERE mac_a='02:00:00:00:13:01'".query[String].unique.transact(xa)
      _ <- sql"""UPDATE atheros_search.merge_candidates SET status='confirmed',confirmation_source='automatic',
        trusted_registered_device_id='00000000-0000-0000-0000-000000000001'::UUID,confirmed_at=CURRENT_TIMESTAMP,
        updated_at=CURRENT_TIMESTAMP-INTERVAL '100 days' WHERE candidate_id=$candidate""".update.run.transact(xa)
      _ <- seed("reopened-run","reopened-review","succeeded",Json.obj("review_candidate_ids"->Json.arr(Json.fromString(candidate))),true)
      _ <- DqRetentionSql.closeArtifacts("retention-test","reopened-review",100).transact(xa)
      initially <- DqRetentionSql.expiredArtifacts("retention-test","reopened-review",100).transact(xa)
      _ = assertEquals(initially.size,1)
      _ <- IdentityGraphSql.reconcileMergeConfirmationsFor(List(candidate)).transact(xa)
      status <- sql"SELECT status FROM atheros_search.merge_candidates WHERE candidate_id=$candidate".query[String].unique.transact(xa)
      _ = assertEquals(status,"pending")
      held <- DqRetentionSql.expiredArtifacts("retention-test","reopened-review",100).transact(xa)
      _ = assertEquals(held,Nil)
      reset <- DqRetentionSql.reopenArtifacts("retention-test","reopened-review",100).transact(xa)
      _ = assertEquals(reset,1)
      open <- sql"SELECT closed_at IS NULL AND retained_until IS NULL FROM octopus_core.dq_artifacts WHERE artifact_id='reopened-run'".query[Boolean].unique.transact(xa)
      _ = assert(open)
    yield ()
  }

  test("reconfirmation between collection cycles extends retention from the new closure") {
    for
      _ <- IdentityGraphSql.persistMergeCandidate("02:00:00:00:14:01","02:00:00:00:14:02",0.99).transact(xa)
      candidate <- sql"SELECT candidate_id FROM atheros_search.merge_candidates WHERE mac_a='02:00:00:00:14:01'".query[String].unique.transact(xa)
      _ <- sql"""UPDATE atheros_search.merge_candidates SET status='confirmed',confirmation_source='automatic',
        trusted_registered_device_id='00000000-0000-0000-0000-000000000001'::UUID,confirmed_at=CURRENT_TIMESTAMP,
        updated_at=CURRENT_TIMESTAMP-INTERVAL '100 days' WHERE candidate_id=$candidate""".update.run.transact(xa)
      _ <- seed("reconfirmed-run","reconfirmed-review","succeeded",Json.obj("review_candidate_ids"->Json.arr(Json.fromString(candidate))),true)
      _ <- DqRetentionSql.closeArtifacts("retention-test","reconfirmed-review",100).transact(xa)
      _ <- IdentityGraphSql.reconcileMergeConfirmationsFor(List(candidate)).transact(xa)
      _ <- sql"""UPDATE atheros_search.merge_candidates SET status='confirmed',confirmation_source='automatic',
        trusted_registered_device_id='00000000-0000-0000-0000-000000000001'::UUID,confirmed_at=CURRENT_TIMESTAMP,
        updated_at=CURRENT_TIMESTAMP WHERE candidate_id=$candidate""".update.run.transact(xa)
      held <- DqRetentionSql.expiredArtifacts("retention-test","reconfirmed-review",100).transact(xa)
      _ = assertEquals(held,Nil)
      refreshed <- DqRetentionSql.closeArtifacts("retention-test","reconfirmed-review",100).transact(xa)
      _ = assertEquals(refreshed,1)
      future <- sql"SELECT retained_until>CURRENT_TIMESTAMP+INTERVAL '89 days' FROM octopus_core.dq_artifacts WHERE artifact_id='reconfirmed-run'".query[Boolean].unique.transact(xa)
      _ = assert(future)
    yield ()
  }

  test("orphan collection only accepts this installation's exact input and attempt paths") {
    val config = DqConfig(installationId="i",namespace="n")
    assert(DqRetention.allowedKey("i/n/inputs/run/hash",config))
    assert(DqRetention.allowedKey("i/n/runs/run/attempts/attempt/object",config))
    List("other/n/inputs/run/hash","i/n/inputs/run/../hash","i/n/runs/run/object","i/n/ui/remote.js","i/n/inputs/run/hash/extra")
      .foreach(key => assert(!DqRetention.allowedKey(key,config),key))
    IO.unit
  }

  test("durable DQ metrics isolate installations and expose active queue age") {
    for
      _ <- seed("metrics-run","metrics","queued",Json.obj())
      _ <- sql"UPDATE octopus_core.dq_runs SET created_at=CURRENT_TIMESTAMP-INTERVAL '5 seconds' WHERE run_id='metrics-run'".update.run.transact(xa)
      values <- DqMetricsSql.sample("retention-test","metrics").transact(xa)
      _ = assertEquals(values("queue.runs"),1.0)
      _ = assert(values("queue.oldest.seconds")>=5.0)
      other <- DqMetricsSql.sample("other-installation","metrics").transact(xa)
      _ = assertEquals(other("queue.runs"),0.0)
    yield ()
  }
