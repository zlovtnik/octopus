package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.parser.parse
import munit.FunSuite

import java.nio.file.{Files, Path}

class ProcessorCatalogSuite extends FunSuite:
  test("catalog covers every runtime processor exactly once") {
    val ids = ProcessorCatalog.contracts.map(_.id)
    assertEquals(ids.toSet, ProcessorId.all.toSet)
    assertEquals(ids.distinct.size, ids.size)
  }

  test("runtime ownership is exactly 34 Octopus and 2 Atheros Search processors") {
    assertEquals(ProcessorId.octopusOwned.size, 34)
    assertEquals(ProcessorId.all.count(_.owner == ProcessorOwner.AtherosSearch), 2)
    assertEquals(
      ProcessorId.all.filter(_.owner == ProcessorOwner.AtherosSearch).map(_.value).toSet,
      Set("embedding-completer", "embedding-lease-recovery")
    )
  }

  test("processor identifiers are stable and kebab-cased") {
    ProcessorId.all.foreach { id =>
      assert(id.value.matches("^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$"), id.value)
      assertEquals(ProcessorId.fromString(id.value), Right(id))
    }
  }

  test("every processor declares its consistency contract") {
    ProcessorCatalog.contracts.foreach { contract =>
      assert(contract.inputs.nonEmpty, contract.id.value)
      assert(contract.outputs.nonEmpty, contract.id.value)
      assert(contract.dedupeKey.nonEmpty, contract.id.value)
      assert(contract.leaseScope.nonEmpty, contract.id.value)
      assert(contract.terminalBehavior.nonEmpty, contract.id.value)
      assert(contract.reconciliationPolicy.nonEmpty, contract.id.value)
      assert(!contract.defaultEnabled, contract.id.value)
      contract.dependencies.foreach(dependency => assertNotEquals(dependency, contract.id))
    }
  }

  test("payload audit contract matches the content-addressed consumer") {
    val contract = ProcessorCatalog.byId(ProcessorId.PayloadAuditIngestion)

    assertEquals(contract.inputs, List("proxy.payload_audit"))
    assertEquals(contract.dependencies, Nil)
    assertEquals(contract.dedupeKey, "stream_name/payload_sha256")
  }

  test("shared manifest exactly matches runtime processor contracts") {
    val manifest = findRepositoryRoot(Path.of(sys.props("user.dir")))
      .resolve("sql/postgres/contracts/processors.json")
    val json = parse(Files.readString(manifest)).fold(throw _, identity)
    val entries = json.hcursor.downField("processors").as[List[Json]].fold(throw _, identity)
    val manifestContracts = entries.map { entry =>
      val cursor = entry.hcursor
      val id = cursor.get[String]("id").fold(throw _, identity)
      id -> (
        cursor.get[String]("owner").fold(throw _, identity),
        cursor.get[String]("family").fold(throw _, identity),
        cursor.get[String]("mode").fold(throw _, identity),
        cursor.get[List[String]]("inputs").fold(throw _, identity),
        cursor.get[List[String]]("outputs").fold(throw _, identity),
        cursor.get[List[String]]("dependencies").fold(throw _, identity),
        cursor.get[String]("dedupe_key").fold(throw _, identity),
        cursor.get[String]("lease_scope").fold(throw _, identity),
        cursor.get[String]("terminal_behavior").fold(throw _, identity),
        cursor.get[String]("reconciliation_policy").fold(throw _, identity),
        cursor.get[Boolean]("default_enabled").fold(throw _, identity)
      )
    }

    val runtimeContracts = ProcessorCatalog.contracts.map { contract =>
      contract.id.value -> (
        contract.id.owner.value,
        contract.id.family.value,
        contract.mode.value,
        contract.inputs,
        contract.outputs,
        contract.dependencies.map(_.value),
        contract.dedupeKey,
        contract.leaseScope,
        contract.terminalBehavior,
        contract.reconciliationPolicy,
        contract.defaultEnabled
      )
    }

    assertEquals(manifestContracts.map(_._1).distinct.size, manifestContracts.size)
    assertEquals(manifestContracts.toMap, runtimeContracts.toMap)
  }

  private def findRepositoryRoot(start: Path): Path =
    Iterator.iterate(start)(_.getParent)
      .takeWhile(_ != null)
      .find(path => Files.exists(path.resolve("sql/postgres/contracts/manifest.yaml")))
      .getOrElse(fail(s"repository root not found from $start"))
