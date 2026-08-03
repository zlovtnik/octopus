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

  test("runtime ownership is exactly 26 Octopus and 2 Atheros Search processors") {
    assertEquals(ProcessorId.octopusOwned.size, 26)
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

  test("shared manifest has the same IDs and exactly one owner per ID") {
    val manifest = findRepositoryRoot(Path.of(sys.props("user.dir")))
      .resolve("sql/tidb/contracts/processors.json")
    val json = parse(Files.readString(manifest)).fold(throw _, identity)
    val entries = json.hcursor.downField("processors").as[List[Json]].fold(throw _, identity)
    val idsAndOwners = entries.map { entry =>
      val cursor = entry.hcursor
      val id = cursor.get[String]("id").fold(throw _, identity)
      val owner = cursor.get[String]("owner").fold(throw _, identity)
      id -> owner
    }

    assertEquals(idsAndOwners.map(_._1).distinct.size, idsAndOwners.size)
    assertEquals(idsAndOwners.toMap, ProcessorId.all.map(id => id.value -> id.owner.value).toMap)
  }

  private def findRepositoryRoot(start: Path): Path =
    Iterator.iterate(start)(_.getParent)
      .takeWhile(_ != null)
      .find(path => Files.exists(path.resolve("sql/tidb/contracts/manifest.yaml")))
      .getOrElse(fail(s"repository root not found from $start"))
