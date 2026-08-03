package com.sslproxy.coordinator.tidb.sql

import munit.FunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.*

class SqlPlacementSuite extends FunSuite:
  test("SQL instructions exist only in catalog modules"):
    val sourceRoot = serviceRoot.resolve("src/main/scala/com/sslproxy/coordinator")
    val catalogRoot = sourceRoot.resolve("tidb/sql")
    val sqlInterpolator = raw"\b(?:sql|fr|fr0)\s*\"".r
    val instruction = raw"(?is)(?:\"\"\"|\")\s*(?:SELECT|INSERT\s+INTO|UPDATE|DELETE\s+FROM)\b".r

    val violations = scalaFiles(sourceRoot).filterNot(_.startsWith(catalogRoot)).flatMap { path =>
      val source = Files.readString(path, StandardCharsets.UTF_8)
      Option.when(sqlInterpolator.findFirstIn(source).nonEmpty || instruction.findFirstIn(source).nonEmpty)(
        serviceRoot.relativize(path).toString
      )
    }

    assertEquals(violations, Nil)

  private def scalaFiles(path: Path): List[Path] =
    if Files.isRegularFile(path) then List(path)
    else if !Files.isDirectory(path) then Nil
    else
      val stream = Files.walk(path)
      try stream.iterator.asScala.filter(value => Files.isRegularFile(value) && value.toString.endsWith(".scala")).toList
      finally stream.close()

  private def serviceRoot: Path =
    val current = Paths.get("").toAbsolutePath.normalize
    Iterator.iterate(current)(_.getParent)
      .takeWhile(_ != null)
      .find(path => Files.isRegularFile(path.resolve("build.sbt")))
      .getOrElse(fail(s"could not locate Octopus service root from $current"))
