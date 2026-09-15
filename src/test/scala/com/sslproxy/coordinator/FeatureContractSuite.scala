package com.sslproxy.coordinator

import com.sslproxy.coordinator.processor.{ProcessorFamily, ProcessorId, ProcessorOwner}
import munit.FunSuite

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

class FeatureContractSuite extends FunSuite:
  private val serviceRoot = Path.of(sys.props("user.dir"))
  private val featuresRoot = serviceRoot.resolve("src/test/resources/features")
  private val processorTag = raw"@processor-([a-z0-9-]+)".r

  private def featureName(family: ProcessorFamily): String =
    s"${family.value.replace('_', '-')}.feature"

  private def featureFiles: Map[String, String] =
    val paths = Files.list(featuresRoot)
    try
      paths.iterator().asScala
        .filter(path => path.getFileName.toString.endsWith(".feature"))
        .map(path => path.getFileName.toString -> Files.readString(path))
        .toMap
    finally paths.close()

  test("feature files match the processor family taxonomy") {
    val expected = ProcessorFamily.values.map(featureName).toSet
    assertEquals(featureFiles.keySet, expected)
  }

  test("every Octopus-owned processor has an executable scenario in its family feature") {
    val features = featureFiles
    ProcessorId.octopusOwned.foreach { id =>
      val fileName = featureName(id.family)
      val content = features(fileName)
      val tag = s"@processor-${id.value}"
      val tagIndex = content.indexOf(tag)
      assert(tagIndex >= 0, s"missing $tag in $fileName")
      assert(content.indexOf("Scenario", tagIndex) >= 0, s"$tag does not annotate a scenario")
      val processorRow =
        raw"""(?m)^\s*\|\s*${java.util.regex.Pattern.quote(id.value)}\s*\|""".r
      assert(
        processorRow.findFirstIn(content).nonEmpty,
        s"${id.value} has no executable example in $fileName"
      )
    }
  }

  test("feature files do not reference external or unknown processors") {
    val features = featureFiles
    val tagsByFile = features.view.mapValues { content =>
      processorTag.findAllMatchIn(content).map(_.group(1)).toSet
    }.toMap
    val tags = tagsByFile.values.flatten.toSet
    val known = ProcessorId.all.map(id => id.value -> id).toMap
    val unknown = tags.diff(known.keySet)
    assertEquals(unknown, Set.empty)
    val external = tags.filter(id => known(id).owner != ProcessorOwner.Octopus)
    assertEquals(external, Set.empty)
    tagsByFile.foreachEntry { (fileName, fileTags) =>
      val misplaced = fileTags.filter(id => featureName(known(id).family) != fileName)
      assertEquals(misplaced, Set.empty, s"processor tags in $fileName must match its family")
    }
    val content = features.values.mkString("\n")
    ProcessorId.all.filter(_.owner != ProcessorOwner.Octopus).foreach { id =>
      assert(!content.contains(id.value), s"external processor ${id.value} is referenced by a feature")
    }
  }
