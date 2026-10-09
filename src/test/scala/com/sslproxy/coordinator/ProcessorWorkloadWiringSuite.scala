package com.sslproxy.coordinator

import com.sslproxy.coordinator.processor.ProcessorId
import munit.FunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class ProcessorWorkloadWiringSuite extends FunSuite:
  test("every Octopus-owned processor has exactly one workload declaration"):
    val sources = List(
      "wiring/workloads/ConsumerWorkloads.scala",
      "wiring/workloads/ScheduledWorkloads.scala",
      "wiring/workloads/RetentionWorkloads.scala",
      "wiring/RuntimeStreams.scala"
    )
    val source = sources.map { path =>
      Files.readString(
        java.nio.file.Path.of(s"src/main/scala/com/sslproxy/coordinator/$path"),
        StandardCharsets.UTF_8
      )
    }.mkString("\n")

    ProcessorId.octopusOwned.foreach { id =>
      val caseName = id.productPrefix
      val declaration = raw"ProcessorWorkload\(\s*ProcessorId\.$caseName\b".r
      val count = declaration.findAllMatchIn(source).size
      assertEquals(count, 1, s"${id.value} workload declaration count")
    }
    assertEquals(
      raw"hydrationService\.runOnce".r.findAllMatchIn(source).size,
      1,
      "hydration backfill must run only through requiredRuntimeStreams"
    )
