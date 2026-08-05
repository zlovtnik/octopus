package com.sslproxy.coordinator

import com.sslproxy.coordinator.processor.ProcessorId
import munit.FunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class ProcessorWorkloadWiringSuite extends FunSuite:
  test("every Octopus-owned processor has exactly one workload declaration"):
    val source = Files.readString(
      java.nio.file.Path.of("src/main/scala/com/sslproxy/coordinator/Main.scala"),
      StandardCharsets.UTF_8
    )

    ProcessorId.octopusOwned.foreach { id =>
      val caseName = id.productPrefix
      val declaration = raw"ProcessorWorkload\(\s*ProcessorId\.$caseName\b".r
      val count = declaration.findAllMatchIn(source).size
      assertEquals(count, 1, s"${id.value} workload declaration count")
    }
