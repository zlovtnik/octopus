package com.sslproxy.coordinator

import com.sslproxy.coordinator.processor.ProcessorId
import munit.FunSuite

import java.nio.file.{Files, Path}

class DocumentationContractSuite extends FunSuite:
  private val serviceRoot = Path.of(sys.props("user.dir"))
  private val readme = Files.readString(serviceRoot.resolve("README.md"))
  private val application = Files.readString(serviceRoot.resolve("src/main/resources/application.conf"))
  private val healthRoutes = Files.readString(
    serviceRoot.resolve("src/main/scala/com/sslproxy/coordinator/http/HealthRoutes.scala")
  )

  test("README documents every shared processor ID") {
    ProcessorId.all.foreach(id => assert(readme.contains(s"`${id.value}`"), id.value))
  }

  test("README, configuration, and runtime keep locked topics aligned") {
    List("sync.scan.request", "sync.oracle.load", "sync.oracle.result").foreach { topic =>
      assert(readme.contains(topic), topic)
      assert(application.contains(topic), topic)
    }
  }

  test("documented HTTP endpoints exist in the route source") {
    List("live", "ready", "metrics", "health", "prometheus").foreach { endpoint =>
      assert(readme.contains(s"/$endpoint") || readme.contains(s"actuator/$endpoint"), endpoint)
      assert(healthRoutes.contains(s"\"$endpoint\""), endpoint)
    }
  }

  test("documented runtime gates match application configuration") {
    val gates = List(
      "OCTOPUS_PROCESSORS_ENABLED" -> "processors-enabled",
      "OCTOPUS_CONSUMERS_ENABLED" -> "consumers-enabled",
      "OCTOPUS_ENABLED_PROCESSORS" -> "enabled"
    )
    gates.foreach { case (environment, key) =>
      assert(readme.contains(environment), environment)
      assert(application.contains(environment), environment)
      assert(application.contains(key), key)
    }
  }
