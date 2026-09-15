package com.sslproxy.coordinator.bdd

import io.cucumber.junit.platform.engine.Constants
import org.junit.platform.suite.api.*

@Suite
@IncludeEngines(Array("cucumber"))
@SelectPackages(Array("features"))
@ConfigurationParameter(
  key = Constants.GLUE_PROPERTY_NAME,
  value = "com.sslproxy.coordinator.bdd"
)
class RunCucumberTest
