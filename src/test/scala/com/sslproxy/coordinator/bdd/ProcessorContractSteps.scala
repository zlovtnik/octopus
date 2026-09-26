package com.sslproxy.coordinator.bdd

import com.sslproxy.coordinator.processor.{ProcessorCatalog, ProcessorId, ProcessorOwner}
import io.cucumber.scala.{EN, ScalaDsl}

class ProcessorContractSteps extends ScalaDsl with EN:
  private var selected: Option[ProcessorId] = None

  Given("processor {string} belongs to {string}") { (processor: String, family: String) =>
    val id = ProcessorId.fromString(processor).fold(error => throw IllegalArgumentException(error), identity)
    assert(id.family.value == family, s"expected $processor to belong to $family")
    selected = Some(id)
  }

  Then("the processor is owned by Octopus") { () =>
    assert(requireSelected().owner == ProcessorOwner.Octopus)
  }

  Then("the processor declares {string} as an input") { (input: String) =>
    assert(ProcessorCatalog.byId(requireSelected()).inputs.contains(input), input)
  }

  Then("the processor declares {string} as an output") { (output: String) =>
    assert(ProcessorCatalog.byId(requireSelected()).outputs.contains(output), output)
  }

  private def requireSelected(): ProcessorId =
    selected.getOrElse(throw IllegalStateException("a processor must be selected before asserting its contract"))
