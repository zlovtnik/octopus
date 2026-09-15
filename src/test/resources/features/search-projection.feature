@processor-rf-alert-projector
Feature: Search projection processor contracts

  Scenario: The RF alert projector has its declared durable contract
    Given processor "rf-alert-projector" belongs to "search_projection"
    Then the processor is owned by Octopus
    And the processor declares "wireless events" as an input
