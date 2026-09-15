@processor-rf-alert-projector
Feature: Search projection processor contracts

  Scenario Outline: An Octopus search projection processor has its declared durable contract
    Given processor "<processor>" belongs to "search_projection"
    Then the processor is owned by Octopus
    And the processor declares "<input>" as an input

    Examples:
      | processor          | input           |
      | rf-alert-projector | wireless events |
