@processor-embedding-preparer @processor-embedding-text-builder
Feature: Embedding preparation processor contracts

  Scenario Outline: An Octopus embedding processor has its declared durable contract
    Given processor "<processor>" belongs to "embedding"
    Then the processor is owned by Octopus
    And the processor declares "<input>" as an input

    Examples:
      | processor              | input                   |
      | embedding-preparer     | search documents        |
      | embedding-text-builder | normalized domain rows  |
