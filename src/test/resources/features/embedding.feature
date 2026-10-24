@processor-embedding-preparer @processor-embedding-text-builder @processor-proxy-search-preparer
Feature: Embedding preparation processor contracts

  Proxy windows publish only privacy-safe legacy host values. Parked windows
  retire their current Search documents and cancel unfinished embedding jobs;
  later complete windows may resume only those parking-cancelled jobs.
  Input revision changes migrate retained active proxy documents in bounded
  pages, preserving terminal history and making room in old worker queues.

  Scenario Outline: An Octopus embedding processor has its declared durable contract
    Given processor "<processor>" belongs to "embedding"
    Then the processor is owned by Octopus
    And the processor declares "<input>" as an input

    Examples:
      | processor              | input                   |
      | embedding-preparer     | search documents        |
      | embedding-text-builder | normalized domain rows  |
      | proxy-search-preparer  | proxy observations      |
