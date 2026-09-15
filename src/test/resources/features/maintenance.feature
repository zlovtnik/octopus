@processor-event-retention @processor-search-retention @processor-stale-worker-cleanup @processor-scheduled-reconciliation
Feature: Maintenance processor contracts

  Scenario Outline: An Octopus maintenance processor has its declared durable contract
    Given processor "<processor>" belongs to "maintenance"
    Then the processor is owned by Octopus
    And the processor declares "<input>" as an input

    Examples:
      | processor                  | input                 |
      | event-retention            | expired core events   |
      | search-retention           | expired search rows   |
      | stale-worker-cleanup       | expired worker leases |
      | scheduled-reconciliation   | domain/projection state|
