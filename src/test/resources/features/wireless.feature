@processor-wireless-heartbeat-ingestion @processor-wireless-frame-normalizer @processor-wireless-inventory-projector @processor-wireless-identity-projector
Feature: Wireless processor contracts

  Scenario Outline: An Octopus wireless processor has its declared durable contract
    Given processor "<processor>" belongs to "wireless"
    Then the processor is owned by Octopus
    And the processor declares "<input>" as an input

    Examples:
      | processor                    | input                     |
      | wireless-heartbeat-ingestion  | wireless.sensor.heartbeat |
      | wireless-frame-normalizer     | wireless.audit            |
      | wireless-inventory-projector  | wireless normalized tables|
      | wireless-identity-projector   | inventory                 |
