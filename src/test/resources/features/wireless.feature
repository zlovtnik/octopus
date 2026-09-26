@processor-wireless-heartbeat-ingestion @processor-wireless-frame-normalizer @processor-wireless-inventory-projector @processor-wireless-identity-projector @processor-wireless-behavior-projector @processor-wireless-timing-projector @processor-wireless-sequence-projector @processor-wireless-baseline-projector @processor-wireless-similarity-projector
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
      | wireless-behavior-projector   | wireless normalized tables|
      | wireless-timing-projector     | wireless normalized tables|
      | wireless-sequence-projector   | wireless normalized tables|
      | wireless-baseline-projector   | wireless normalized tables|
      | wireless-similarity-projector | search_vectors            |

  Scenario: Identity projection exposes only confirmed device relationships
    Given processor "wireless-identity-projector" belongs to "wireless"
    Then the processor declares "confirmed identity clusters and same_device edges" as an output
