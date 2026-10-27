@processor-rf-alert-projector @processor-threat-risk-projector
Feature: Search projection processor contracts
  Legacy blocked-host intensity remains explicitly uncalibrated observation
  evidence, with no claim of measured transfer, attempts or a confirmed threat.
  Expired or corrected source evidence resolves its signal; renewed qualifying
  observations may reopen it. Late observations preserve event-time latest
  rollup metadata. Similarity requires current text and matching unified and
  per-kind vectors on both sides within the selected scope and input revision.
  Bounded DNS and AP risk passes prioritize never-projected and oldest-projected
  candidates so recent sources cannot permanently starve the retained backlog.

  Scenario Outline: An Octopus search projection processor has its declared durable contract
    Given processor "<processor>" belongs to "search_projection"
    Then the processor is owned by Octopus
    And the processor declares "<input>" as an input

    Examples:
      | processor          | input           |
      | rf-alert-projector | wireless events |
      | threat-risk-projector | wireless alerts |
