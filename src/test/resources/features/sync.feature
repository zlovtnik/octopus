@processor-sync-scan-ingestion @processor-sync-job-planner @processor-sync-backlog-recovery @processor-sync-load-dispatch @processor-sync-load-consumer @processor-sync-result-consumer @processor-sync-outbox-publisher
Feature: Sync processor contracts

  Scenario Outline: An Octopus sync processor has its declared durable contract
    Given processor "<processor>" belongs to "sync"
    Then the processor is owned by Octopus
    And the processor declares "<input>" as an input

    Examples:
      | processor                | input               |
      | sync-scan-ingestion      | sync.scan.request   |
      | sync-job-planner         | sync_events         |
      | sync-backlog-recovery    | expired sync leases |
      | sync-load-dispatch       | sync_batches        |
      | sync-load-consumer       | sync.oracle.load    |
      | sync-result-consumer     | sync.oracle.result  |
      | sync-outbox-publisher    | outbox_events       |
