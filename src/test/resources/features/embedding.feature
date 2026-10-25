@processor-embedding-preparer @processor-embedding-text-builder @processor-proxy-search-preparer
Feature: Embedding preparation processor contracts

  Proxy windows publish only privacy-safe legacy host values. Parked windows
  retire their current Search documents and cancel unfinished embedding jobs;
  later complete windows may resume only those parking-cancelled jobs.
  Input revision changes migrate retained active proxy documents in bounded
  pages, preserving terminal history and making room in old worker queues.
  Full queues validate subsequent dirty windows even when earlier windows remain
  valid. Returning proxy content resumes eligible supersession-cancelled jobs;
  input revision rollbacks preserve existing job IDs, including legacy IDs.
  Corrected or removed proxy events retire stale documents and cancel unfinished
  jobs under the preparation fence, including when the queue is full. Moving or
  unblocking an observation invalidates its previous blocked window; corrected
  evidence timestamps agree with the source and current document.
  Source revision remains monotonic when current text returns to an earlier
  value: an older preparation cannot reactivate intermediate content or alter
  its jobs. A live lease requires the current owner, token and fence together.

  Scenario Outline: An Octopus embedding processor has its declared durable contract
    Given processor "<processor>" belongs to "embedding"
    Then the processor is owned by Octopus
    And the processor declares "<input>" as an input

    Examples:
      | processor              | input                   |
      | embedding-preparer     | search documents        |
      | embedding-text-builder | normalized domain rows  |
      | proxy-search-preparer  | proxy observations      |
