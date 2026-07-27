package com.sslproxy.coordinator.observability

import cats.effect.IO
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.core.instrument.{Counter, Gauge, MeterRegistry}
import com.sslproxy.coordinator.observability.StructuredLogger

import java.util.concurrent.{ConcurrentHashMap, atomic}
import atomic.AtomicLong

class CoordinatorMetrics(registry: MeterRegistry):
  import CoordinatorMetrics.log

  private val pendingLedgerGauge: AtomicLong = new AtomicLong(0)
  private val backpressureActiveGauge: AtomicLong = new AtomicLong(0)
  private val ingestLastSuccessTimestamp: AtomicLong = new AtomicLong(0)

  private val routeRunningGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val routeSuspendedGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()

  private val loopAttemptsCounter: Counter = Counter.builder("coordinator.loop.attempts.total")
    .description("Total main loop iterations")
    .register(registry)

  private val ingestInvocationsCounter: Counter = Counter.builder("coordinator.ingest.ledger.invocations.total")
    .description("Total process_ingest_ledger invocations")
    .register(registry)

  private val ingestProcessedCounter: Counter = Counter.builder("coordinator.ingest.processed.total")
    .description("Total events processed by ingest ledger")
    .register(registry)

  private val batchesDispatchedCounter: Counter = Counter.builder("coordinator.batches.dispatched.total")
    .description("Total batches dispatched")
    .register(registry)

  private val heartbeatCounter: Counter = Counter.builder("coordinator.heartbeat.total")
    .description("Heartbeat counter")
    .register(registry)

  private val payloadAuditIngestedCounter: Counter = Counter.builder("coordinator.payload.audit.ingested.total")
    .description("Total payload audit records ingested")
    .register(registry)

  private val payloadAuditDlqCounter: Counter = Counter.builder("coordinator.payload.audit.dlq.total")
    .description("Total payload audit records sent to DLQ")
    .register(registry)

  private val syncEventsHydratedCounter: Counter = Counter.builder("coordinator.sync.events.hydrated.total")
    .description("Total sync event payloads hydrated into the durable ledger")
    .register(registry)

  private val syncEventsBackfillFailedCounter: Counter =
    Counter.builder("coordinator.sync.events.backfill.failed.total")
      .description("Total sync event payloads that failed historical hydration")
      .register(registry)

  Gauge.builder("coordinator.pending.ledger.count", pendingLedgerGauge, (value: AtomicLong) => value.doubleValue())
    .description("Number of pending ledger entries")
    .register(registry)

  Gauge.builder("coordinator.backpressure.active", backpressureActiveGauge, (value: AtomicLong) => value.doubleValue())
    .description("1 if backpressure is throttling, 0 otherwise")
    .register(registry)

  Gauge.builder("coordinator.ingest.ledger.last.success.timestamp.seconds",
      ingestLastSuccessTimestamp, (value: AtomicLong) => value.doubleValue())
    .description("Unix timestamp for the last successful ingest invocation")
    .baseUnit("seconds")
    .register(registry)

  def recordPendingLedgerCount(count: Long): Unit =
    pendingLedgerGauge.set(count)

  def recordBackpressureActive(active: Boolean): Unit =
    backpressureActiveGauge.set(if active then 1L else 0L)

  def incrementLoopCounter(): Unit =
    loopAttemptsCounter.increment()

  def recordIngestInvocation(success: Boolean): Unit =
    ingestInvocationsCounter.increment()
    if success then
      ingestLastSuccessTimestamp.set(System.currentTimeMillis() / 1000)

  def recordIngestProcessed(count: Long): Unit =
    if count > 0 then ingestProcessedCounter.increment(count.toDouble)

  def recordBatchDispatched(): Unit =
    batchesDispatchedCounter.increment()

  def recordPayloadAuditIngested(count: Int): Unit =
    payloadAuditIngestedCounter.increment(count.toDouble)

  def recordPayloadAuditDlq(): Unit =
    payloadAuditDlqCounter.increment()

  def recordSyncEventHydrated(count: Long = 1L): Unit =
    if count > 0 then syncEventsHydratedCounter.increment(count.toDouble)

  def recordSyncEventHydrationBackfill(hydrated: Long, failed: Long): Unit =
    recordSyncEventHydrated(hydrated)
    if failed > 0 then syncEventsBackfillFailedCounter.increment(failed.toDouble)

  def recordTickFailure(): Unit = ()

  def recordRouteState(role: String, routeId: String, running: Boolean, suspended: Boolean): Unit =
    val tagKey = s"$role:$routeId"
    val runningHolder = routeRunningGauges.computeIfAbsent(tagKey, _ =>
      val h = new AtomicLong(if running then 1L else 0L)
      Gauge.builder("coordinator.route.running", h, (v: AtomicLong) => v.doubleValue())
        .tags("role", role, "route", routeId)
        .register(registry)
      h
    )
    runningHolder.set(if running then 1L else 0L)
    val suspendedHolder = routeSuspendedGauges.computeIfAbsent(tagKey, _ =>
      val h = new AtomicLong(if suspended then 1L else 0L)
      Gauge.builder("coordinator.route.suspended", h, (v: AtomicLong) => v.doubleValue())
        .tags("role", role, "route", routeId)
        .register(registry)
      h
    )
    suspendedHolder.set(if suspended then 1L else 0L)

  def heartbeat(): IO[Unit] =
    IO(heartbeatCounter.increment()) *>
      IO(log.info("heartbeat",
        "loop_count" -> loopAttemptsCounter.count().toLong.toString,
        "pending_ledger_count" -> pendingLedgerGauge.get().toString,
        "backpressure_active" -> backpressureActiveGauge.get().toString
      ))

object CoordinatorMetrics:
  private val log = StructuredLogger(getClass)

  def apply(): CoordinatorMetrics = new CoordinatorMetrics(new SimpleMeterRegistry())
