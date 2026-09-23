package com.sslproxy.coordinator.observability

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.observability.StructuredLogger
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.core.instrument.{Counter, Gauge, MeterRegistry}
import io.micrometer.core.instrument.binder.jvm.{JvmGcMetrics, JvmMemoryMetrics}
import org.apache.kafka.common.{Metric, MetricName}

import java.util.concurrent.{ConcurrentHashMap, atomic}
import scala.jdk.CollectionConverters.*

import atomic.AtomicLong

class CoordinatorMetrics(private val registry: MeterRegistry):
  import CoordinatorMetrics.{ProcessorLifecycleValues, log}

  private val kafkaGauges = scala.collection.mutable.Map.empty[(String, String, String, String), (atomic.AtomicReference[java.lang.Double], Gauge)]

  def jvmMetrics: Resource[IO, Unit] =
    Resource.make(IO {
      val gc = new JvmGcMetrics()
      new JvmMemoryMetrics().bindTo(registry)
      gc.bindTo(registry)
      gc
    })(gc => IO { gc.close(); registry.close() }).map(_ => ())

  def recordKafkaMetrics(group: String, values: Map[MetricName, Metric]): Unit = synchronized {
    val samples = values.iterator.flatMap { case (name, metric) =>
      val exported = name.name() match
        case "records-lag" => Some("coordinator.kafka.partition.lag")
        case "rebalance-total" => Some("coordinator.kafka.rebalances")
        case _ => None
      exported.flatMap { metricName =>
        metric.metricValue() match
          case number: java.lang.Number if java.lang.Double.isFinite(number.doubleValue()) =>
            Some((group, metricName, Option(name.tags().get("topic")).getOrElse(""),
              Option(name.tags().get("partition")).getOrElse("")) -> number.doubleValue())
          case _ => None
      }
    }.toMap
    kafkaGauges.keysIterator.filter(key => key._1 == group && !samples.contains(key)).toList.foreach { key =>
      kafkaGauges.remove(key).foreach(value => registry.remove(value._2): Unit)
    }
    samples.foreachEntry { (key, sample) =>
      val (holder, _) = kafkaGauges.getOrElseUpdate(key, {
        val value = new atomic.AtomicReference[java.lang.Double](sample)
        val gauge = Gauge.builder(key._2, value, (v: atomic.AtomicReference[java.lang.Double]) => v.get().doubleValue())
          .tags("group", key._1, "topic", key._3, "partition", key._4).register(registry)
        (value, gauge)
      })
      holder.set(sample)
    }
  }

  def clearKafkaMetrics(group: String): Unit = recordKafkaMetrics(group, Map.empty)

  private val pendingLedgerGauge: AtomicLong = new AtomicLong(0)
  private val backpressureActiveGauge: AtomicLong = new AtomicLong(0)
  private val ingestLastSuccessTimestamp: AtomicLong = new AtomicLong(0)

  private val routeRunningGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val routeSuspendedGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val processorLifecycleGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val processorRestartGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val processorRetryCounters: ConcurrentHashMap[String, Counter] = ConcurrentHashMap()

  private val loopAttemptsCounter: Counter = Counter
    .builder("coordinator.loop.attempts.total")
    .description("Total main loop iterations")
    .register(registry)

  private val ingestInvocationsCounter: Counter = Counter
    .builder("coordinator.ingest.ledger.invocations.total")
    .description("Total process_ingest_ledger invocations")
    .register(registry)

  private val ingestProcessedCounter: Counter = Counter
    .builder("coordinator.ingest.processed.total")
    .description("Total events processed by ingest ledger")
    .register(registry)

  private val batchesDispatchedCounter: Counter = Counter
    .builder("coordinator.batches.dispatched.total")
    .description("Total batches dispatched")
    .register(registry)

  private val heartbeatCounter: Counter = Counter
    .builder("coordinator.heartbeat.total")
    .description("Heartbeat counter")
    .register(registry)

  private val payloadAuditIngestedCounter: Counter = Counter
    .builder("coordinator.payload.audit.ingested.total")
    .description("Total payload audit records ingested")
    .register(registry)

  private val payloadAuditDlqCounter: Counter = Counter
    .builder("coordinator.payload.audit.dlq.total")
    .description("Total payload audit records sent to DLQ")
    .register(registry)

  private val syncEventsHydratedCounter: Counter = Counter
    .builder("coordinator.sync.events.hydrated.total")
    .description("Total sync event payloads hydrated into the durable ledger")
    .register(registry)

  private val syncEventsBackfillFailedCounter: Counter =
    Counter
      .builder("coordinator.sync.events.backfill.failed.total")
      .description("Total sync event payloads that failed historical hydration")
      .register(registry)

  Gauge
    .builder("coordinator.pending.ledger.count", pendingLedgerGauge, (value: AtomicLong) => value.doubleValue())
    .description("Number of pending ledger entries")
    .register(registry)

  Gauge
    .builder("coordinator.backpressure.active", backpressureActiveGauge, (value: AtomicLong) => value.doubleValue())
    .description("1 if backpressure is throttling, 0 otherwise")
    .register(registry)

  Gauge
    .builder(
      "coordinator.ingest.ledger.last.success.timestamp.seconds",
      ingestLastSuccessTimestamp,
      (value: AtomicLong) => value.doubleValue()
    )
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
    if success then ingestLastSuccessTimestamp.set(System.currentTimeMillis() / 1000)

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
    val runningHolder = routeRunningGauges.computeIfAbsent(
      tagKey,
      _ =>
        val h = new AtomicLong(if running then 1L else 0L)
        Gauge
          .builder("coordinator.route.running", h, (v: AtomicLong) => v.doubleValue())
          .tags("role", role, "route", routeId)
          .register(registry)
        h
    )
    runningHolder.set(if running then 1L else 0L)
    val suspendedHolder = routeSuspendedGauges.computeIfAbsent(
      tagKey,
      _ =>
        val h = new AtomicLong(if suspended then 1L else 0L)
        Gauge
          .builder("coordinator.route.suspended", h, (v: AtomicLong) => v.doubleValue())
          .tags("role", role, "route", routeId)
          .register(registry)
        h
    )
    suspendedHolder.set(if suspended then 1L else 0L)

  def recordProcessorState(processorId: String, lifecycle: String, restartCount: Int): Unit =
    if !ProcessorLifecycleValues.contains(lifecycle) then
      log.warn("processor_lifecycle_unknown", "processor" -> processorId, "lifecycle" -> lifecycle)
    ProcessorLifecycleValues.foreach { state =>
      val key = s"$processorId:$state"
      val holder = processorLifecycleGauges.computeIfAbsent(
        key,
        _ =>
          val value = new AtomicLong(0L)
          Gauge
            .builder("coordinator.processor.lifecycle", value, (v: AtomicLong) => v.doubleValue())
            .tags("processor", processorId, "state", state)
            .description("One-hot processor lifecycle state")
            .register(registry)
          value
      )
      holder.set(if state == lifecycle then 1L else 0L)
    }

    val restartHolder = processorRestartGauges.computeIfAbsent(
      processorId,
      _ =>
        val value = new AtomicLong(0L)
        Gauge
          .builder("coordinator.processor.restart.count", value, (v: AtomicLong) => v.doubleValue())
          .tag("processor", processorId)
          .description("Current persisted processor restart count")
          .register(registry)
        value
    )
    restartHolder.set(restartCount.toLong)

  def recordProcessorRetry(processorId: String): Unit =
    processorRetryCounters
      .computeIfAbsent(
        processorId,
        _ =>
          Counter
            .builder("coordinator.processor.retries.total")
            .tag("processor", processorId)
            .description("Total supervised processor retries")
            .register(registry)
      )
      .increment()

  def heartbeat(): IO[Unit] =
    IO(heartbeatCounter.increment()) *>
      IO(
        log.info(
          "heartbeat",
          "loop_count" -> loopAttemptsCounter.count().toLong.toString,
          "pending_ledger_count" -> pendingLedgerGauge.get().toString,
          "backpressure_active" -> backpressureActiveGauge.get().toString
        )
      )

  def scrape: String =
    registry.getMeters.asScala.toList
      .flatMap { meter =>
        val id = meter.getId
        val baseName = id.getName.replace('.', '_').replace('-', '_')
        val labels = id.getTags.asScala.toList
          .map(tag => s"${tag.getKey}=\"${escapeLabel(tag.getValue)}\"")
        val labelText = if labels.isEmpty then "" else labels.mkString("{", ",", "}")
        meter.measure().asScala.map { measurement =>
          val suffix = measurement.getStatistic.toString.toLowerCase(java.util.Locale.ROOT)
          s"${baseName}_$suffix$labelText ${measurement.getValue}"
        }
      }
      .mkString("", "\n", "\n")

  private def escapeLabel(value: String): String =
    value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")

object CoordinatorMetrics:
  private val log = StructuredLogger(getClass)
  private val ProcessorLifecycleValues = List(
    "disabled",
    "starting",
    "ready",
    "backing_off",
    "failed_terminal"
  )

  def apply(): CoordinatorMetrics = new CoordinatorMetrics(new SimpleMeterRegistry())
