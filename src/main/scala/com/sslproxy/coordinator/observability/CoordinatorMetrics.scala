package com.sslproxy.coordinator.observability

import cats.effect.{IO, Resource}
import com.sslproxy.coordinator.observability.StructuredLogger
import io.micrometer.core.instrument.{Counter, Gauge, Meter, Timer}
import io.micrometer.core.instrument.binder.jvm.{JvmGcMetrics, JvmMemoryMetrics}
import io.micrometer.prometheusmetrics.{PrometheusConfig, PrometheusMeterRegistry}
import org.apache.kafka.common.{Metric, MetricName}

import java.util.concurrent.{ConcurrentHashMap, TimeUnit, atomic}
import scala.concurrent.duration.FiniteDuration

import atomic.AtomicLong

class CoordinatorMetrics(
  private val registry: PrometheusMeterRegistry,
  nowMillis: () => Long = () => System.currentTimeMillis()
):
  import CoordinatorMetrics.{ProcessorLifecycleValues, log}

  private val kafkaGauges =
    scala.collection.mutable.Map.empty[(String, String, String, String), (atomic.AtomicReference[java.lang.Double], List[Meter])]
  private val lagSampleAt =
    ConcurrentHashMap[(String, String, String), AtomicLong]()

  def jvmMetrics: Resource[IO, Unit] =
    Resource.make(IO {
      val gc = new JvmGcMetrics()
      new JvmMemoryMetrics().bindTo(registry)
      gc.bindTo(registry)
      gc
    })(gc => IO(gc.close())).map(_ => ())

  def recordKafkaMetrics(group: String, values: Map[MetricName, Metric]): Unit = synchronized {
    val samples = values.iterator.flatMap { case (name, metric) =>
      val exported = name.name() match
        case "records-lag" => Some("coordinator.redpanda.consumer.lag.records")
        case "rebalance-total" => Some("coordinator.kafka.rebalances.total")
        case _ => None
      exported.flatMap { metricName =>
        metric.metricValue() match
          case number: java.lang.Number if java.lang.Double.isFinite(number.doubleValue()) =>
            Some(
              (group, metricName, Option(name.tags().get("topic")).getOrElse(""),
                Option(name.tags().get("partition")).getOrElse("")) -> number.doubleValue()
            )
          case _ => None
      }
    }.toMap
    kafkaGauges.keysIterator.filter(key => key._1 == group && !samples.contains(key)).toList.foreach { key =>
      kafkaGauges.remove(key).foreach { case (_, meters) =>
        meters.foreach(meter => registry.remove(meter): Unit)
      }
      if key._2 == "coordinator.redpanda.consumer.lag.records" then
        lagSampleAt.remove((key._1, key._3, key._4)): Unit
    }
    samples.foreachEntry { (key, sample) =>
      val (groupKey, metricName, topic, partition) = key
      val (holder, _) = kafkaGauges.getOrElseUpdate(key, {
        val value = new atomic.AtomicReference[java.lang.Double](sample)
        val tags = Array("group", groupKey, "topic", topic, "partition", partition)
        if metricName == "coordinator.kafka.rebalances.total" then
          val meter = Gauge
            .builder(metricName, value, (v: atomic.AtomicReference[java.lang.Double]) => v.get().doubleValue())
            .tags(tags*)
            .description("Cumulative Kafka consumer rebalance count")
            .register(registry)
          (value, List(meter))
        else
          val sampleAt = lagSampleAt.computeIfAbsent((groupKey, topic, partition), _ => new AtomicLong(nowMillis()))
          val lagMeter = Gauge
            .builder(metricName, value, (v: atomic.AtomicReference[java.lang.Double]) => v.get().doubleValue())
            .tags(tags*)
            .description("Consumer fetch-position lag in records")
            .register(registry)
          val staleMeter = Gauge
            .builder(
              "coordinator.redpanda.lag.stale.seconds",
              sampleAt,
              (v: AtomicLong) =>
                val at = v.get()
                if at <= 0L then 0.0
                else (nowMillis() - at).toDouble / 1000.0
            )
            .tags(tags*)
            .description("Seconds since the last successful lag sample")
            .register(registry)
          (value, List(lagMeter, staleMeter))
      })
      holder.set(sample)
      if metricName == "coordinator.redpanda.consumer.lag.records" then
        lagSampleAt.computeIfAbsent((groupKey, topic, partition), _ => new AtomicLong(0L)).set(nowMillis())
    }
  }

  def clearKafkaMetrics(group: String): Unit = recordKafkaMetrics(group, Map.empty)

  private val pendingLedgerGauge: AtomicLong = new AtomicLong(0)
  private val backpressureActiveGauge: AtomicLong = new AtomicLong(0)
  private val ingestLastSuccessTimestamp: AtomicLong = new AtomicLong(0)
  private val pendingObservedAt: AtomicLong = new AtomicLong(0)
  private val backpressureObservedAt: AtomicLong = new AtomicLong(0)
  private val processedObservedAt: AtomicLong = new AtomicLong(0)
  private val startedAt: Long = nowMillis()

  private val processedSamples: java.util.concurrent.atomic.AtomicReference[Vector[(Long, Long)]] =
    java.util.concurrent.atomic.AtomicReference(Vector.empty)
  private val brokerProcessedSamples: java.util.concurrent.atomic.AtomicReference[Vector[(Long, Long)]] =
    java.util.concurrent.atomic.AtomicReference(Vector.empty)

  private val routeRunningGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val routeSuspendedGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val processorLifecycleGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val processorRestartGauges: ConcurrentHashMap[String, AtomicLong] = ConcurrentHashMap()
  private val processorRetryCounters: ConcurrentHashMap[String, Counter] = ConcurrentHashMap()
  private val tickFailureCounters: ConcurrentHashMap[String, Counter] = ConcurrentHashMap()
  private val leaseClaimCounters: ConcurrentHashMap[String, Counter] = ConcurrentHashMap()
  private val lagRefreshFailureCounters: ConcurrentHashMap[String, Counter] = ConcurrentHashMap()

  private val loopAttemptsCounter: Counter = Counter
    .builder("coordinator.loop.attempts")
    .description("Total main loop iterations")
    .register(registry)

  private val ingestInvocationsSuccess: Counter = Counter
    .builder("coordinator.ingest.ledger.invocations")
    .description("Total process_ingest_ledger invocations")
    .tag("outcome", "success")
    .register(registry)

  private val ingestInvocationsFailure: Counter = Counter
    .builder("coordinator.ingest.ledger.invocations")
    .description("Total process_ingest_ledger invocations")
    .tag("outcome", "failure")
    .register(registry)

  private val ingestProcessedCounter: Counter = Counter
    .builder("coordinator.ingest.processed")
    .description("Total events processed by ingest ledger")
    .register(registry)

  private val brokerCommittedCounter: Counter = Counter
    .builder("coordinator.broker.records.committed")
    .description("Total broker records durably handled and successfully committed")
    .register(registry)

  private val ingestDeduplicatedCounter: Counter = Counter
    .builder("coordinator.ingest.deduplicated")
    .description("Total events skipped as durable-ledger duplicates")
    .register(registry)

  private val batchesDispatchedCounter: Counter = Counter
    .builder("coordinator.batches.dispatched")
    .description("Total batches dispatched")
    .register(registry)

  private val heartbeatCounter: Counter = Counter
    .builder("coordinator.heartbeat")
    .description("Heartbeat counter")
    .register(registry)

  private val payloadAuditIngestedCounter: Counter = Counter
    .builder("coordinator.payload.audit.ingested")
    .description("Total payload audit records ingested")
    .register(registry)

  private val payloadAuditDlqCounter: Counter = Counter
    .builder("coordinator.payload.audit.dlq")
    .description("Total payload audit records sent to DLQ")
    .register(registry)

  private val syncEventsHydratedCounter: Counter = Counter
    .builder("coordinator.sync.events.hydrated")
    .description("Total sync event payloads hydrated into the durable ledger")
    .register(registry)

  private val syncEventsBackfillFailedCounter: Counter =
    Counter
      .builder("coordinator.sync.events.backfill.failed")
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
    pendingObservedAt.set(nowMillis())

  def recordBackpressureActive(active: Boolean): Unit =
    backpressureActiveGauge.set(if active then 1L else 0L)
    backpressureObservedAt.set(nowMillis())

  def incrementLoopCounter(): Unit =
    loopAttemptsCounter.increment()

  def recordIngestInvocation(success: Boolean): Unit =
    if success then ingestInvocationsSuccess.increment() else ingestInvocationsFailure.increment()
    if success then ingestLastSuccessTimestamp.set(nowMillis() / 1000)

  def recordIngestProcessed(count: Long): Unit =
    val now = nowMillis()
    if count > 0 then
      ingestProcessedCounter.increment(count.toDouble)
      processedSamples.updateAndGet { samples =>
        samples.filter(s => now - s._1 < 300_000L) :+ (now -> count)
      }: Unit
    // Empty successful processing passes are observations too.
    processedObservedAt.set(now)
    ()

  def recordIngestDeduplicated(count: Long = 1L): Unit =
    if count > 0 then ingestDeduplicatedCounter.increment(count.toDouble)

  def ingestProcessedRatePerSec(nowMs: Long): Double =
    processedSamples.get().iterator
      .filter { case (at, _) => at <= nowMs && nowMs - at < 300_000L }
      .map(_._2.toDouble).sum / 300.0

  // The scheduled ledger pass can be empty while locked consumers persist
  // scan/load/result or wireless records directly. Count only successful
  // broker commits, separately from the scheduled ledger counter.
  def recordBrokerRecordsCommitted(count: Long): Unit =
    if count > 0 then
      val now = nowMillis()
      brokerCommittedCounter.increment(count.toDouble)
      brokerProcessedSamples.updateAndGet { samples =>
        val recent = samples.filter(s => now - s._1 < 300_000L)
        // Coalesce commits in the same second to bound the five-minute window.
        val second = now / 1000L * 1000L
        if recent.exists(_._1 == second) then
          recent.map { case (at, records) => (at, if at == second then records + count else records) }
        else recent :+ (second -> count)
      }: Unit
      processedObservedAt.set(now)

  def brokerProcessedRatePerSec(nowMs: Long): Double =
    brokerProcessedSamples.get().iterator
      .filter { case (at, _) => at <= nowMs && nowMs - at < 300_000L }
      .map(_._2.toDouble).sum / 300.0

  // Fetch-position lag excludes records already fetched, including in-flight
  // batches. Keep it separate from the durable pending/processing ledger.
  def brokerLagCountValue(nowMs: Long): Option[Long] = synchronized {
    val samples = kafkaGauges.iterator.collect {
      case ((group, "coordinator.redpanda.consumer.lag.records", topic, partition), (value, _)) =>
        val observedAt = Option(lagSampleAt.get((group, topic, partition))).map(_.get())
        (value.get().doubleValue(), observedAt)
    }.toList
    Option.when(samples.nonEmpty && samples.forall { case (value, observedAt) =>
      value >= 0 && value < Long.MaxValue.toDouble && value == math.floor(value) &&
        observedAt.exists(at => at <= nowMs && nowMs - at <= 60_000L)
    }) {
      samples.map { case (value, _) => BigInt(value.toLong) }.sum
    }.filter(_.isValidLong).map(_.toLong)
  }

  def publicReadingsFresh(nowMs: Long): Boolean =
    def fresh(at: Long): Boolean = at > 0 && nowMs >= at && nowMs - at <= 60_000L
    nowMs - startedAt >= 300_000L &&
      fresh(pendingObservedAt.get()) && fresh(backpressureObservedAt.get()) && fresh(processedObservedAt.get())

  def pendingLedgerCountValue: Long = pendingLedgerGauge.get()

  def backpressureActiveValue: Boolean = backpressureActiveGauge.get() > 0

  def ingestLastSuccessEpochSeconds: Option[Long] =
    val ts = ingestLastSuccessTimestamp.get()
    if ts > 0 then Some(ts) else None

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

  def recordTickFailure(job: String): Unit =
    tickFailureCounters
      .computeIfAbsent(
        job,
        _ =>
          Counter
            .builder("coordinator.tick.failures")
            .tag("job", job)
            .description("Total failed periodic coordinator jobs")
            .register(registry)
      )
      .increment()

  def recordLagRefreshFailure(group: String): Unit =
    lagRefreshFailureCounters
      .computeIfAbsent(
        group,
        _ =>
          Counter
            .builder("coordinator.redpanda.lag.refresh.failures")
            .tag("consumer_group", group)
            .description("Total failed Redpanda lag metric refreshes")
            .register(registry)
      )
      .increment()

  def recordLeaseClaim(result: String): Unit =
    leaseClaimCounters
      .computeIfAbsent(
        result,
        _ =>
          Counter
            .builder("coordinator.lease.claims")
            .tag("result", result)
            .description("Total processor lease claim attempts by outcome")
            .register(registry)
      )
      .increment()

  def recordPostgresQuery(operation: String, outcome: String, duration: FiniteDuration): Unit =
    Timer
      .builder("coordinator.postgres.query.duration")
      .description("PostgreSQL durable operation duration")
      .tags("operation", operation, "outcome", outcome)
      .publishPercentileHistogram()
      .register(registry)
      .record(duration.toNanos, TimeUnit.NANOSECONDS)

  def recordLockedBatchDuration(topic: String, outcome: String, duration: FiniteDuration): Unit =
    Timer
      .builder("coordinator.locked.batch.duration")
      .description("Locked Kafka batch processing duration")
      .tags("topic", topic, "outcome", outcome)
      .publishPercentileHistogram()
      .register(registry)
      .record(duration.toNanos, TimeUnit.NANOSECONDS)

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
            .builder("coordinator.processor.retries")
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

  def scrape: String = registry.scrape("text/plain;version=0.0.4;charset=utf-8")

object CoordinatorMetrics:
  private val log = StructuredLogger(getClass)
  private val ProcessorLifecycleValues = List(
    "disabled",
    "starting",
    "ready",
    "backing_off",
    "failed_terminal"
  )

  def apply(): CoordinatorMetrics =
    new CoordinatorMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT))

  def withClock(nowMillis: () => Long): CoordinatorMetrics =
    new CoordinatorMetrics(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT), nowMillis)
