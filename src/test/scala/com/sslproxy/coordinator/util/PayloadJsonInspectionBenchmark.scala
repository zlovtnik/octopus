package com.sslproxy.coordinator.util

import java.lang.management.ManagementFactory
import scala.jdk.CollectionConverters.*

/** Standalone comparison; deliberately excluded from unit-test timing assertions.
  * Run with Test / runMain com.sslproxy.coordinator.util.PayloadJsonInspectionBenchmark.
  */
object PayloadJsonInspectionBenchmark:
  @volatile private var consumed = PayloadJsonInspection.Metadata(None)

  private def baseline(payload: String): PayloadJsonInspection.Metadata =
    val json = io.circe.parser.parse(payload).fold(throw _, identity)
    require(!json.isNull)
    PayloadJsonInspection.Metadata(
      json.hcursor.get[String]("event_type").toOption.filter(_.nonEmpty)
        .orElse(json.hcursor.get[String]("type").toOption.filter(_.nonEmpty))
    )

  private def streaming(payload: String): PayloadJsonInspection.Metadata =
    PayloadJsonInspection.inspect(payload).fold(throw _, identity)

  private final case class Sample(bytesPerOperation: Double, operationsPerSecond: Double)

  private def measure(
    payload: String,
    inspect: String => PayloadJsonInspection.Metadata,
    durationNanos: Long,
    bean: com.sun.management.ThreadMXBean
  ): Sample =
    val threadId = Thread.currentThread().threadId()
    val allocatedBefore = bean.getThreadAllocatedBytes(threadId)
    val start = System.nanoTime()
    var count = 0L
    while System.nanoTime() - start < durationNanos do
      consumed = inspect(payload)
      count += 1
    val elapsed = System.nanoTime() - start
    val allocated = bean.getThreadAllocatedBytes(threadId) - allocatedBefore
    Sample(allocated.toDouble / count, count.toDouble * 1e9 / elapsed)

  def main(args: Array[String]): Unit =
    require(args.isEmpty, "benchmark takes no arguments")
    val bean = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    require(bean.isThreadAllocatedMemorySupported, "thread allocation measurement is required")
    bean.setThreadAllocatedMemoryEnabled(true)
    val row = """{"id":123,"event_type":"nested","nested":{"enabled":true},"text":"abcdefghijklmnopqrstuvwxyz"}"""
    // Fixtures are constructed before either warmup or measurement.
    val fixtures = List(1, 8).map { mib =>
      val count = (mib * 1024 * 1024 - 2) / (row.length + 1)
      mib -> ("[" + List.fill(count)(row).mkString(",") + "]")
    }
    println(s"JDK=${System.getProperty("java.runtime.version")} JVM=${ManagementFactory.getRuntimeMXBean.getInputArguments}")
    println(s"collectors=${ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getName).mkString(",")} maxHeap=${Runtime.getRuntime.maxMemory()}")
    fixtures.foreach { (mib, payload) =>
      (1 to 4).foreach { _ =>
        measure(payload, baseline, 500_000_000L, bean): Unit
        measure(payload, streaming, 500_000_000L, bean): Unit
      }
      val samples = (1 to 5).toList.map { round =>
        // Alternate measurement order to reduce warmup and GC-order bias.
        val (before, after) =
          if round % 2 == 1 then
            val before = measure(payload, baseline, 1_000_000_000L, bean)
            before -> measure(payload, streaming, 1_000_000_000L, bean)
          else
            val after = measure(payload, streaming, 1_000_000_000L, bean)
            measure(payload, baseline, 1_000_000_000L, bean) -> after
        println(f"MiB=$mib round=$round bytes=${payload.length} baseline_B/op=${before.bytesPerOperation}%.0f streaming_B/op=${after.bytesPerOperation}%.0f baseline_ops/s=${before.operationsPerSecond}%.2f streaming_ops/s=${after.operationsPerSecond}%.2f")
        before -> after
      }
      val allocationRatios = samples.map((before, after) => after.bytesPerOperation / before.bytesPerOperation).sorted
      val throughputRatios = samples.map((before, after) => after.operationsPerSecond / before.operationsPerSecond).sorted
      println(f"MiB=$mib median_allocation_reduction_pct=${(1 - allocationRatios(2)) * 100}%.2f median_throughput_change_pct=${(throughputRatios(2) - 1) * 100}%.2f")
    }
    require(consumed.eventKind.isEmpty, "array roots must not extract nested event kinds")
