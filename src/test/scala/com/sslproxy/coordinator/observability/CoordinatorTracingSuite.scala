package com.sslproxy.coordinator.observability

import cats.effect.IO
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter
import io.opentelemetry.sdk.trace.SdkTracerProvider
import io.opentelemetry.sdk.trace.`export`.SimpleSpanProcessor
import munit.CatsEffectSuite
import org.apache.kafka.common.header.internals.RecordHeaders
import org.slf4j.MDC

class CoordinatorTracingSuite extends CatsEffectSuite:
  private def withInMemorySdk[A](body: InMemorySpanExporter => IO[A]): IO[A] =
    IO {
      val exporter = InMemorySpanExporter.create()
      val provider = SdkTracerProvider.builder()
        .addSpanProcessor(SimpleSpanProcessor.create(exporter))
        .build()
      val sdk = OpenTelemetrySdk.builder().setTracerProvider(provider).build()
      CoordinatorTracing.installForTest(sdk)
      (exporter, sdk)
    }.flatMap { case (exporter, sdk) =>
      body(exporter).guarantee(IO {
        sdk.close()
        CoordinatorTracing.installForTest(OpenTelemetry.noop())
      })
    }

  test("span works before the SDK resource is acquired (no-op tracer)") {
    // Regression: class init must not call GlobalOpenTelemetry.get(), which
    // claims the singleton and makes setResultAsGlobal abort startup.
    CoordinatorTracing.span("test.noop", SpanKind.INTERNAL)(IO.unit).as(())
  }

  test("tracing resource acquires without claiming GlobalOpenTelemetry") {
    CoordinatorTracing.resource.use(_ => IO.unit)
  }

  test("span records typed attributes and ends successfully") {
    withInMemorySdk { exporter =>
      CoordinatorTracing.span(
        "test.span",
        SpanKind.INTERNAL,
        "str" -> CoordinatorTracing.Attr.S("value"),
        "num" -> CoordinatorTracing.Attr.L(42L),
        "flag" -> CoordinatorTracing.Attr.B(true)
      )(IO.unit) *> IO {
        val spans = exporter.getFinishedSpanItems
        assertEquals(spans.size(), 1)
        val span = spans.get(0)
        assertEquals(span.getName, "test.span")
        assertEquals(span.getKind, SpanKind.INTERNAL)
        assertEquals(span.getStatus.getStatusCode, StatusCode.UNSET)
        assertEquals(span.getAttributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("str")), "value")
        assertEquals(
          span.getAttributes.get(io.opentelemetry.api.common.AttributeKey.longKey("num")).longValue(),
          42L
        )
        assertEquals(
          span.getAttributes.get(io.opentelemetry.api.common.AttributeKey.booleanKey("flag")).booleanValue(),
          true
        )
      }
    }
  }

  test("failed spans record exception and ERROR status") {
    withInMemorySdk { exporter =>
      CoordinatorTracing
        .span("test.error", SpanKind.INTERNAL)(IO.raiseError(RuntimeException("boom")))
        .attempt *> IO {
          val spans = exporter.getFinishedSpanItems
          assertEquals(spans.size(), 1)
          val span = spans.get(0)
          assertEquals(span.getStatus.getStatusCode, StatusCode.ERROR)
          assert(span.getEvents.size() >= 1)
        }
    }
  }

  test("MDC carries trace_id and span_id during the span and is cleared after") {
    withInMemorySdk { _ =>
      for
        during <- CoordinatorTracing.span("test.mdc", SpanKind.INTERNAL) {
          IO(Option(MDC.get("trace_id")) -> Option(MDC.get("span_id")))
        }
        after <- IO(Option(MDC.get("trace_id")) -> Option(MDC.get("span_id")))
      yield
        assert(during._1.exists(_.nonEmpty))
        assert(during._2.exists(_.nonEmpty))
        assertEquals(after, (None, None))
    }
  }

  test("W3C inject and extract round-trip through Kafka headers") {
    withInMemorySdk { _ =>
      CoordinatorTracing.span("test.propagate", SpanKind.PRODUCER) {
        IO {
          val headers = new RecordHeaders()
          CoordinatorTracing.injectCurrent(headers)
          headers
        }
      }.flatMap { headers =>
        IO {
          val parent = CoordinatorTracing.extractParent(headers)
          assert(parent != null)
          val spanContext = io.opentelemetry.api.trace.Span.fromContext(parent).getSpanContext
          assert(spanContext.isValid)
        }
      }
    }
  }
