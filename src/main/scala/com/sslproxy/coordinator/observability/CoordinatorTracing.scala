package com.sslproxy.coordinator.observability

import cats.data.{Kleisli, OptionT}
import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator
import io.opentelemetry.api.trace.{SpanKind, StatusCode, Tracer}
import io.opentelemetry.api.{GlobalOpenTelemetry, OpenTelemetry}
import io.opentelemetry.context.Context
import io.opentelemetry.context.propagation.TextMapGetter
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeaders
import org.http4s.{HttpRoutes, Request}
import org.slf4j.MDC

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

object CoordinatorTracing:
  private val InstrumentationName = "com.sslproxy.octopus"
  private val otelRef = new AtomicReference[OpenTelemetry](GlobalOpenTelemetry.get())
  private val UntracedPaths = Set("/live", "/metrics", "/actuator/prometheus")

  enum Attr:
    case S(value: String)
    case L(value: Long)
    case D(value: Double)
    case B(value: Boolean)

  val resource: Resource[IO, Unit] =
    Resource.make {
      IO.blocking {
        val sdk = AutoConfiguredOpenTelemetrySdk
          .builder()
          .setResultAsGlobal()
          .build()
          .getOpenTelemetrySdk
        otelRef.set(sdk)
        sdk
      }
    } { sdk =>
      IO.blocking {
        sdk.getSdkTracerProvider.shutdown().join(10L, TimeUnit.SECONDS)
        sdk.close()
      }.void
    }.void

  private[observability] def installForTest(otel: OpenTelemetry): Unit = otelRef.set(otel)

  private def tracer: Tracer = otelRef.get().getTracer(InstrumentationName)

  def span[A](
    name: String,
    kind: SpanKind,
    attributes: (String, Attr)*
  )(operation: IO[A]): IO[A] =
    spanWithParent(name, kind, None, attributes*)(operation)

  def spanWithParent[A](
    name: String,
    kind: SpanKind,
    parent: Option[Context],
    attributes: (String, Attr)*
  )(operation: IO[A]): IO[A] =
    IO {
      val builder = tracer
        .spanBuilder(name)
        .setSpanKind(kind)
      parent.foreach { p =>
        val _ = builder.setParent(p)
      }
      attributes.foreach {
        case (key, Attr.S(value)) => val _ = builder.setAttribute(key, value)
        case (key, Attr.L(value)) => val _ = builder.setAttribute(key, value)
        case (key, Attr.D(value)) => val _ = builder.setAttribute(key, value)
        case (key, Attr.B(value)) => val _ = builder.setAttribute(key, value)
      }
      builder.startSpan()
    }.flatMap { span =>
      val scope = span.makeCurrent()
      IO {
        val ctx = span.getSpanContext
        MDC.put("trace_id", ctx.getTraceId)
        MDC.put("span_id", ctx.getSpanId)
      } *> operation
        .onError { case error =>
          IO {
            val _ = span.recordException(error)
            val _ = span.setStatus(
              StatusCode.ERROR,
              Option(error.getMessage).getOrElse(error.getClass.getName)
            )
          }
        }
        .guarantee(IO {
          MDC.remove("trace_id")
          MDC.remove("span_id")
          scope.close()
          span.end()
        })
    }

  def serverMiddleware(routes: HttpRoutes[IO]): HttpRoutes[IO] =
    Kleisli { (req: Request[IO]) =>
      val path = req.uri.path.renderString
      if UntracedPaths.contains(path) then routes(req)
      else
        OptionT {
          IO {
            val span = tracer
              .spanBuilder(s"HTTP ${req.method.name} $path")
              .setSpanKind(SpanKind.SERVER)
              .setAttribute("http.request.method", req.method.name)
              .setAttribute("url.path", path)
              .startSpan()
            val scope = span.makeCurrent()
            (span, scope)
          }.flatMap { case (span, scope) =>
            routes(req).value.attempt.flatMap {
              case Right(Some(response)) =>
                IO {
                  val _ = span.setAttribute("http.response.status_code", response.status.code.toLong)
                  if response.status.code >= 500 then {
                    val _ = span.setStatus(StatusCode.ERROR)
                  }
                  scope.close()
                  span.end()
                }.as(Some(response))
              case Right(None) =>
                IO {
                  scope.close()
                  span.end()
                }.as(None)
              case Left(error) =>
                IO {
                  val _ = span.recordException(error)
                  val _ = span.setStatus(
                    StatusCode.ERROR,
                    Option(error.getMessage).getOrElse(error.getClass.getName)
                  )
                  scope.close()
                  span.end()
                } *> IO.raiseError(error)
            }
          }
        }
    }

  private val kafkaHeaderGetter: TextMapGetter[java.lang.Iterable[Header]] =
    new TextMapGetter[java.lang.Iterable[Header]]:
      def keys(carrier: java.lang.Iterable[Header]): java.lang.Iterable[String] =
        val keys = new java.util.ArrayList[String]()
        carrier.forEach { h =>
          val _ = keys.add(h.key())
        }
        keys
      def get(carrier: java.lang.Iterable[Header], key: String): String =
        var found: String = null
        carrier.forEach { h =>
          if h.key() == key then found = new String(h.value(), StandardCharsets.UTF_8)
        }
        found

  def extractParent(headers: java.lang.Iterable[Header]): Context =
    W3CTraceContextPropagator.getInstance().extract(Context.current(), headers, kafkaHeaderGetter)

  def injectCurrent(headers: RecordHeaders): Unit =
    W3CTraceContextPropagator.getInstance().inject(
      Context.current(),
      headers,
      (carrier: RecordHeaders, key: String, value: String) => {
        val _ = carrier.add(key, value.getBytes(StandardCharsets.UTF_8))
      }
    )
