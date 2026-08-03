package com.sslproxy.coordinator.observability

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk

import java.util.concurrent.TimeUnit

object CoordinatorTracing:
  private val InstrumentationName = "com.sslproxy.octopus"

  val resource: Resource[IO, Unit] =
    Resource.make {
      IO.blocking(
        AutoConfiguredOpenTelemetrySdk.builder()
          .setResultAsGlobal()
          .build()
          .getOpenTelemetrySdk
      )
    } { sdk =>
      IO.blocking(sdk.getSdkTracerProvider.shutdown().join(10L, TimeUnit.SECONDS)).void
    }.void

  def span[A](
      name: String,
      kind: SpanKind,
      attributes: (String, String)*
  )(operation: IO[A]): IO[A] =
    IO {
      val builder = GlobalOpenTelemetry.get().getTracer(InstrumentationName)
        .spanBuilder(name)
        .setSpanKind(kind)
      attributes.foreach { (key, value) =>
        val _ = builder.setAttribute(key, value)
      }
      builder.startSpan()
    }.flatMap { span =>
      operation.onError { case error =>
        IO {
          val _ = span.recordException(error)
          val _ = span.setStatus(
            StatusCode.ERROR,
            Option(error.getMessage).getOrElse(error.getClass.getName)
          )
        }
      }.guarantee(IO(span.end()))
    }
