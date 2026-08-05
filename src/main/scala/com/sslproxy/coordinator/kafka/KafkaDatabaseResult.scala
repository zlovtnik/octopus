package com.sslproxy.coordinator.kafka

import cats.effect.IO
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.util.ErrorSanitizer

private[kafka] object KafkaDatabaseResult:
  def require[A](effect: IO[Either[DatabaseError, A]]): IO[A] =
    effect.flatMap {
      case Right(value) => IO.pure(value)
      case Left(error)  => IO.raiseError(
        KafkaDatabaseWriteFailure(error.operation, error.message, error.cause)
      )
    }

private final case class KafkaDatabaseWriteFailure(
    operation: String,
    detail: String,
    underlying: Throwable
) extends RuntimeException(
      s"durable Kafka handler failed during $operation: ${ErrorSanitizer.sanitize(Option(detail).getOrElse(""))}",
      underlying
    )
