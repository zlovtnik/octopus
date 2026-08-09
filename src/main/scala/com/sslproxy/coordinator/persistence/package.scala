package com.sslproxy.coordinator

import cats.data.EitherT
import cats.MonadThrow
import cats.syntax.all.*
import com.sslproxy.coordinator.domain.DatabaseError
import com.sslproxy.coordinator.util.ErrorSanitizer

package object persistence:
  type DbResultT[F[_], A] = EitherT[F, DatabaseError, A]

  final case class DatabaseOperationException(error: DatabaseError)
      extends RuntimeException(
        s"${error.operation}: ${ErrorSanitizer.sanitize(error.message)}",
        RuntimeException(ErrorSanitizer.message(error.cause))
      )

  extension [F[_]: MonadThrow, A](result: DbResultT[F, A])
    def orRaise: F[A] =
      result.value.flatMap(
        _.fold(
          error => MonadThrow[F].raiseError(DatabaseOperationException(error)),
          MonadThrow[F].pure
        )
      )
