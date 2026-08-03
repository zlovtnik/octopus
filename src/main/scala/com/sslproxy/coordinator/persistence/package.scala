package com.sslproxy.coordinator

import cats.data.EitherT
import com.sslproxy.coordinator.domain.DatabaseError

package object persistence:
  type DbResultT[F[_], A] = EitherT[F, DatabaseError, A]
