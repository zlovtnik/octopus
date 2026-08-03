package com.sslproxy.coordinator.persistence

import java.sql.PreparedStatement

final case class BatchStatement[A](
    name: String,
    sql: String,
    bind: (PreparedStatement, A) => Unit
):
  require(name.trim.nonEmpty, "batch statement name must not be blank")
  require(sql.trim.nonEmpty, "batch statement SQL must not be blank")

object BatchStatement:
  def validateBatch[A](rows: List[A], maxBatchSize: Int): Either[String, List[A]] =
    if maxBatchSize <= 0 then Left("max batch size must be positive")
    else if rows.size > maxBatchSize then
      Left(s"batch contains ${rows.size} rows but maximum is $maxBatchSize")
    else Right(rows)
