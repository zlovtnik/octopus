package com.sslproxy.coordinator.postgres

import io.circe.Json
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import scala.util.Try

/** Circe-based JSON field extraction helpers. Ported from JsonFields.java. */
object JsonFields:

  private val timestampParser: DateTimeFormatter =
    DateTimeFormatter.ISO_OFFSET_DATE_TIME

  def requiredString(row: Json, field: String, context: String): String =
    row.hcursor.get[String](field) match
      case Right(value) => value
      case Left(_) =>
        throw new IllegalArgumentException(
          s"Missing required string field '$field' in $context"
        )

  def optionalString(row: Json, field: String): Option[String] =
    row.hcursor.get[String](field).toOption.filter(_.nonEmpty)

  /** Present string, preserving empty values (e.g. hidden SSID); absent/null → None. */
  def presentString(row: Json, field: String): Option[String] =
    row.hcursor.get[String](field).toOption

  def requiredLong(row: Json, field: String, context: String): Long =
    row.hcursor.get[Long](field) match
      case Right(value) => value
      case Left(_) =>
        throw new IllegalArgumentException(
          s"Missing required long field '$field' in $context"
        )

  def optionalLong(row: Json, field: String): Option[Long] =
    row.hcursor.get[Long](field).toOption

  def optionalDouble(row: Json, field: String): Option[Double] =
    row.hcursor.get[Double](field).toOption

  def requiredTimestamp(row: Json, field: String, context: String): OffsetDateTime =
    row.hcursor.get[String](field) match
      case Right(value) =>
        parseTimestamp(value).getOrElse(
          throw new IllegalArgumentException(
            s"Invalid timestamp '$value' for field '$field' in $context"
          )
        )
      case Left(_) =>
        throw new IllegalArgumentException(
          s"Missing required timestamp field '$field' in $context"
        )

  def optionalTimestamp(row: Json, field: String): Option[OffsetDateTime] =
    row.hcursor.get[String](field).toOption.flatMap(parseTimestamp)

  /** Alias: try primary field, fall back to secondary. */
  def timestampAlias(
    row: Json,
    primary: String,
    secondary: String,
    context: String
  ): OffsetDateTime =
    optionalTimestamp(row, primary)
      .orElse(optionalTimestamp(row, secondary))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Missing timestamp: neither '$primary' nor '$secondary' in $context"
        )
      )

  /** Alias: try primary field, fall back to secondary. */
  def stringAlias(
    row: Json,
    primary: String,
    secondary: String,
    context: String
  ): String =
    optionalString(row, primary)
      .orElse(optionalString(row, secondary))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Missing string: neither '$primary' nor '$secondary' in $context"
        )
      )

  /** Alias: try primary long, fall back to secondary. */
  def longAlias(
    row: Json,
    primary: String,
    secondary: String,
    context: String
  ): Long =
    optionalLong(row, primary)
      .orElse(optionalLong(row, secondary))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Missing long: neither '$primary' nor '$secondary' in $context"
        )
      )

  def boolFlag(row: Json, field: String): Long =
    row.hcursor.get[Boolean](field).toOption match
      case Some(true) => 1L
      case _ =>
        row.hcursor.get[Long](field).toOption match
          case Some(v) if v != 0 => 1L
          case _ => 0L

  def boolValue(row: Json, field: String): Boolean =
    boolFlag(row, field) != 0L

  def nestedLong(row: Json, parent: String, field: String): Option[Long] =
    for
      parentObj <- row.hcursor.downField(parent).focus
      value <- parentObj.hcursor.get[Long](field).toOption
    yield value

  def nestedDouble(row: Json, parent: String, field: String): Option[Double] =
    for
      parentObj <- row.hcursor.downField(parent).focus
      value <- parentObj.hcursor.get[Double](field).toOption
    yield value

  def jsonArrayString(row: Json, field: String): Option[String] =
    row.hcursor.downField(field).focus.flatMap { value =>
      if value.isNull || !value.isArray then None
      else Some(value.noSpaces)
    }

  def rawJson(row: Json): Option[String] =
    if row.isNull then None
    else Some(row.noSpaces)

  def rowSequence(index: Int, context: String): Long =
    if index < 0 then
      throw new IllegalArgumentException(
        s"Negative row sequence index $index in $context"
      )
    index.toLong

  private def parseTimestamp(value: String): Option[OffsetDateTime] =
    Try(OffsetDateTime.parse(value, timestampParser)).toOption
