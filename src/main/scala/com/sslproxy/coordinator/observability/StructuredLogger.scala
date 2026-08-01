package com.sslproxy.coordinator.observability

import net.logstash.logback.argument.StructuredArguments
import org.slf4j.{Logger, LoggerFactory}

/** Type-safe structured logger that emits one JSON object per log line.
  *
  * Fields are passed as SLF4J structured key-value arguments via
  * Logstash StructuredArguments so the LogstashLogbackEncoder in
  * logback.xml renders them as top-level JSON properties.
  *
  * Field values are strings. Convert non-string values with `.toString`
  * before passing them to this logger.
  */
final class StructuredLogger private (underlying: Logger):

  def info(event: String, fields: (String, String)*): Unit =
    val args = Array[AnyRef](fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }: _*)
    underlying.info(event, args: _*)

  def warn(event: String, fields: (String, String)*): Unit =
    val args = Array[AnyRef](fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }: _*)
    underlying.warn(event, args: _*)

  def error(event: String, fields: (String, String)*): Unit =
    val args = Array[AnyRef](fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }: _*)
    underlying.error(event, args: _*)

  def debug(event: String, fields: (String, String)*): Unit =
    val args = Array[AnyRef](fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }: _*)
    underlying.debug(event, args: _*)

  def trace(event: String, fields: (String, String)*): Unit =
    val args = Array[AnyRef](fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }: _*)
    underlying.trace(event, args: _*)

  /** Error with cause — the Throwable is appended as the final vararg so
    * Logback attaches it as a real `stack_trace` field.
    */
  def error(event: String, cause: Throwable, fields: (String, String)*): Unit =
    val base = Array[AnyRef](fields.map { case (k, v) => StructuredArguments.keyValue(k, v) }: _*)
    val args = base :+ cause
    underlying.error(event, args: _*)

object StructuredLogger:
  def apply(clazz: Class[?]): StructuredLogger =
    new StructuredLogger(LoggerFactory.getLogger(clazz))

  def apply(name: String): StructuredLogger =
    new StructuredLogger(LoggerFactory.getLogger(name))
