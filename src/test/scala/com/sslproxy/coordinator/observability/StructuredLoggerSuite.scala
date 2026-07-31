package com.sslproxy.coordinator.observability

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Logger, LoggerContext}
import ch.qos.logback.core.OutputStreamAppender
import ch.qos.logback.core.encoder.Encoder
import io.circe.parser.parse
import munit.FunSuite
import net.logstash.logback.encoder.LogstashEncoder
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream

class StructuredLoggerSuite extends FunSuite:
  test("structured fields appear as top-level JSON properties") {
    val baos = new ByteArrayOutputStream()
    val ctx = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

    val encoder = new LogstashEncoder()
    encoder.setContext(ctx)
    encoder.start()

    val appender = new OutputStreamAppender[ILoggingEvent]()
    appender.setContext(ctx)
    appender.setEncoder(encoder.asInstanceOf[Encoder[ILoggingEvent]])
    appender.setOutputStream(baos)
    appender.start()

    val logger = LoggerFactory.getLogger("test.structured").asInstanceOf[Logger]
    logger.addAppender(appender)
    logger.setLevel(ch.qos.logback.classic.Level.INFO)

    val structuredLogger = StructuredLogger("test.structured")
    structuredLogger.info(
      "test_event",
      "status" -> "ok",
      "offset" -> "42",
      "table" -> "users",
      "error" -> "none"
    )

    appender.stop()

    val lines = new String(baos.toByteArray, "UTF-8").split('\n').filter(_.nonEmpty)
    assertEquals(lines.length, 1)

    val json = parse(lines(0)).getOrElse(throw new RuntimeException("invalid json"))
    assertEquals(json.hcursor.downField("status").as[String].toOption, Some("ok"))
    assertEquals(json.hcursor.downField("offset").as[String].toOption, Some("42"))
    assertEquals(json.hcursor.downField("table").as[String].toOption, Some("users"))
    assertEquals(json.hcursor.downField("error").as[String].toOption, Some("none"))
    assert(json.hcursor.downField("message").as[String].toOption.exists(_.contains("test_event")))
  }
