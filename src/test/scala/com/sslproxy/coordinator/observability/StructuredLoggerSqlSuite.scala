package com.sslproxy.coordinator.observability

import ch.qos.logback.classic.{Logger, spi}
import ch.qos.logback.core.read.ListAppender
import java.sql.SQLException
import munit.FunSuite
import org.slf4j.LoggerFactory

class StructuredLoggerSqlSuite extends FunSuite:
  test("throwable logging cannot bypass SQL sanitization through a stack trace"):
    val name = "test.sql.sanitization"
    val logger = LoggerFactory.getLogger(name).asInstanceOf[Logger]
    val appender = new ListAppender[spi.ILoggingEvent]()
    appender.start()
    logger.addAppender(appender)
    try
      StructuredLogger(name).error("failed", RuntimeException("outer-secret", SQLException("INSERT synthetic-private", "57014")))
      val event = appender.list.get(0)
      assertEquals(event.getThrowableProxy, null)
      val rendered = event.getArgumentArray.mkString(" ")
      assert(rendered.contains("SQLSTATE=57014"))
      assert(!rendered.contains("outer-secret"))
      assert(!rendered.contains("synthetic-private"))
    finally
      logger.detachAppender(appender): Unit
      appender.stop()
