package com.sslproxy.coordinator.tidb

import cats.effect.{IO, Resource}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Logger, LoggerContext}
import ch.qos.logback.core.OutputStreamAppender
import ch.qos.logback.core.encoder.Encoder
import io.circe.parser.parse
import munit.CatsEffectSuite
import net.logstash.logback.encoder.LogstashEncoder
import org.slf4j.LoggerFactory

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.Base64

class TidbLoadHandlerSuite extends CatsEffectSuite:

  test("database insert failure logs a structured error with its stack trace") {
    val output = new ByteArrayOutputStream()
    val context = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val encoder = new LogstashEncoder()
    encoder.setContext(context)
    encoder.start()

    val appender = new OutputStreamAppender[ILoggingEvent]()
    appender.setContext(context)
    appender.setEncoder(encoder.asInstanceOf[Encoder[ILoggingEvent]])
    appender.setOutputStream(output)

    val logger = LoggerFactory
      .getLogger("com.sslproxy.coordinator.tidb.TidbLoadHandler$")
      .asInstanceOf[Logger]
    val originalAdditivity = logger.isAdditive
    val cause = new RuntimeException("database insert unavailable")
    val handler = new TidbLoadHandler(
      new TidbPayloadResolver("/tmp"),
      TidbTransformService,
      new FailingProxyEventSink(cause),
      TidbClock,
      _ => IO.pure(None)
    )

    Resource
      .make(IO {
        logger.setAdditive(false)
        logger.addAppender(appender)
        appender.start()
      })(_ => IO {
        logger.detachAppender(appender)
        appender.stop()
        logger.setAdditive(originalAdditivity)
        encoder.stop()
      })
      .use(_ => handler.handle(proxyEventsLoad))
      .map { result =>
        assertEquals(result.status, "failed")
        assertEquals(result.errorText, "database insert unavailable")

        val records = new String(output.toByteArray, StandardCharsets.UTF_8)
          .split('\n')
          .iterator
          .filter(_.nonEmpty)
          .map(line => parse(line).getOrElse(fail(s"invalid JSON log: $line")))
          .toList
        val error = records.find(
          _.hcursor.downField("level").as[String].toOption.contains("ERROR")
        ).getOrElse(fail("expected an error-level tidb_load log"))

        assertEquals(error.hcursor.downField("message").as[String].toOption, Some("tidb_load"))
        assertEquals(error.hcursor.downField("status").as[String].toOption, Some("insert_failed"))
        assertEquals(error.hcursor.downField("batch_id").as[String].toOption, Some("batch-1"))
        assertEquals(error.hcursor.downField("stream_name").as[String].toOption, Some("proxy.events"))
        assertEquals(error.hcursor.downField("error_class").as[String].toOption, Some("permanent"))
        assert(error.hcursor.downField("stack_trace").as[String].toOption.exists(
          _.contains("database insert unavailable")
        ))
      }
  }

  private def proxyEventsLoad: TidbLoad =
    val payload = """[{"type":"tls_scan","host":"example.com","time":"2026-07-20T12:00:00Z","blocked":false}]"""
    val payloadRef = "inline://json/" + Base64.getUrlEncoder.withoutPadding
      .encodeToString(payload.getBytes(StandardCharsets.UTF_8))
    TidbLoad("job-1", "batch-1", None, "proxy.events", payloadRef, "", "", 0)

  private final class FailingProxyEventSink(cause: Throwable) extends TidbSink:
    override def insertProxyEvents(
        _batchId: String,
        _rows: List[ProxyEventInsert],
        _blockedRows: List[BlockedEventInsert]
    ): IO[Long] = IO.raiseError(cause)

    override def insertProxyPayloadAudit(_batchId: String, _rows: List[ProxyPayloadAuditInsert]): IO[Long] = unexpected

    override def insertWirelessAuditFrames(_batchId: String, _rows: List[WirelessAuditFrameInsert]): IO[Long] = unexpected

    override def insertWirelessBandwidth(_batchId: String, _rows: List[WirelessBandwidthInsert]): IO[Long] = unexpected

    override def insertWirelessRogueAp(_batchId: String, _rows: List[WirelessRogueApInsert]): IO[Long] = unexpected

    override def insertWirelessDeauthFlood(_batchId: String, _rows: List[WirelessDeauthFloodInsert]): IO[Long] = unexpected

    override def insertWirelessSignalAnomaly(_batchId: String, _rows: List[WirelessSignalAnomalyInsert]): IO[Long] = unexpected

    override def insertWirelessPmfAttack(_batchId: String, _rows: List[WirelessPmfAttackInsert]): IO[Long] = unexpected

    override def insertWirelessClientInventory(_batchId: String, _rows: List[WirelessClientInventoryInsert]): IO[Long] = unexpected

    override def insertWirelessProbeRequests(_batchId: String, _rows: List[WirelessProbeRequestInsert]): IO[Long] = unexpected

    override def insertWirelessAttackSequence(_batchId: String, _rows: List[WirelessAttackSequenceInsert]): IO[Long] = unexpected

    override def insertWirelessSequenceAlert(_batchId: String, _rows: List[WirelessSequenceAlertInsert]): IO[Long] = unexpected

    override def insertWirelessHandshakeAlert(_batchId: String, _rows: List[WirelessHandshakeAlertInsert]): IO[Long] = unexpected

    private def unexpected: IO[Long] =
      IO.raiseError(IllegalStateException("unexpected sink target"))
