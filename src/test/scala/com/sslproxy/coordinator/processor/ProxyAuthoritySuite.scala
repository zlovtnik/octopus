package com.sslproxy.coordinator.processor

import io.circe.generic.auto.*
import io.circe.parser.parse
import io.circe.syntax.*
import munit.FunSuite
import scala.io.Source

class ProxyAuthoritySuite extends FunSuite:
  private val resource = Source.fromResource("contracts/proxy-authority-v1.json")
  private val corpus = try parse(resource.mkString).fold(throw _, identity) finally resource.close()
  private val cases = corpus.hcursor.downField("cases").focus.flatMap(_.asArray).getOrElse(fail("missing corpus"))
  assert(cases.size >= 60)
  test("userinfo is discarded before sanitization or source classification") {
    for
      raw <- List("user:secret@example.com:443", "hmac:user@example.com", "@example.com")
      sanitized <- List(false, true)
      source <- List("query", "unknown")
    do
      val got = ProxyAuthority.normalize(Some(raw), source, sanitized)
      assertEquals(got.authority_raw, None)
      assertEquals(got.host_raw, None)
      assertEquals(got.parse_status, "invalid")
  }
  cases.foreach { fixture =>
    val c = fixture.hcursor
    test(c.get[String]("id").toOption.getOrElse("missing id")) {
      val input = c.downField("input")
      val raw = input.get[Option[String]]("authority").fold(throw _, identity)
      val source = input.get[String]("source").fold(throw _, identity)
      val sanitized = input.get[Boolean]("sanitized").fold(throw _, identity)
      val expected = c.downField("expected").focus.getOrElse(fail("missing expected"))
      val got = ProxyAuthority.normalize(raw, source, sanitized)
      assertEquals(got.asJson, expected)
      assertEquals(ProxyAuthority.normalize(raw, source, sanitized), got)
    }
  }
