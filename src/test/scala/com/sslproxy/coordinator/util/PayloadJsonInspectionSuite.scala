package com.sslproxy.coordinator.util

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken, StreamReadConstraints}
import com.fasterxml.jackson.core.util.JsonParserDelegate
import munit.FunSuite

class PayloadJsonInspectionSuite extends FunSuite:
  private def circeKind(payload: String): Option[String] =
    val value = io.circe.parser.parse(payload).fold(throw _, identity)
    value.hcursor.get[String]("event_type").toOption.filter(_.nonEmpty)
      .orElse(value.hcursor.get[String]("type").toOption.filter(_.nonEmpty))

  private val documents = List(
    "{}",
    """{"event_type":"primary","type":"fallback"}""",
    """{"type":"fallback","event_type":"primary"}""",
    """{"event_type":"old","event_type":"new"}""",
    """{"event_type":"old","event_type":null,"type":"fallback"}""",
    """{"event_type":"old","event_type":"","type":"fallback"}""",
    """{"event_type":{},"type":"old","type":"new"}""",
    """{"event_type":[],"type":12}""",
    """{"event_type":true,"type":null}""",
    """{"event_type":"","type":""}""",
    """{"event_type":"  ","type":"fallback"}""",
    """{"event_type":"line\nquote\"slash\\\u263a","type":"fallback"}""",
    """{"event_\u0074ype":"escaped key"}""",
    """{"nested":{"event_type":"nested"},"rows":[{"type":"nested"}],"type":"top"}""",
    """{"event_type":{"event_type":"nested"},"type":"fallback"}""",
    "[]", """[{"event_type":"nested"},null,[],1,"text"]""",
    "true", "false", "123", "-1.25e+3", """"event_type""""
  )

  documents.zipWithIndex.foreach { (payload, index) =>
    test(s"metadata matches Circe for document $index") {
      assertEquals(PayloadJsonInspection.inspect(payload), Right(PayloadJsonInspection.Metadata(circeKind(payload))))
    }
  }

  test("duplicate empty and wrong-type fields clear earlier values") {
    for
      field <- List("event_type", "type")
      replacement <- List("null", "false", "1", "[]", "{}", "\"\"")
    do
      val payload = s"""{"$field":"old","$field":$replacement}"""
      assertEquals(PayloadJsonInspection.inspect(payload).map(_.eventKind), Right(circeKind(payload)))
      assertEquals(PayloadJsonInspection.inspect(payload).map(_.eventKind), Right(None))
  }

  test("metadata strings retain the previous skip-based validation limits") {
    val kind = "k" * (StreamReadConstraints.defaults().getMaxStringLength + 1)
    val payload = s"""{"event_type":"$kind"}"""
    scala.util.Using.resource(new JsonFactory().createParser(payload)) { parser =>
      assertEquals(parser.nextToken(), JsonToken.START_OBJECT)
      parser.skipChildren(): Unit
      assertEquals(parser.nextToken(), null)
    }
    assertEquals(PayloadJsonInspection.inspect(payload).map(_.eventKind), Right(circeKind(payload)))
  }

  test("Jackson nesting and numeric limits match the previous validator") {
    val constraints = StreamReadConstraints.defaults()
    val depth = constraints.getMaxNestingDepth
    val digits = constraints.getMaxNumberLength
    List(
      "[" * depth + "0" + "]" * depth,
      "[" * (depth + 1) + "0" + "]" * (depth + 1),
      "[" + "9" * digits + "]",
      "[" + "9" * (digits + 1) + "]"
    ).foreach { payload =>
      val previouslyAccepted = scala.util.Try {
        scala.util.Using.resource(new JsonFactory().createParser(payload)) { parser =>
          parser.nextToken(): Unit
          parser.skipChildren(): Unit
          require(parser.nextToken() == null)
        }
      }.isSuccess
      assertEquals(PayloadJsonInspection.inspect(payload).isRight, previouslyAccepted)
    }
  }

  test("rejects empty, null, malformed tails and additional roots like Circe") {
    List(
      "", " \n\t", "null", " null ", "[1,]", "{", "[{}", "{} x", "{} {}", "[] null",
      "true false", "1 2", "\"ok\" []", "\"unterminated", "\"bad\\x\"",
      """{"event_type":"valid","tail":[1,broken]}""",
      """{"event_type":"valid","tail":"bad\x"}""",
      """{"event_type":"valid"} trailing-secret"""
    ).foreach { payload =>
      assert(io.circe.parser.parse(payload).fold(_ => true, _.isNull), clues(payload))
      val result = PayloadJsonInspection.inspect(payload)
      assert(result.isLeft, clues(payload))
      assertEquals(result.left.toOption.map(_.getMessage), Some("payload_ref resolved non-JSON payload"))
    }
  }

  test("closes the parser on success and syntax failure") {
    List("{}", "[1,broken]", "null", "{} {}").foreach { payload =>
      val parser = new JsonFactory().createParser(payload)
      PayloadJsonInspection.inspectParser(() => parser): Unit
      assert(parser.isClosed)
    }
  }

  test("parser creation, token and close failures are sanitized and parsers are closed") {
    val secret = "private-parser-detail"
    val creation = PayloadJsonInspection.inspectParser(() => throw IllegalStateException(secret))
    assertEquals(creation.left.toOption.map(_.getMessage), Some("payload_ref resolved non-JSON payload"))
    List(false, true).foreach { failOnClose =>
      var closed = false
      val parser: JsonParser = new JsonParserDelegate(new JsonFactory().createParser("{}")):
        override def nextToken(): JsonToken =
          if failOnClose then super.nextToken() else throw IllegalStateException(secret)
        override def close(): Unit =
          closed = true
          super.close()
          if failOnClose then throw IllegalStateException(secret)
      val result = PayloadJsonInspection.inspectParser(() => parser)
      assert(closed)
      assert(parser.isClosed)
      assertEquals(result.left.toOption.map(_.getMessage), Some("payload_ref resolved non-JSON payload"))
    }
  }
