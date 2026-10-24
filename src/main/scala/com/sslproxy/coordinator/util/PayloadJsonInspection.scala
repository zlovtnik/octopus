package com.sslproxy.coordinator.util

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken, StreamReadConstraints}

import scala.util.Using
import scala.util.control.NonFatal

/** Validates a complete non-null document without retaining its body or rows. */
object PayloadJsonInspection:
  final case class Metadata(eventKind: Option[String])

  // The previous validation pass skipped strings, which does not enforce Jackson's
  // materialized-string limit. Reading event metadata must not add that limit.
  private val factory = new JsonFactory().setStreamReadConstraints(
    StreamReadConstraints.defaults().rebuild().maxStringLength(Int.MaxValue).build()
  )

  def inspect(payload: String): Either[Throwable, Metadata] =
    inspectParser(() => factory.createParser(payload))

  // Owns the supplied parser, including failure paths and lazy string tokens.
  private[util] def inspectParser(createParser: () => JsonParser): Either[Throwable, Metadata] =
    try
      Right(Using.resource(createParser()) { parser =>
        val first = parser.nextToken()
        require(first != null && first != JsonToken.VALUE_NULL, "payload_ref resolved an empty JSON payload")
        var eventType = Option.empty[String]
        var fallbackType = Option.empty[String]
        if first == JsonToken.START_OBJECT then
          while parser.nextToken() != JsonToken.END_OBJECT do
            val name = parser.currentName()
            val token = parser.nextToken()
            if name == "event_type" || name == "type" then
              val value =
                if token == JsonToken.VALUE_STRING then Option(parser.getText).filter(_.nonEmpty)
                else None
              if name == "event_type" then eventType = value else fallbackType = value
            parser.skipChildren(): Unit
        else parser.skipChildren(): Unit
        require(parser.nextToken() == null, "payload_ref contains multiple JSON values")
        Metadata(eventType.orElse(fallbackType))
      })
    catch
      case NonFatal(error) =>
        Left(IllegalArgumentException("payload_ref resolved non-JSON payload", error))
