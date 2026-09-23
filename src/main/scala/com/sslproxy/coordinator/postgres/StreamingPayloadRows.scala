package com.sslproxy.coordinator.postgres

import com.fasterxml.jackson.core.{JsonFactory, JsonParser, JsonToken}
import io.circe.Json

/** Token scanning retains the source string and one row, never a payload-sized JSON tree.
  * The first pass also validates the complete document before any rows are written.
  */
private[postgres] object StreamingPayloadRows:
  private val factory = new JsonFactory()

  def validate(payload: String): Unit =
    val parser = factory.createParser(payload)
    try
      val first = parser.nextToken()
      require(first != null && first != JsonToken.VALUE_NULL, "payload_ref resolved an empty JSON payload")
      parser.skipChildren(): Unit
      require(parser.nextToken() == null, "payload_ref contains multiple JSON values")
    catch
      case error: com.fasterxml.jackson.core.JacksonException =>
        throw IllegalArgumentException("payload_ref resolved non-JSON payload", error)
    finally parser.close()

  def open(target: PostgresSinkTarget, payload: String, maxRowBytes: Int): Rows =
    validate(payload)
    new Rows(target, payload, maxRowBytes)

  final class Rows(target: PostgresSinkTarget, payload: String, maxRowBytes: Int)
      extends Iterator[Json] with AutoCloseable:
    private val arrayField = target match
      case PostgresSinkTarget.WirelessProbeRequests => Some("probes")
      case PostgresSinkTarget.WirelessClientInventory => Some("clients")
      case _ => None

    // Envelope fields can follow the row array. Scan for them without retaining that array.
    private val parents: List[(String, Json)] =
      if target != PostgresSinkTarget.WirelessClientInventory then Nil
      else
        val scan = factory.createParser(payload)
        try
          require(scan.nextToken() == JsonToken.START_OBJECT, "client inventory must be an object")
          var fields = Map.empty[String, Json]
          while scan.nextToken() != JsonToken.END_OBJECT do
            val name = scan.currentName()
            scan.nextToken(): Unit
            if Set("sensor_id", "location_id", "snapshot_at", "observed_at").contains(name) then
              fields = fields.updated(name, readRow(scan))
            else scan.skipChildren(): Unit
          List(
            fields.get("sensor_id").map("sensor_id" -> _),
            fields.get("location_id").map("location_id" -> _),
            fields.get("snapshot_at").orElse(fields.get("observed_at")).map("snapshot_at" -> _)
          ).flatten
        finally scan.close()

    private val parser = factory.createParser(payload)
    private var nextToken: JsonToken =
      try
        val first = parser.nextToken()
        arrayField match
          case None => if first == JsonToken.START_ARRAY then parser.nextToken() else first
          case Some(field) =>
            require(first == JsonToken.START_OBJECT, s"payload must contain a $field array")
            var found = false
            while !found && parser.nextToken() != JsonToken.END_OBJECT do
              val name = parser.currentName()
              parser.nextToken(): Unit
              if name == field then found = true else parser.skipChildren(): Unit
            require(found && parser.currentToken() == JsonToken.START_ARRAY, s"payload must contain a $field array")
            parser.nextToken()
      catch
        case error: Throwable =>
          parser.close()
          throw error

    def hasNext: Boolean = nextToken != null && nextToken != JsonToken.END_ARRAY

    def next(): Json =
      if !hasNext then throw new NoSuchElementException("payload rows exhausted")
      val row = readRow(parser)
      nextToken = parser.nextToken()
      if target == PostgresSinkTarget.WirelessClientInventory then
        val obj = row.asObject.getOrElse(throw IllegalArgumentException("each client must be a JSON object"))
        Json.fromJsonObject(parents.foldLeft(obj) { case (value, (key, field)) => value.add(key, field) })
      else row

    private def readRow(input: JsonParser): Json =
      val start = input.currentTokenLocation().getCharOffset.toInt
      input.skipChildren(): Unit
      // String tokens are lazily completed by Jackson.
      input.finishToken()
      val end = input.currentLocation().getCharOffset.toInt
      require(end - start <= maxRowBytes, s"PostgreSQL row exceeds $maxRowBytes bytes")
      val text = payload.substring(start, end)
      require(text.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= maxRowBytes,
        s"PostgreSQL row exceeds $maxRowBytes bytes")
      io.circe.parser.parse(text).fold(error => throw IllegalArgumentException("invalid payload row", error), identity)

    def close(): Unit = parser.close()
