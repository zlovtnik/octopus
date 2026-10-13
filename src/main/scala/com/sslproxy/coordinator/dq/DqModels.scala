package com.sslproxy.coordinator.dq

import io.circe.{Json, JsonNumber, JsonObject, Printer}
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant

final case class DqSettings(
  installationId: String,
  namespace: String,
  scopeModes: Map[String, String],
  policyVersion: String,
  approvedDigests: Set[String],
  maxQueued: Int = 10000,
  leaseSeconds: Int = 60,
  artifactBucket: String = "dq-artifacts"
):
  def mode(scope: String): Option[String] =
    scopeModes.get(scope).filter(Set("shadow", "enabled"))

final case class DqPrincipal(
  actor: String,
  roles: Set[String],
  scopes: Set[String]
):
  def permits(scope: String): Boolean =
    scopes.contains("*") || scopes.contains(scope)
  def requireRole(allowed: Set[String]): Unit =
    if roles.intersect(allowed).isEmpty then throw DqError(403, "forbidden_role")

object DqPrincipal:
  val internal: DqPrincipal =
    DqPrincipal("octopus-runtime", Set("service"), Set("*"))

final case class DqOutbox(
  id: String,
  topic: String,
  key: String,
  payload: Json,
  owner: String,
  token: String,
  fence: Long
)
final case class DqError(status: Int, code: String) extends RuntimeException(code)

object DqJson:
  val DispatchPageBytes: Int = 1024 * 1024

  /** Keep the ordered prefix within the dispatch HTTP response budget. The
    * actual Circe HTTP encoding uses noSpaces; mutation/broker limits stay 64K.
    */
  def dispatchPage(items: List[Json]): Json =
    val (selected, _, _) = items.foldLeft((Vector.empty[Json], 12, true)) {
      case ((prefix, size, true), item) =>
        val next = size + item.noSpaces.getBytes(StandardCharsets.UTF_8).length + (if prefix.isEmpty then 0 else 1)
        if next <= DispatchPageBytes then (prefix :+ item, next, true)
        else if prefix.isEmpty then throw DqError(503, "dispatch_item_too_large")
        else (prefix, size, false)
      case (state, _) => state
    }
    Json.obj("items" -> Json.fromValues(selected))

  private val canonicalPrinter = Printer.noSpaces
  private def normalized(value: Json, depth: Int): Json =
    if depth > 128 then throw DqError(400, "json_depth_exceeded")
    value.asNumber match
      case Some(number) =>
        val decimal =
          number.toBigDecimal.getOrElse(throw DqError(400, "invalid_number")).bigDecimal.stripTrailingZeros()
        if decimal.scale().toLong.abs > 65536L || decimal.precision() > 65536 then
          throw DqError(413, "number_too_large")
        Json.fromJsonNumber(JsonNumber.fromDecimalStringUnsafe(decimal.toPlainString))
      case None =>
        value.asArray match
          case Some(values) => Json.fromValues(values.map(normalized(_, depth + 1)))
          case None =>
            value.mapObject { obj =>
              val ordered = obj.toIterable.toVector.sortWith { (left, right) =>
                java.util.Arrays.compare(left._1.codePoints().toArray, right._1.codePoints().toArray) < 0
              }
              JsonObject.fromIterable(ordered.map { case (key, item) => key -> normalized(item, depth + 1) })
            }
  def canonical(value: Json): String = canonicalPrinter
    .print(normalized(value, 0))
    .replace(new String(Character.toChars(0x2028)), "\\u2028")
    .replace(new String(Character.toChars(0x2029)), "\\u2029")
  def sha256(value: Json): String = digest(canonical(value))
  def digest(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"${byte & 0xff}%02x")
      .mkString
  def text(value: Json, field: String): String =
    value.hcursor
      .get[String](field)
      .fold(_ => throw DqError(400, "invalid_" + field), identity)
  def int(value: Json, field: String): Int =
    value.hcursor
      .get[Int](field)
      .fold(_ => throw DqError(400, "invalid_" + field), identity)
  def long(value: Json, field: String): Long =
    value.hcursor
      .get[Long](field)
      .fold(_ => throw DqError(400, "invalid_" + field), identity)
  def field(value: Json, name: String): Json =
    value.hcursor
      .downField(name)
      .focus
      .getOrElse(throw DqError(400, "missing_" + name))
  def optional(value: Json, name: String): Option[String] =
    value.hcursor.get[String](name).toOption
  def identifier(value: String): Boolean =
    value.matches("[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
  def hash(value: String): Boolean = value.matches("[0-9a-f]{64}")
  def scope(parameters: Json): String = text(parameters, "authorized_scope")
  def scoped(
    principal: DqPrincipal,
    request: Json,
    settings: DqSettings
  ): Unit =
    if text(request, "installation_id") != settings.installationId || text(
        request,
        "namespace"
      ) != settings.namespace
    then throw DqError(403, "forbidden_installation")
    val requested = optional(request, "authorized_scope").orElse(
      request.hcursor.downField("parameters").focus.map(scope)
    )
    requested.foreach(s => if !principal.permits(s) then throw DqError(403, "forbidden_scope"))
  def bounded(value: Json): Unit =
    if canonical(value).getBytes(StandardCharsets.UTF_8).length > 65536 then throw DqError(413, "record_too_large")
  def deadline(request: Json): Instant =
    optional(request, "deadline_at").fold(Instant.now().plusSeconds(3600)) { value =>
      scala.util
        .Try(Instant.parse(value))
        .getOrElse(throw DqError(400, "invalid_deadline"))
    }

  // The v1 registry deliberately supports this bounded, local JSON Schema vocabulary.
  // Unsupported validation keywords fail registration rather than weakening validation.
  private val schemaKeywords = Set(
    "$schema",
    "$id",
    "title",
    "description",
    "type",
    "properties",
    "required",
    "additionalProperties",
    "enum",
    "const",
    "minimum",
    "maximum",
    "minLength",
    "maxLength",
    "pattern",
    "items",
    "minItems",
    "maxItems",
    "default"
  )
  def checkSchema(schema: Json): Unit = checkSchemaAt(schema, 0)
  private def checkSchemaAt(schema: Json, depth: Int): Unit =
    val obj =
      schema.asObject.getOrElse(throw DqError(400, "invalid_parameter_schema"))
    if depth > 16 then throw DqError(400, "schema_depth_exceeded")
    if !obj.keys.forall(schemaKeywords) then throw DqError(400, "unsupported_schema_keyword")
    def reject(): Nothing = throw DqError(400, "invalid_parameter_schema")
    List("$schema", "$id", "title", "description").foreach { key =>
      obj(key).foreach(value => if !value.isString then reject())
    }
    obj("$schema").foreach { value =>
      if !value.asString.contains("https://json-schema.org/draft/2020-12/schema") then reject()
    }
    obj("type").foreach { value =>
      if !value.asString.exists(Set("object", "array", "string", "boolean", "integer", "number", "null")) then reject()
    }
    obj("additionalProperties").foreach(value => if !value.isBoolean then reject())
    obj("properties").foreach { value =>
      value.asObject.getOrElse(reject()).values.foreach(checkSchemaAt(_, depth + 1))
    }
    obj("required").foreach { value =>
      val names = value.asArray.getOrElse(reject())
      if !names.forall(_.isString) || names.distinct.size != names.size then reject()
    }
    obj("enum").foreach { value =>
      val values = value.asArray.getOrElse(reject())
      if values.isEmpty || values.distinct.size != values.size then reject()
    }
    List("minimum", "maximum").foreach { key =>
      obj(key).foreach(value => if value.asNumber.flatMap(_.toBigDecimal).isEmpty then reject())
    }
    List("minLength", "maxLength", "minItems", "maxItems").foreach { key =>
      obj(key).foreach(value => if !value.asNumber.flatMap(_.toInt).exists(_ >= 0) then reject())
    }
    obj("items").foreach(checkSchemaAt(_, depth + 1))
    obj("pattern").foreach(value => if !value.isString then reject())
    obj("pattern").flatMap(_.asString).foreach { value =>
      if value.length > 256 || scala.util.Try(value.r).isFailure then throw DqError(400, "invalid_schema_pattern")
    }
  def validate(schema: Json, value: Json): Unit =
    val c = schema.hcursor
    val validType = c.get[String]("type").toOption.forall {
      case "object" => value.isObject
      case "array" => value.isArray
      case "string" => value.isString
      case "boolean" => value.isBoolean
      case "integer" => value.asNumber.flatMap(_.toBigDecimal).exists(_.isWhole)
      case "number" => value.isNumber
      case "null" => value.isNull
      case _ => false
    }
    if !validType || c
        .get[Vector[Json]]("enum")
        .toOption
        .exists(values => !values.contains(value)) ||
      c.downField("const").focus.exists(_ != value)
    then throw DqError(400, "invalid_parameters")
    value.asObject.foreach { obj =>
      val required = c.get[Vector[String]]("required").getOrElse(Vector.empty)
      val properties = c
        .downField("properties")
        .focus
        .flatMap(_.asObject)
        .map(_.toMap)
        .getOrElse(Map.empty)
      if !required.forall(obj.contains) || (c
          .get[Boolean]("additionalProperties")
          .contains(false) &&
          !obj.keys.forall(properties.contains))
      then throw DqError(400, "invalid_parameters")
      properties.foreachEntry { (key, rule) =>
        obj(key).foreach(validate(rule, _))
      }
    }
    value.asString.foreach { str =>
      val length = str.codePointCount(0, str.length)
      if c.get[Int]("minLength").toOption.exists(length < _) || c
          .get[Int]("maxLength")
          .toOption
          .exists(length > _) ||
        c.get[String]("pattern")
          .toOption
          .exists(pattern => !pattern.r.findFirstIn(str).isDefined)
      then throw DqError(400, "invalid_parameters")
    }
    value.asNumber.flatMap(_.toBigDecimal).foreach { n =>
      if c.get[BigDecimal]("minimum")
          .toOption
          .exists(n < _) || c.get[BigDecimal]("maximum").toOption.exists(n > _)
      then throw DqError(400, "invalid_parameters")
    }
    value.asArray.foreach { values =>
      if c.get[Int]("minItems").toOption.exists(values.size < _) || c
          .get[Int]("maxItems")
          .toOption
          .exists(values.size > _)
      then throw DqError(400, "invalid_parameters")
      c.downField("items")
        .focus
        .foreach(rule => values.foreach(validate(rule, _)))
    }

  def validateArtifacts(
    refs: Json,
    settings: DqSettings,
    runId: String,
    attemptId: Option[String],
    input: Boolean
  ): Unit =
    val values = refs.asArray.getOrElse(throw DqError(400, "invalid_artifacts"))
    if values.size > 16 then throw DqError(400, "too_many_artifacts")
    values.foreach { ref =>
      scoped(DqPrincipal.internal, ref, settings)
      if !identifier(text(ref, "artifact_id")) || !hash(text(ref, "sha256")) ||
        text(ref, "version_id").isBlank || text(ref, "etag").isBlank || long(
          ref,
          "size_bytes"
        ) < 0
      then throw DqError(400, "invalid_artifact_reference")
      val prefix =
        "s3://" + settings.artifactBucket + "/" + settings.installationId + "/" + settings.namespace + "/"
      val uri = text(ref, "uri")
      val permitted =
        if input then prefix + "inputs/"
        else prefix + "runs/" + runId + "/attempts/" + attemptId.getOrElse("") + "/"
      if !uri.startsWith(permitted) || uri.contains("..") || uri.contains(
          "?"
        ) || uri.contains("#") || uri.contains("@")
      then throw DqError(403, "forbidden_artifact_reference")
      if !input && (optional(ref, "producer_run_id") != Some(runId) || optional(
          ref,
          "producer_attempt_id"
        ) != attemptId)
      then throw DqError(403, "forbidden_artifact_producer")
    }
