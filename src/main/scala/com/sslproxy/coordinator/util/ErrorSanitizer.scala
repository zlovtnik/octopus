package com.sslproxy.coordinator.util

object ErrorSanitizer:
  private val MaximumLength = 512
  private val SecretAssignment =
    "(?i)(password|passwd|token|secret|api[-_]?key)\\s*[:=]\\s*([^\\s,;]+)".r
  private val UriUserinfo = "://[^/]*?:([^@/]+)@".r
  private val BearerToken = "(?i)((?:authorization\\s*[:=]?\\s*)?Bearer\\s+)([A-Za-z0-9._\\-]+)".r
  private val ControlCharacters = "[\\p{Cc}\\p{Cf}]".r
  private val RepeatedWhitespace = "\\s+".r

  def message(error: Throwable): String =
    val sqlErrors =
      com.sslproxy.coordinator.postgres.PostgresErrorClass.exceptions(error, includeSuppressed = true).collect {
        case sql: java.sql.SQLException => sql
      }
    if sqlErrors.nonEmpty then
      val classification = com.sslproxy.coordinator.postgres.PostgresErrorClass.classify(error).wireValue
      sqlErrors
        .take(4)
        .map { sql =>
          val state = Option(sql.getSQLState).filter(_.matches("[A-Za-z0-9]{5}")).getOrElse("unknown")
          s"${sql.getClass.getSimpleName} SQLSTATE=$state vendor_code=${sql.getErrorCode} classification=$classification"
        }
        .mkString("; ")
        .take(MaximumLength)
    else sanitize(Option(error).flatMap(e => Option(e.getMessage)).getOrElse("Unknown error"))

  def sanitize(value: String): String =
    val withoutControls = ControlCharacters.replaceAllIn(Option(value).getOrElse(""), "")
    val withoutUserinfo = UriUserinfo.replaceAllIn(withoutControls, "://[REDACTED]@")
    val withoutBearer = BearerToken.replaceAllIn(withoutUserinfo, m => s"${m.group(1)}[REDACTED]")
    val redacted = SecretAssignment.replaceAllIn(withoutBearer, matched => s"${matched.group(1)}=[REDACTED]")
    RepeatedWhitespace.replaceAllIn(redacted, " ").trim.take(MaximumLength)
