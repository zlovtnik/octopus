package com.sslproxy.coordinator.util

object ErrorSanitizer:
  private val MaximumLength = 512
  private val SecretAssignment =
    "(?i)(password|passwd|token|secret|authorization|api[-_]?key)\\s*[:=]\\s*([^\\s,;]+)".r
  private val ControlCharacters = "[\\p{Cc}\\p{Cf}]".r
  private val RepeatedWhitespace = "\\s+".r

  def message(error: Throwable): String =
    sanitize(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))

  def sanitize(value: String): String =
    val withoutControls = ControlCharacters.replaceAllIn(Option(value).getOrElse(""), " ")
    val redacted = SecretAssignment.replaceAllIn(withoutControls, matched =>
      s"${matched.group(1)}=[REDACTED]"
    )
    RepeatedWhitespace.replaceAllIn(redacted, " ").trim.take(MaximumLength)
