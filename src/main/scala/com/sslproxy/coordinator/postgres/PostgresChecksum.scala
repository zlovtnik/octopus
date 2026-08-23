package com.sslproxy.coordinator.postgres

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** SHA-256 checksum construction. */
object PostgresChecksum:

  def checksum(target: PostgresSinkTarget, payload: String): String =
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(target.checksumTag.getBytes(StandardCharsets.UTF_8))
    digest.update(0.toByte)
    digest.update(payload.getBytes(StandardCharsets.UTF_8))
    digest.digest().map("%02x".format(_)).mkString