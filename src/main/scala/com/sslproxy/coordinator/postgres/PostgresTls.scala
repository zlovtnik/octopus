package com.sslproxy.coordinator.postgres

import com.sslproxy.coordinator.config.PostgresConfig
import com.zaxxer.hikari.HikariConfig

import java.nio.file.{Files, Path}

private[postgres] final case class PostgresTlsMaterial():
  def delete(): Unit = ()

/** Configures PostgreSQL JDBC TLS directly from platform-mounted certificate
  * files. PostgreSQL validates the endpoint identity in `verify-full` mode.
  */
private[postgres] object PostgresTls:
  def configure(hikari: HikariConfig, config: PostgresConfig): PostgresTlsMaterial =
    val caPath = Path.of(config.sslCaPath)
    if !Files.isRegularFile(caPath) then
      throw IllegalArgumentException(s"PostgreSQL CA bundle is not a regular file: $caPath")
    hikari.addDataSourceProperty("sslmode", "verify-full")
    hikari.addDataSourceProperty("sslrootcert", caPath.toString)
    if config.sslClientKeyStorePath.nonEmpty then hikari.addDataSourceProperty("sslcert", config.sslClientKeyStorePath)
    if config.sslClientKeyStorePassword.nonEmpty then
      hikari.addDataSourceProperty("sslpassword", config.sslClientKeyStorePassword)
    PostgresTlsMaterial()
