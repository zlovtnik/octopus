package com.sslproxy.coordinator.tidb

import com.sslproxy.coordinator.config.TiDbConfig
import com.zaxxer.hikari.HikariConfig

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path}
import java.security.KeyStore
import java.security.cert.{Certificate, CertificateFactory}
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Try

private[tidb] final case class TidbTlsMaterial(
    trustStorePath: Path,
    trustStorePassword: String
):
  def delete(): Unit = Files.deleteIfExists(trustStorePath): Unit

/** Converts the mounted PEM/DER CA bundle to the PKCS12 trust-store format
  * accepted by MySQL Connector/J. The generated file contains public
  * certificates only and is removed when the Hikari pool closes.
  */
private[tidb] object TidbTls:
  def configure(hikari: HikariConfig, config: TiDbConfig): TidbTlsMaterial =
    val material = createTrustStore(Path.of(config.sslCaPath))

    hikari.addDataSourceProperty("trustCertificateKeyStoreUrl", material.trustStorePath.toUri.toString)
    hikari.addDataSourceProperty("trustCertificateKeyStoreType", "PKCS12")
    hikari.addDataSourceProperty("trustCertificateKeyStorePassword", material.trustStorePassword)
    hikari.addDataSourceProperty("fallbackToSystemTrustStore", "false")
    hikari.addDataSourceProperty("tlsVersions", "TLSv1.2,TLSv1.3")

    if config.sslClientKeyStorePath.nonEmpty then
      hikari.addDataSourceProperty(
        "clientCertificateKeyStoreUrl",
        Path.of(config.sslClientKeyStorePath).toUri.toString
      )
      hikari.addDataSourceProperty(
        "clientCertificateKeyStoreType",
        config.sslClientKeyStoreType.toUpperCase(java.util.Locale.ROOT)
      )
      hikari.addDataSourceProperty(
        "clientCertificateKeyStorePassword",
        config.sslClientKeyStorePassword
      )
      hikari.addDataSourceProperty("fallbackToSystemKeyStore", "false")

    material

  private def createTrustStore(caPath: Path): TidbTlsMaterial =
    val certificates = readCertificates(caPath)
    if certificates.isEmpty then
      throw IllegalArgumentException(s"TiDB CA bundle contains no X.509 certificates: $caPath")

    val password = UUID.randomUUID().toString
    val passwordChars = password.toCharArray
    val trustStore = KeyStore.getInstance("PKCS12")
    trustStore.load(null, passwordChars)
    certificates.zipWithIndex.foreach { case (certificate, index) =>
      trustStore.setCertificateEntry(s"tidb-ca-$index", certificate)
    }

    val path = Files.createTempFile("octopus-tidb-ca-", ".p12")
    path.toFile.deleteOnExit()
    setOwnerOnlyPermissions(path)
    val output = new BufferedOutputStream(Files.newOutputStream(path))
    try trustStore.store(output, passwordChars)
    catch
      case error: Throwable =>
        Files.deleteIfExists(path): Unit
        throw error
    finally output.close()

    TidbTlsMaterial(path, password)

  private def readCertificates(path: Path): List[Certificate] =
    val input = new BufferedInputStream(Files.newInputStream(path))
    try
      CertificateFactory.getInstance("X.509").generateCertificates(input).asScala.toList
    finally input.close()

  private def setOwnerOnlyPermissions(path: Path): Unit =
    Try(Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-------"))): Unit
