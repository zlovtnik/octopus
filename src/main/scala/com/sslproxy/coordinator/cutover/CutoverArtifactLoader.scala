package com.sslproxy.coordinator.cutover

import cats.effect.kernel.{Async, Clock}
import cats.syntax.all.*
import com.sslproxy.coordinator.config.CutoverConfig

import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.file.{Files, Path}
import java.security.spec.X509EncodedKeySpec
import java.security.{KeyFactory, MessageDigest, Signature}
import java.time.Instant
import java.util.Base64
import scala.util.Try

object CutoverArtifactLoader:
  private val MaximumArtifactBytes = 1024L * 1024L
  private val MaximumSignatureBytes = 4096L
  private val MaximumPublicKeyBytes = 65536L

  /** Loads a canonical artifact and detached Base64 Ed25519 signature. The
    * public key may be X.509 DER, unwrapped Base64 X.509, or PUBLIC KEY PEM;
    * its decoded DER fingerprint must match the configured SHA-256 pin.
    */
  def loadAndVerify[F[_]: Async](config: CutoverConfig): F[VerifiedCutoverArtifact] =
    for
      _ <- Async[F].fromEither(validateLoaderConfig(config))
      artifactBytes <- readBounded[F](
        "artifact",
        config.artifactPath,
        MaximumArtifactBytes
      )
      signatureFileBytes <- readBounded[F](
        "signature",
        config.signaturePath,
        MaximumSignatureBytes
      )
      publicKeySource <-
        if config.publicKeyPath.trim.nonEmpty then
          readBounded[F]("public key", config.publicKeyPath, MaximumPublicKeyBytes)
        else Async[F].pure(config.publicKeyBase64.getBytes(StandardCharsets.US_ASCII))
      verifiedAt <- Clock[F].realTimeInstant
      verified <- Async[F].fromEither(
        CutoverArtifactVerifier.verify(
          artifactBytes,
          signatureFileBytes,
          publicKeySource,
          config,
          verifiedAt
        )
      )
    yield verified

  private def validateLoaderConfig(config: CutoverConfig): Either[CutoverError, Unit] =
    val keySources = List(config.publicKeyPath, config.publicKeyBase64).count(_.trim.nonEmpty)
    val error =
      if config.artifactPath.trim.isEmpty then Some("artifact path is blank")
      else if config.signaturePath.trim.isEmpty then Some("signature path is blank")
      else if keySources != 1 then Some("exactly one public key source is required")
      else if !config.publicKeySha256.matches("^[0-9a-f]{64}$") then
        Some("public key SHA-256 pin is invalid")
      else if config.expectedSchemaVersion <= 0 then Some("expected schema version must be positive")
      else if config.expectedClusterId.trim.isEmpty then Some("expected cluster id is blank")
      else if config.requiredConsumerGroups.isEmpty then Some("required consumer groups are empty")
      else None

    error match
      case Some(detail) => Left(CutoverFormatError(s"invalid cutover loader configuration: $detail"))
      case None         => Right(())

  private def readBounded[F[_]: Async](
      kind: String,
      configuredPath: String,
      maximumBytes: Long
  ): F[Array[Byte]] =
    val path = Path.of(configuredPath)
    Async[F].blocking {
      val reportedSize = Files.size(path)
      if reportedSize > maximumBytes then
        throw CutoverSizeError(kind, reportedSize, maximumBytes)
      val bytes = Files.readAllBytes(path)
      if bytes.length.toLong > maximumBytes then
        throw CutoverSizeError(kind, bytes.length.toLong, maximumBytes)
      bytes
    }.handleErrorWith {
      case error: CutoverError => Async[F].raiseError(error)
      case error =>
        val detail = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
        Async[F].raiseError(CutoverReadError(kind, configuredPath, detail))
    }

object CutoverArtifactVerifier:
  def verify(
      artifactBytes: Array[Byte],
      detachedSignatureFileBytes: Array[Byte],
      publicKeySource: Array[Byte],
      config: CutoverConfig,
      verifiedAt: Instant
  ): Either[CutoverError, VerifiedCutoverArtifact] =
    for
      parsed <- CutoverArtifactCodec.parseAndValidate(
        artifactBytes,
        config.expectedSchemaVersion,
        config.expectedClusterId,
        config.requiredConsumerGroups
      )
      signatureBytes <- decodeDetachedSignature(detachedSignatureFileBytes)
      publicKeyBytes <- decodePublicKeySource(publicKeySource)
      publicKeySha256 = CutoverSha256.hex(publicKeyBytes)
      _ <- Either.cond(
        MessageDigest.isEqual(
          config.publicKeySha256.getBytes(StandardCharsets.US_ASCII),
          publicKeySha256.getBytes(StandardCharsets.US_ASCII)
        ),
        (),
        CutoverPublicKeyPinMismatch(config.publicKeySha256, publicKeySha256)
      )
      publicKey <- Try {
        KeyFactory.getInstance("Ed25519").generatePublic(X509EncodedKeySpec(publicKeyBytes))
      }.toEither.left.map(error => CutoverPublicKeyError(safeMessage(error)))
      signatureValid <- Try {
        val verifier = Signature.getInstance("Ed25519")
        verifier.initVerify(publicKey)
        verifier.update(parsed.canonicalBytes)
        verifier.verify(signatureBytes)
      }.toEither.left.map(error => CutoverSignatureError(safeMessage(error)))
      _ <- Either.cond(signatureValid, (), CutoverSignatureMismatch())
    yield VerifiedCutoverArtifact(
      artifact = parsed.artifact,
      canonicalArtifactSha256 = parsed.canonicalSha256,
      signatureSha256 = CutoverSha256.hex(signatureBytes),
      publicKeySha256 = publicKeySha256,
      verifiedAt = verifiedAt
    )

  private def decodeDetachedSignature(bytes: Array[Byte]): Either[CutoverError, Array[Byte]] =
    for
      raw <- decodeAscii(bytes, "signature")
      normalized = stripOneTerminalLineEnding(raw)
      _ <- Either.cond(
        normalized.nonEmpty && !normalized.exists(_.isWhitespace),
        (),
        CutoverSignatureError("must be unwrapped Base64 with at most one trailing line ending")
      )
      decoded <- Try(Base64.getDecoder.decode(normalized)).toEither.left.map { _ =>
        CutoverSignatureError("must be valid Base64")
      }
      _ <- Either.cond(
        decoded.length == 64,
        (),
        CutoverSignatureError(s"decoded Ed25519 signature must be 64 bytes, found ${decoded.length}")
      )
    yield decoded

  private def decodePublicKeySource(bytes: Array[Byte]): Either[CutoverError, Array[Byte]] =
    if bytes.headOption.contains(0x30.toByte) then Right(bytes.clone())
    else
      decodeAscii(bytes, "public key") match
        case Right(text) => decodeTextPublicKey(text)
        case Left(_)     => Right(bytes.clone())

  private def decodeTextPublicKey(text: String): Either[CutoverError, Array[Byte]] =
    val normalized = stripOneTerminalLineEnding(text)
    val encoded =
      if normalized.startsWith("-----BEGIN PUBLIC KEY-----") then
        val lines = normalized.split("\\r?\\n", -1).toList
        lines match
          case "-----BEGIN PUBLIC KEY-----" :: body
              if body.nonEmpty && body.lastOption.contains("-----END PUBLIC KEY-----") =>
            body.dropRight(1).mkString
          case _ => return Left(CutoverPublicKeyError("malformed PUBLIC KEY PEM document"))
      else normalized

    if encoded.isEmpty || encoded.exists(_.isWhitespace) then
      Left(CutoverPublicKeyError("must be X.509 DER, unwrapped Base64, or PUBLIC KEY PEM"))
    else
      Try(Base64.getDecoder.decode(encoded)).toEither.left.map { _ =>
        CutoverPublicKeyError("text key must contain valid Base64 X.509 bytes")
      }

  private def decodeAscii(bytes: Array[Byte], kind: String): Either[CutoverError, String] =
    Try {
      StandardCharsets.US_ASCII
        .newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(java.nio.ByteBuffer.wrap(bytes))
        .toString
    }.toEither.left.map { _ =>
      if kind == "signature" then CutoverSignatureError("must be ASCII Base64")
      else CutoverPublicKeyError("is not ASCII text")
    }

  private def stripOneTerminalLineEnding(value: String): String =
    if value.endsWith("\r\n") then value.dropRight(2)
    else if value.endsWith("\n") then value.dropRight(1)
    else value

  private def safeMessage(error: Throwable): String =
    Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
