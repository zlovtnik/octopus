package com.sslproxy.coordinator.processor

import com.ibm.icu.text.IDNA
import java.net.InetAddress
import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.io.Source
import scala.util.Try

final case class ProxyAuthority(
  authority_raw: Option[String],
  host_raw: Option[String] = None,
  host_normalized: Option[String] = None,
  port: Option[Int] = None,
  authority_normalized: Option[String] = None,
  registrable_domain: Option[String] = None,
  normalization_version: String = ProxyAuthority.Version,
  parse_status: String = "invalid",
  authority_source: String,
  sanitization_status: String = "not_sanitized"
)

/** Pure evidence normalization; never performs DNS or infers a routing port.
  * Kept separate from generic text normalization and legacy document identities.
  */
object ProxyAuthority:
  val Version = "proxy-authority-v1"
  private val Idna = IDNA.getUTS46Instance(
    IDNA.USE_STD3_RULES | IDNA.CHECK_BIDI | IDNA.CHECK_CONTEXTJ |
      IDNA.NONTRANSITIONAL_TO_ASCII | IDNA.NONTRANSITIONAL_TO_UNICODE
  )
  private val Sources = Set("http_authority", "connect_authority", "quic_authority", "sni",
    "original_destination", "legacy_host", "query")
  private lazy val Rules: Set[String] =
    val stream = Source.fromResource("proxy-authority/public_suffix_list.dat", getClass.getClassLoader)
    try stream.getLines().map(_.trim).filter(s => s.nonEmpty && !s.startsWith("//")).map { rule =>
      val prefix = if rule.startsWith("!") then "!" else if rule.startsWith("*.") then "*." else ""
      prefix + ascii(rule.drop(prefix.length)).getOrElse(throw new IllegalStateException("invalid pinned authority PSL"))
    }.toSet
    finally stream.close()

  def normalize(raw: Option[String], source: String, sanitized: Boolean = false): ProxyAuthority =
    val base = ProxyAuthority(None, authority_source = source,
      sanitization_status = if sanitized then "sanitized" else "not_sanitized")
    raw match
      case None | Some("") => base.copy(authority_raw = raw, parse_status = "missing")
      case Some(value) if value.getBytes(StandardCharsets.UTF_8).length > 1024 ||
          value.exists(Character.isISOControl) || hasUnpairedSurrogate(value) => base
      case Some(value) =>
        val evidence = base.copy(authority_raw = Some(value))
        if sanitized || value.toLowerCase(Locale.ROOT).startsWith("hmac:") then
          evidence.copy(parse_status = "sanitized", sanitization_status = "sanitized")
        else if !Sources.contains(source) || value.exists(c => "@/\\?#%".contains(c) ||
            Character.isWhitespace(c) || Character.isSpaceChar(c)) then evidence
        else parse(value, evidence)

  private def parse(value: String, evidence: ProxyAuthority): ProxyAuthority =
    val bracketed = value.startsWith("[")
    val split: Option[(String, Option[String])] =
      if bracketed then
        val end = value.indexOf(']')
        if end < 0 then None
        else
          val tail = value.substring(end + 1)
          if tail.isEmpty then Some(value.substring(1, end) -> None)
          else if tail.startsWith(":") then Some(value.substring(1, end) -> Some(tail.drop(1)))
          else None
      else if value.count(_ == ':') == 1 then
        val end = value.indexOf(':')
        Some(value.take(end) -> Some(value.drop(end + 1)))
      else Some(value -> None)
    split match
      case None => evidence
      case Some((host, portText)) =>
        val port = portText.flatMap(p => Option.when(p.nonEmpty && p.length <= 5 && p.forall(c => c >= '0' && c <= '9'))(p))
          .flatMap(_.toIntOption).filter(p => p >= 1 && p <= 65535)
        if portText.isDefined && port.isEmpty then evidence
        else
          val withHost = evidence.copy(host_raw = Some(host))
          val ipv4 = strictIpv4(host)
          val ipv6 = if host.contains(':') && host.forall(c => "0123456789abcdefABCDEF:.".contains(c)) &&
              (!host.contains('.') || strictIpv4(host.substring(host.lastIndexOf(':') + 1)).isDefined) then
            Try(InetAddress.getByName(host).getAddress).toOption.map { bytes =>
              val b = if bytes.length == 4 then Array.fill[Byte](10)(0) ++ Array[Byte](-1, -1) ++ bytes else bytes
              canonicalIpv6(b)
            }
          else None
          val normalized =
            if ipv6.isDefined then ipv6.map(_ -> "valid_ipv6")
            else if ipv4.isDefined && !bracketed then ipv4.map(_ -> "valid_ipv4")
            else if bracketed || host.exists(c => ":[]".contains(c)) then None
            else ascii(host).map(_.stripSuffix(".")).filter(s => s.nonEmpty && s.length <= 253 &&
              !s.forall(c => "0123456789.".contains(c))).map(_ -> "valid_dns")
          normalized match
            case None => withHost
            case Some((name, status)) =>
              val endpoint = (if status == "valid_ipv6" then s"[$name]" else name) + port.fold("")(p => s":$p")
              withHost.copy(host_normalized = Some(name), port = port, authority_normalized = Some(endpoint),
                registrable_domain = if status == "valid_dns" then domain(name) else None, parse_status = status)

  private def ascii(host: String): Option[String] =
    val info = new IDNA.Info()
    val output = new java.lang.StringBuilder()
    val _ = Idna.nameToASCII(host, output, info)
    Option.unless(info.hasErrors)(output.toString.toLowerCase(Locale.ROOT))

  private def strictIpv4(host: String): Option[String] =
    val labels = host.split("\\.", -1).toList
    Option.when(labels.size == 4 && labels.forall(s => s.nonEmpty && s.length <= 3 &&
      s.forall(c => c >= '0' && c <= '9') && (s == "0" || !s.startsWith("0")) && s.toInt <= 255))(host)

  private def canonicalIpv6(bytes: Array[Byte]): String =
    val parts = (0 until 8).map(i => Integer.toHexString((bytes(i * 2) & 255) * 256 + (bytes(i * 2 + 1) & 255))).toVector
    val runs = (0 until 8).filter(i => parts(i) == "0" && (i == 0 || parts(i - 1) != "0"))
      .map(i => i -> parts.drop(i).takeWhile(_ == "0").length).filter(_._2 >= 2)
    runs.minByOption { case (start, length) => (-length, start) } match
      case None => parts.mkString(":")
      case Some((start, length)) => parts.take(start).mkString(":") + "::" + parts.drop(start + length).mkString(":")

  private def domain(host: String): Option[String] =
    val labels = host.split("\\.").toVector
    val exception = labels.indices.find(i => Rules.contains("!" + labels.drop(i).mkString(".")))
    val suffix = exception.map(i => labels.size - i - 1).getOrElse {
      labels.indices.foldLeft(1) { (size, i) =>
        val candidate = labels.drop(i).mkString(".")
        val exact = if Rules.contains(candidate) then labels.size - i else 1
        val wildcard = if i > 0 && Rules.contains("*." + candidate) then labels.size - i + 1 else 1
        size.max(exact).max(wildcard)
      }
    }
    Option.when(labels.size > suffix)(labels.takeRight(suffix + 1).mkString("."))

  private def hasUnpairedSurrogate(value: String): Boolean =
    value.codePoints().anyMatch(cp => cp >= 0xd800 && cp <= 0xdfff)
