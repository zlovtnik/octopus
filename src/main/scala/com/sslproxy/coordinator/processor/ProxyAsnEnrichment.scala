package com.sslproxy.coordinator.processor

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.{Json, Printer}
import io.circe.syntax.*
import java.net.{InetAddress, URI}
import java.time.Instant
import scala.util.Try

/** A supplied, versioned local cache. It never resolves names or fetches data.
  * ASN membership describes routing evidence, not ownership or threat status.
  */
object ProxyAsnEnrichment:
  val Version = "proxy-asn-enrichment-v1"
  private val MaximumEntries = 256
  private val MaximumAddresses = 256
  private val PrinterCanonical = Printer.noSpaces.copy(sortKeys = true)
  private val FirstInstant = Instant.parse("0001-01-01T00:00:00Z")
  private val LastInstant = Instant.parse("9999-12-31T23:59:59.999999999Z")
  private val Fields = Set("source", "publisher", "source_uri", "snapshot_version", "license",
    "retrieved_at", "refresh_after", "expires_at", "entries_sha256", "entries")

  private final case class Address(host: String, bits: Int, value: BigInt)
  private final case class Entry(network: Address, prefix: Int, asn: Long):
    def cidr: String = s"${network.host}/$prefix"
    def matches(address: Address): Boolean =
      address.bits == network.bits &&
        (address.value >> (network.bits - prefix)) == (network.value >> (network.bits - prefix))
    def json: Json = Json.obj("cidr" -> cidr.asJson, "asn" -> asn.asJson)
  private final case class Snapshot(metadata: Json, entries: List[Entry], refresh: Instant, expiry: Instant)

  private def text(row: Json, key: String, maximum: Int): Option[String] =
    row.hcursor.get[String](key).toOption.filter(value => value.nonEmpty && value.length <= maximum &&
      value.forall(c => c >= ' ' && c <= '~'))

  private def finite(at: Instant): Boolean = !at.isBefore(FirstInstant) && !at.isAfter(LastInstant)
  private def instant(row: Json, key: String): Option[Instant] =
    text(row, key, 40).flatMap(value => Try(Instant.parse(value)).toOption).filter(finite)

  private def numeric(value: String): Option[Address] =
    val safe = value.nonEmpty && value.length <= 45 && value.forall(c => "0123456789abcdefABCDEF:.".contains(c))
    if !safe then None
    else
      val normalized = ProxyAuthority.normalize(Some(value), "original_destination")
      normalized.host_normalized.filter(_ => normalized.port.isEmpty).flatMap { host =>
        normalized.parse_status match
          case "valid_ipv4" =>
            val bytes = host.split("\\.").map(_.toInt.toByte)
            Some(Address(host, 32, BigInt(1, bytes)))
          case "valid_ipv6" =>
            // Numeric-only input cannot take InetAddress's hostname/DNS path.
            // Java collapses mapped IPv6 to IPv4; reject that ambiguous family.
            Try(InetAddress.getByName(host).getAddress).toOption.filter(_.length == 16)
              .map(bytes => Address(host, 128, BigInt(1, bytes)))
          case _ => None
      }

  private def entry(row: Json): Option[Entry] =
    for
      _ <- row.asObject.filter(_.keys.toSet == Set("cidr", "asn"))
      cidr <- text(row, "cidr", 49).filter(_.count(_ == '/') == 1)
      parts = cidr.split("/", -1)
      address <- numeric(parts(0))
      prefix <- Option.when(parts(1).matches("0|[1-9][0-9]{0,2}"))(parts(1))
        .flatMap(_.toIntOption).filter(_ <= address.bits)
      _ <- Option.when((address.value >> (address.bits - prefix) << (address.bits - prefix)) == address.value)(())
      asn <- row.hcursor.get[Long]("asn").toOption.filter(value => value >= 1 && value <= 4294967295L)
    yield Entry(address, prefix, asn)

  private def entries(value: Json): Option[List[Entry]] =
    value.asArray.filter(_.size <= MaximumEntries).flatMap { rows =>
      val parsed = rows.toList.flatMap(entry)
      Option.when(parsed.size == rows.size && parsed.map(_.cidr).distinct.size == parsed.size)(
        parsed.sortBy(_.cidr))
    }

  /** Checksum of validated network-aligned, normalized entries, independent of
    * input order and JSON object key order. Useful when preparing local inputs.
    */
  def snapshotChecksum(value: Json): Option[String] =
    entries(value).map(rows => Sha256Utils.sha256Hex(PrinterCanonical.print(rows.map(_.json).asJson)))

  private def snapshot(cache: Json, asOf: Instant): Option[Snapshot] =
    for
      _ <- cache.asObject.filter(_.keys.toSet == Fields)
      _ <- text(cache, "source", 128)
      _ <- text(cache, "publisher", 256)
      sourceUri <- text(cache, "source_uri", 1024)
      uri <- Try(new URI(sourceUri)).toOption.filter(uri => uri.isAbsolute &&
        Set("https", "http", "file", "urn")(Option(uri.getScheme).getOrElse("")) && uri.getUserInfo == null)
      _ <- Option.when(uri.getScheme != "http" && uri.getScheme != "https" || Option(uri.getHost).exists(_.nonEmpty))(())
      _ <- text(cache, "snapshot_version", 128)
      _ <- text(cache, "license", 256)
      retrieved <- instant(cache, "retrieved_at").filter(!_.isAfter(asOf))
      refresh <- instant(cache, "refresh_after").filter(!_.isBefore(retrieved))
      expiry <- instant(cache, "expires_at").filter(_.isAfter(refresh))
      input <- cache.hcursor.downField("entries").focus
      rows <- entries(input)
      checksum <- text(cache, "entries_sha256", 64).filter(_.matches("[0-9a-f]{64}"))
      actual <- snapshotChecksum(input).filter(_ == checksum)
    yield Snapshot(Json.obj(
      "source" -> cache.hcursor.downField("source").focus.get,
      "publisher" -> cache.hcursor.downField("publisher").focus.get,
      "source_uri" -> sourceUri.asJson,
      "snapshot_version" -> cache.hcursor.downField("snapshot_version").focus.get,
      "license" -> cache.hcursor.downField("license").focus.get,
      "retrieved_at" -> retrieved.toString.asJson,
      "refresh_after" -> refresh.toString.asJson,
      "expires_at" -> expiry.toString.asJson,
      "entries_sha256" -> actual.asJson,
      "entry_count" -> rows.size.asJson
    ), rows, refresh, expiry)

  def evidence(addresses: List[String], cache: Json, asOf: Instant): Json =
    val overflow = addresses.size > MaximumAddresses
    val supplied = !cache.isNull && !cache.asObject.exists(_.isEmpty)
    val loaded = if overflow || !finite(asOf) then None else snapshot(cache, asOf)
    val usable = loaded.filter(_.expiry.isAfter(asOf))
    val normalized = if overflow then Nil else addresses.flatMap(numeric).groupBy(_.host)
      .toList.sortBy(_._1).map(_._2.head)
    val results = usable.toList.flatMap { data =>
      normalized.map { address =>
        val matched = data.entries.filter(_.matches(address)).minByOption(row => (-row.prefix, row.cidr))
        Json.obj("ip" -> address.host.asJson,
          "asn" -> matched.map(_.asn).asJson,
          "matched_cidr" -> matched.map(_.cidr).asJson,
          "disposition" -> (if matched.isDefined then "cached_prefix_match" else "no_cached_prefix_match").asJson)
      }
    }
    val disposition = if overflow then "address_bound_exceeded"
      else if !finite(asOf) then "invalid_evaluation_time"
      else if !supplied then "unavailable_no_local_cache"
      else if loaded.isEmpty then "unavailable_invalid_local_cache"
      else if usable.isEmpty then "unavailable_expired_local_cache"
      else "cached_evidence_available"
    Json.obj(
      "calculation_version" -> Version.asJson,
      "disposition" -> disposition.asJson,
      "refresh_state" -> (if usable.isEmpty then "unavailable"
        else if loaded.exists(data => !asOf.isBefore(data.refresh)) then "refresh_due" else "fresh").asJson,
      "stale_fallback" -> false.asJson,
      "lookup_mode" -> "local_snapshot_longest_prefix_no_network".asJson,
      "ownership" -> "unavailable".asJson,
      "caveat" -> "ASN prefix membership does not establish infrastructure ownership or threat; shared infrastructure may serve unrelated destinations".asJson,
      "input_address_count" -> addresses.size.asJson,
      "invalid_address_count" -> (if overflow then Json.Null else addresses.count(value => numeric(value).isEmpty).asJson),
      "unique_address_count" -> normalized.size.asJson,
      "matched_address_count" -> results.count(_.hcursor.get[String]("disposition").contains("cached_prefix_match")).asJson,
      "provenance" -> loaded.map(_.metadata).asJson,
      "addresses" -> results.asJson
    )
