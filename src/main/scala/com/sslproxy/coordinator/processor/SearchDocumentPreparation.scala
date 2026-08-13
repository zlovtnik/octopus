package com.sslproxy.coordinator.processor

import com.sslproxy.coordinator.util.Sha256Utils

import java.nio.charset.StandardCharsets
import java.text.Normalizer
import java.util.{Locale, UUID}

enum SearchDocumentKind(
    val sourceTable: String,
    val sourceKind: String,
    val embeddingKind: String
):
  case Event extends SearchDocumentKind("wireless_frames", "event", "event")
  case Device extends SearchDocumentKind("inventory_devices", "device", "device")
  case Behaviour extends SearchDocumentKind("behaviour_snapshots", "behaviour_window", "behaviour")
  case Sequence extends SearchDocumentKind("frame_sequences", "frame_sequence", "sequence")

final case class SearchDocumentSource(
    kind: SearchDocumentKind,
    sourceKey: String,
    sourceMac: Option[String],
    locationId: Option[String],
    sensorId: Option[String],
    observedAt: java.sql.Timestamp,
    bssid: Option[String],
    ssid: Option[String],
    frameSubtype: Option[String],
    securityFlags: Int,
    handshakeCaptured: Boolean,
    searchText: String,
    detailJson: String
)

final case class PreparedSearchDocument(
    kind: SearchDocumentKind,
    documentId: String,
    sourceKey: String,
    sourceVersion: Long,
    sourceMac: Option[String],
    locationId: Option[String],
    sensorId: Option[String],
    observedAt: java.sql.Timestamp,
    bssid: Option[String],
    ssid: Option[String],
    frameSubtype: Option[String],
    tags: List[(String, String)],
    detailJson: String,
    securityFlags: Int,
    handshakeCaptured: Boolean,
    title: Option[String],
    normalizedText: String,
    normalizedSha256: String,
    tokens: List[(String, Double, Int)]
)

object SearchDocumentPreparation:
  private val TokenPattern = "[a-z0-9][a-z0-9:_./-]{0,190}".r

  def prepare(source: SearchDocumentSource): Either[String, PreparedSearchDocument] =
    val normalizedText = normalize(source.searchText)
    if source.sourceKey.isBlank then Left("search document source key must not be blank")
    else if normalizedText.isEmpty then Left(s"search document ${source.sourceKey} has no searchable text")
    else
      val checksum = Sha256Utils.sha256Hex(normalizedText.getBytes(StandardCharsets.UTF_8))
      val documentId = stableUuid(s"${source.kind.sourceTable}:${source.sourceKey}:$checksum")
      val tokenCounts = tokenize(normalizedText).groupMapReduce(identity)(_ => 1)(_ + _)
      val total = tokenCounts.values.sum.max(1)
      val tokens = tokenCounts.toList.sortBy(_._1).map { case (token, count) =>
        (token, count.toDouble / total.toDouble, count)
      }
      val tags = List(
        source.sourceMac.map("source_mac" -> _),
        source.locationId.map("location_id" -> _),
        source.sensorId.map("sensor_id" -> _),
        source.bssid.map("bssid" -> _),
        source.ssid.map("ssid" -> _),
        source.frameSubtype.map("frame_subtype" -> _)
      ).flatten.distinct.sortBy(identity)
      val title = List(source.frameSubtype, source.ssid, source.sourceMac)
        .flatten.map(_.trim).filter(_.nonEmpty).distinct match
        case Nil => None
        case values => Some(values.mkString(" ").take(512))

      Right(PreparedSearchDocument(
        kind = source.kind,
        documentId = documentId,
        sourceKey = source.sourceKey,
        sourceVersion = java.lang.Long.parseUnsignedLong(checksum.take(15), 16),
        sourceMac = source.sourceMac,
        locationId = source.locationId,
        sensorId = source.sensorId,
        observedAt = source.observedAt,
        bssid = source.bssid,
        ssid = source.ssid,
        frameSubtype = source.frameSubtype,
        tags = tags,
        detailJson = source.detailJson,
        securityFlags = source.securityFlags,
        handshakeCaptured = source.handshakeCaptured,
        title = title,
        normalizedText = normalizedText,
        normalizedSha256 = checksum,
        tokens = tokens
      ))

  def normalize(value: String): String =
    Normalizer.normalize(Option(value).getOrElse(""), Normalizer.Form.NFKC)
      .toLowerCase(Locale.ROOT)
      .split("\\s+")
      .iterator
      .filter(_.nonEmpty)
      .mkString(" ")

  def tokenize(normalizedText: String): List[String] =
    TokenPattern.findAllIn(normalizedText).toList

  private def stableUuid(value: String): String =
    UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8)).toString
