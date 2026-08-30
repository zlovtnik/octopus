package com.sslproxy.coordinator.domain

import io.circe.Decoder
import io.circe.parser.decode as circeDecode

final case class PayloadAudit(
  observedAt: String,
  host: Option[String],
  method: Option[String],
  path: Option[String],
  queryKeys: Option[List[String]],
  body: Option[io.circe.Json],
  bodyBytesOriginal: Option[Int],
  truncated: Option[Boolean],
  contentType: Option[String],
  peerIp: Option[String],
  wgPubkey: Option[String],
  deviceId: Option[String],
  identitySource: Option[String],
  peerHostname: Option[String]
)

object PayloadAudit:
  private val StreamName = "proxy.payload_audit"

  given Decoder[PayloadAudit] = Decoder.instance { c =>
    for
      observedAt <- c.downField("observed_at").as[String]
      host <- c.downField("host").as[Option[String]]
      method <- c.downField("method").as[Option[String]]
      path <- c.downField("path").as[Option[String]]
      queryKeys <- c.downField("query_keys").as[Option[List[String]]]
      body <- c.downField("body").as[Option[io.circe.Json]]
      bodyBytesOriginal <- c.downField("body_bytes_original").as[Option[Int]]
      truncated <- c.downField("truncated").as[Option[Boolean]]
      contentType <- c.downField("content_type").as[Option[String]]
      peerIp <- c.downField("peer_ip").as[Option[String]]
      wgPubkey <- c.downField("wg_pubkey").as[Option[String]]
      deviceId <- c.downField("device_id").as[Option[String]]
      identitySource <- c.downField("identity_source").as[Option[String]]
      peerHostname <- c.downField("peer_hostname").as[Option[String]]
    yield PayloadAudit(
      observedAt,
      host,
      method,
      path,
      queryKeys,
      body,
      bodyBytesOriginal,
      truncated,
      contentType,
      peerIp,
      wgPubkey,
      deviceId,
      identitySource,
      peerHostname
    )
  }

  def parse(json: String): Either[Throwable, PayloadAudit] =
    circeDecode[PayloadAudit](json)

  def streamName: String = StreamName
