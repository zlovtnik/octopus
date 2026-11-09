package com.sslproxy.coordinator.processor

import io.circe.Json
import java.nio.charset.StandardCharsets

/** Privileged opt-in inputs. Component decoders validate their own contracts;
  * an invalid envelope cannot silently activate a subset of its settings.
  */
object ProxyIntelligencePolicy:
  val Version = "proxy-intelligence-policy-v1"
  private val Fields = Set("version", "lexical", "cohorts", "threat_feed", "asn_cache")

  def valid(policy: Json): Boolean =
    policy.asObject.exists { obj =>
      obj.isEmpty || (obj.keys.toSet.subsetOf(Fields) &&
        obj("version").flatMap(_.asString).contains(Version) &&
        obj.toList.filterNot(_._1 == "version").forall(_._2.isObject) &&
        policy.noSpaces.getBytes(StandardCharsets.UTF_8).length <= 1048576)
    }

  def component(policy: Json, name: String): Json =
    if valid(policy) then policy.hcursor.downField(name).focus.getOrElse(Json.obj())
    else Json.obj("invalid_envelope" -> Json.fromBoolean(true))
