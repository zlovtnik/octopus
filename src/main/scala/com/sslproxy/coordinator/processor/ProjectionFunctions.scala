package com.sslproxy.coordinator.processor

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale

enum FrameToken(val value: String):
  case Beacon extends FrameToken("beacon")
  case ProbeRequest extends FrameToken("probe_request")
  case ProbeResponse extends FrameToken("probe_response")
  case Authentication extends FrameToken("authentication")
  case AssociationRequest extends FrameToken("association_request")
  case AssociationResponse extends FrameToken("association_response")
  case ReassociationRequest extends FrameToken("reassociation_request")
  case ReassociationResponse extends FrameToken("reassociation_response")
  case Disassociation extends FrameToken("disassociation")
  case Deauthentication extends FrameToken("deauthentication")
  case Action extends FrameToken("action")
  case Data extends FrameToken("data")
  case Other extends FrameToken("other")

object FrameToken:
  def normalize(frameType: Option[String], frameSubtype: Option[String]): FrameToken =
    val kind = frameType.fold("")(_.trim.toLowerCase(Locale.ROOT))
    val subtype = frameSubtype.fold("")(_.trim.toLowerCase(Locale.ROOT).replace('-', '_').replace(' ', '_'))
    subtype match
      case "beacon" => FrameToken.Beacon
      case "probe_request" => FrameToken.ProbeRequest
      case "probe_response" => FrameToken.ProbeResponse
      case "authentication" | "auth" => FrameToken.Authentication
      case "association_request" | "assoc_request" => FrameToken.AssociationRequest
      case "association_response" | "assoc_response" => FrameToken.AssociationResponse
      case "reassociation_request" | "reassoc_request" => FrameToken.ReassociationRequest
      case "reassociation_response" | "reassoc_response" => FrameToken.ReassociationResponse
      case "disassociation" | "disassoc" => FrameToken.Disassociation
      case "deauthentication" | "deauth" => FrameToken.Deauthentication
      case "action" => FrameToken.Action
      case _ if kind == "data" => FrameToken.Data
      case _ => FrameToken.Other

object ProjectionFunctions:
  def percentile(values: Vector[Double], probability: Double): Option[Double] =
    if values.isEmpty then None
    else
      val sorted = values.sorted
      val bounded = probability.max(0.0d).min(1.0d)
      val position = bounded * (sorted.size - 1).toDouble
      val lower = position.floor.toInt
      val upper = position.ceil.toInt
      val fraction = position - lower.toDouble
      Some(sorted(lower) + ((sorted(upper) - sorted(lower)) * fraction))

  def medianAbsoluteDeviation(values: Vector[Double]): Option[Double] =
    percentile(values, 0.5d).flatMap { median =>
      percentile(values.map(value => Math.abs(value - median)), 0.5d)
    }

  def transitionProbabilities(tokens: Vector[FrameToken]): Map[(FrameToken, FrameToken), Double] =
    val pairs = tokens.zip(tokens.drop(1))
    val totals = pairs.groupMapReduce(_._1)(_ => 1)(_ + _)
    pairs.groupMapReduce(identity)(_ => 1)(_ + _).view.map { case (pair, count) =>
      pair -> (count.toDouble / totals(pair._1).toDouble)
    }.toMap

  def connectedComponents[A: Ordering](edges: Iterable[(A, A)]): Vector[Vector[A]] =
    val adjacency = edges.foldLeft(Map.empty[A, Set[A]]) { case (result, (left, right)) =>
      result
        .updated(left, result.getOrElse(left, Set.empty) + right)
        .updated(right, result.getOrElse(right, Set.empty) + left)
    }

    @annotation.tailrec
    def visit(frontier: List[A], seen: Set[A]): Set[A] =
      frontier match
        case Nil => seen
        case head :: tail if seen.contains(head) => visit(tail, seen)
        case head :: tail =>
          val next = adjacency.getOrElse(head, Set.empty).toList
          visit(next ::: tail, seen + head)

    @annotation.tailrec
    def build(remaining: Set[A], result: Vector[Vector[A]]): Vector[Vector[A]] =
      remaining.minOption match
        case None => result
        case Some(start) =>
          val component = visit(List(start), Set.empty)
          build(remaining -- component, result :+ component.toVector.sorted)

    build(adjacency.keySet, Vector.empty)

  def weightedRisk(components: Iterable[(Double, Double)]): Double =
    components.iterator
      .map((score, weight) => score.max(0.0d).min(100.0d) * weight.max(0.0d))
      .sum
      .max(0.0d)
      .min(100.0d)

  def cosineSimilarity(left: Vector[Double], right: Vector[Double]): Option[Double] =
    Option.when(left.nonEmpty && left.size == right.size) {
      val dot = left.lazyZip(right).map(_ * _).sum
      val leftNorm = Math.sqrt(left.map(value => value * value).sum)
      val rightNorm = Math.sqrt(right.map(value => value * value).sum)
      if leftNorm == 0.0d || rightNorm == 0.0d then 0.0d
      else (dot / (leftNorm * rightNorm)).max(-1.0d).min(1.0d)
    }

  def stableId(namespace: String, parts: Iterable[String]): String =
    val normalized = (namespace +: parts.toVector).mkString("\u001f")
    val digest = MessageDigest.getInstance("SHA-256")
      .digest(normalized.getBytes(StandardCharsets.UTF_8))
    val hex = digest.take(16).map("%02x".format(_)).mkString
    s"${hex.take(8)}-${hex.slice(8, 12)}-5${hex.slice(13, 16)}-a${hex.slice(17, 20)}-${hex.drop(20)}"
