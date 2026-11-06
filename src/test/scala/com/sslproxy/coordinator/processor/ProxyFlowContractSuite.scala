package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.parser.parse
import io.circe.syntax.*
import java.time.{Duration, Instant, OffsetDateTime}
import munit.FunSuite
import scala.util.Try

class ProxyFlowContractSuite extends FunSuite:
  private val Open = "tunnel_open"
  private val Close = "tunnel_close"
  private val Horizon = Duration.ofSeconds(5)
  private val resource = scala.io.Source.fromResource("contracts/proxy-events-v1.json")
  private val corpus = try parse(resource.mkString).toOption.get finally resource.close()
  private val rows = corpus.hcursor.downField("observations").focus.get.asArray.get.toList.map { observation =>
    parse(observation.hcursor.get[String]("raw_json").toOption.get).toOption.get
  }
  private val open = rows.find(text(_, "type").contains(Open)).get
  private val close = rows.find(text(_, "type").contains(Close)).get
  private val openAt = time(open).get
  private val closeAt = time(close).get
  private val coverage = Some((openAt.minus(Horizon), closeAt.plus(Horizon)))
  private val correlation = text(open, "correlation_id").get
  private val key = ("deployment-a", correlation)

  private def text(row: Json, field: String): Option[String] = row.hcursor.get[String](field).toOption
  private def time(row: Json): Option[Instant] =
    text(row, "time").flatMap(value => Try(OffsetDateTime.parse(value).toInstant).toOption)
  private def uuid(row: Json, field: String): Option[String] =
    text(row, field).filter(_.matches("[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"))
  private def withTime(row: Json, at: Instant): Json =
    row.deepMerge(Json.obj("time" -> at.toString.asJson))

  // Offline reference for a bounded, attested snapshot. Runtime lookup,
  // revisions and persistence are outside this contract suite.
  private def dispositions(
    snapshot: List[(String, Json)],
    covered: Option[(Instant, Instant)] = coverage,
    overflow: Boolean = false,
    retainedFrom: Option[Instant] = None,
    unresolvedConflicts: Map[(String, String), Set[String]] = Map.empty
  ): Map[(String, String), String] =
    val versions = snapshot.distinct
    val conflictingIds = versions.flatMap { (scope, row) => uuid(row, "event_id").map(scope -> _) }
      .groupBy(identity).collect { case (id, occurrences) if occurrences.size > 1 => id }.toSet ++
      unresolvedConflicts.keySet
    // Decoded from the separate, attested source_conflicts column, never raw payload fields.
    val conflictingFlows = unresolvedConflicts.toList.flatMap { case ((scope, _), correlations) =>
      correlations.map(scope -> _)
    }.toSet
    val grouped = versions.flatMap { (scope, row) =>
      uuid(row, "correlation_id")
        .filter(_ => scope.nonEmpty && text(row, "type").exists(Set(Open, Close)))
        .map(correlation => (scope, correlation) -> row)
    }.groupMap(_._1)(_._2)
    (grouped.keySet ++ conflictingFlows).map { flowKey =>
      val (scope, _) = flowKey
      val candidates = grouped.getOrElse(flowKey, Nil)
      val (opens, closes) = candidates.partition(text(_, "type").contains(Open))
      val disposition =
        if conflictingFlows(flowKey) ||
          candidates.exists(row => uuid(row, "event_id").exists(id => conflictingIds(scope -> id))) ||
          opens.size > 1 || closes.size > 1 then "ambiguous"
        else if candidates.exists { row =>
          val expected = if text(row, "type").contains(Open) then 1 else 2
          uuid(row, "event_id").isEmpty || time(row).isEmpty || text(row, "host").forall(_.isEmpty) ||
            !row.hcursor.get[Int]("event_sequence").contains(expected) ||
            !row.hcursor.get[Boolean]("blocked").contains(false)
        } then "ineligible"
        else
          val first = opens.headOption.flatMap(time)
          val last = closes.headOption.flatMap(time)
          val start = last.map(_.minus(Horizon)).orElse(first).get
          val end = first.map(_.plus(Horizon)).orElse(last).get
          val full = covered.exists((lower, upper) => !start.isBefore(lower) && end.isBefore(upper))
          (opens, closes) match
            case (o :: Nil, c :: Nil) if text(o, "host") != text(c, "host") ||
              time(c).get.isBefore(time(o).get) => "inconsistent"
            case (_ :: Nil, c :: Nil) if time(c).get.isAfter(end) => "outside_horizon"
            case _ if overflow => "lookup_incomplete"
            case _ if retainedFrom.exists(start.isBefore) => "retention_truncated"
            case _ if !full => "coverage_limited"
            case (Nil, _) => "missing_open"
            case (_, Nil) => "missing_terminal"
            case _ => "validated"
      flowKey -> disposition
    }.toMap

  private def scoped(observations: Json*): List[(String, Json)] =
    observations.toList.map("deployment-a" -> _)

  test("frozen pair is one flow regardless of arrival order and exact re-delivery"):
    assertEquals(dispositions(scoped(rows*)), Map(key -> "validated"))
    assertEquals(dispositions(scoped(close, open, close, open)), Map(key -> "validated"))
    assertEquals(dispositions(scoped(rows.filterNot(row => Set(open, close)(row))*)), Map.empty)

  test("correlation cannot join observations across deployments or unknown provenance"):
    assertEquals(dispositions(List("deployment-a" -> open, "deployment-b" -> close, "" -> close)),
      Map(key -> "missing_terminal", ("deployment-b", correlation) -> "missing_open"))

  test("distinct terminals and changed versions of one source ID invalidate the pair"):
    val otherId = close.deepMerge(Json.obj("event_id" -> "00000000-0000-4000-8000-000000000199".asJson))
    val correction = close.deepMerge(Json.obj("bytes_up" -> 16.asJson))
    assertEquals(dispositions(scoped(open, close, otherId))(key), "ambiguous")
    assertEquals(dispositions(scoped(open, close, correction))(key), "ambiguous")
    assertEquals(dispositions(scoped(open, close, otherId), overflow = true)(key), "ambiguous")
    assertEquals(dispositions(scoped(open, close, otherId).filterNot(_._2 == otherId))(key), "validated")

  test("validate source identity, event sequence, host and chronological order"):
    val invalidSequence = close.deepMerge(Json.obj("event_sequence" -> 1.asJson))
    val invalidSource = close.mapObject(_.remove("event_id"))
    val invalidCorrelation = close.deepMerge(Json.obj("correlation_id" -> "not-a-uuid".asJson))
    val differentHost = close.deepMerge(Json.obj("host" -> "other.example".asJson))
    assertEquals(dispositions(scoped(open, invalidSequence))(key), "ineligible")
    assertEquals(dispositions(scoped(open, invalidSource))(key), "ineligible")
    assertEquals(dispositions(scoped(open, invalidCorrelation))(key), "missing_terminal")
    assertEquals(dispositions(scoped(open, differentHost))(key), "inconsistent")
    assertEquals(dispositions(scoped(open, withTime(close, openAt.minusMillis(1))))(key), "inconsistent")
    assertEquals(dispositions(scoped(open, withTime(close, openAt)))(key), "validated")

  test("persisted conflicts keep every affected correlation ambiguous after first-payload ingestion"):
    val otherCorrelation = "00000000-0000-4000-8000-000000000200"
    val otherKey = ("deployment-a", otherCorrelation)
    val otherOpen = open.deepMerge(Json.obj(
      "event_id" -> "00000000-0000-4000-8000-000000000201".asJson,
      "correlation_id" -> otherCorrelation.asJson))
    val otherClose = close.deepMerge(Json.obj(
      "event_id" -> "00000000-0000-4000-8000-000000000202".asJson,
      "correlation_id" -> otherCorrelation.asJson))
    val conflictId = text(close, "event_id").get
    val conflicts = Map(("deployment-a", conflictId) -> Set(correlation, otherCorrelation))
    assertEquals(dispositions(scoped(open, close, otherOpen, otherClose), unresolvedConflicts = conflicts),
      Map(key -> "ambiguous", otherKey -> "ambiguous"))
    assertEquals(dispositions(scoped(open, close), unresolvedConflicts = conflicts),
      Map(key -> "ambiguous", otherKey -> "ambiguous"))
    assertEquals(dispositions(Nil, unresolvedConflicts = conflicts),
      Map(key -> "ambiguous", otherKey -> "ambiguous"))
    assertEquals(dispositions(List("deployment-b" -> open, "deployment-b" -> close),
      unresolvedConflicts = conflicts),
      Map(("deployment-b", correlation) -> "validated", key -> "ambiguous", otherKey -> "ambiguous"))
    assertEquals(dispositions(scoped(open, close)), Map(key -> "validated"))

  test("an unresolved source ID prevents validation even without a usable conflicting correlation"):
    val conflicts = Map(("deployment-a", text(close, "event_id").get) -> Set.empty[String])
    assertEquals(dispositions(scoped(open, close), unresolvedConflicts = conflicts), Map(key -> "ambiguous"))

  test("payload fields cannot supply trusted ingestion conflict markers"):
    val spoofed = close.deepMerge(Json.obj("source_conflicts" -> Json.obj("untrusted" -> correlation.asJson)))
    assertEquals(dispositions(scoped(open, spoofed)), Map(key -> "validated"))

  test("a pairing horizon includes its endpoint but half-open source coverage must extend past it"):
    val last = openAt.plus(Horizon)
    assertEquals(dispositions(scoped(open, withTime(close, last)))(key), "validated")
    assertEquals(dispositions(scoped(open, withTime(close, last.plusNanos(1))))(key), "outside_horizon")
    assertEquals(dispositions(scoped(open, withTime(close, last)), covered = Some((openAt, last)))(key),
      "coverage_limited")
    // These are distinct attested bounds in the reference's Instant domain, not an endpoint conversion.
    assertEquals(dispositions(scoped(open, withTime(close, last)),
      covered = Some((openAt, last.plusNanos(1))))(key), "validated")
    assertEquals(dispositions(scoped(open, withTime(close, last)),
      covered = Some((openAt.plusNanos(1), last.plusNanos(1))))(key), "coverage_limited")
    assertEquals(dispositions(scoped(open), covered = Some((openAt, last)))(key), "coverage_limited")
    assertEquals(dispositions(scoped(close), covered = Some((openAt, last)))(key), "coverage_limited")
    assertEquals(dispositions(scoped(close), covered = Some((openAt, last)), retainedFrom = Some(openAt))(key),
      "retention_truncated")
    assertEquals(dispositions(scoped(open, close), covered = None)(key), "coverage_limited")
    assertEquals(dispositions(scoped(open, close), overflow = true)(key), "lookup_incomplete")
    assertEquals(dispositions(scoped(open))(key), "missing_terminal")
    assertEquals(dispositions(scoped(close))(key), "missing_open")
