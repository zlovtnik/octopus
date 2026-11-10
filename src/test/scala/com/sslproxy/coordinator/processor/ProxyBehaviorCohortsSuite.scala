package com.sslproxy.coordinator.processor

import com.sslproxy.coordinator.util.Sha256Utils
import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import munit.FunSuite

class ProxyBehaviorCohortsSuite extends FunSuite:
  private val start = Instant.parse("2026-11-01T12:00:00Z")
  private val end = start.plusSeconds(3600)
  private val policy = Json.obj(
    "version" -> "cohorts-fixture-v1".asJson,
    "calibration_reference" -> "synthetic-controls-not-production-calibration".asJson,
    "b11_acceptance_reference" -> "exact-filtered-reference-fixture".asJson,
    "minimum_destinations" -> 1.asJson,
    "minimum_observations" -> 1.asJson,
    "minimum_similarity" -> BigDecimal("0.5").asJson,
    "max_pairs" -> 256.asJson
  )

  private def row(
      scope: String,
      endpoint: String,
      count: Int = 1,
      deployment: String = "deployment-a",
      at: Instant = start
  ): ProxyBehavior.Projection =
    val key = Json
      .arr(deployment.asJson, scope.asJson, endpoint.asJson, at.toString.asJson)
      .noSpaces
    ProxyBehavior.Projection(
      key,
      Json.obj(
        "deployment_scope" -> deployment.asJson,
        "scope_kind" -> "registration".asJson,
        "scope_key" -> scope.asJson,
        "endpoint_key" -> endpoint.asJson,
        "normalization_version" -> ProxyAuthority.Version.asJson,
        "calculation_version" -> "proxy-behavior-v4".asJson,
        "policy_version" -> "source-fixture-v1".asJson,
        "window_start" -> at.toString.asJson,
        "window_end" -> at.plusSeconds(3600).toString.asJson,
        "coverage_start" -> at.toString.asJson,
        "coverage_end" -> at.plusSeconds(3600).toString.asJson,
        "valid_from" -> at.toString.asJson,
        "valid_until" -> at.plusSeconds(3600).toString.asJson,
        "ownership" -> "registration_claim_only".asJson,
        "disposition" -> "available".asJson,
        "observed_count" -> count.asJson
      ),
      "proxy registration window",
      Sha256Utils.sha256Hex(key + "-input-v1"),
      "available",
      1L,
      at
    )
  private def change(
      p: ProxyBehavior.Projection,
      fields: (String, Json)*
  ): ProxyBehavior.Projection =
    p.copy(evidence = p.evidence.deepMerge(Json.obj(fields*)))
  private def output(
      rows: List[ProxyBehavior.Projection],
      settings: Json = policy
  ): Map[String, Json] =
    ProxyBehaviorCohorts.evidence(rows, settings, conflictsAttested = true)
  private def matches(value: Json): List[Json] =
    value.hcursor.get[List[Json]]("matches").toOption.get
  private def pairs(values: Map[String, Json]): Set[(String, String)] =
    values.values.toList
      .flatMap(matches)
      .map { pair =>
        (
          pair.hcursor.downField("left").get[String]("scope_key").toOption.get,
          pair.hcursor.downField("right").get[String]("scope_key").toOption.get
        )
      }
      .toSet

  test(
    "exact direct destination similarities publish deterministic symmetric pairs"
  ):
    val rows = List(
      row("a" * 64, "api.example:443"),
      row("a" * 64, "update.example:443"),
      row("b" * 64, "api.example:443"),
      row("b" * 64, "update.example:443"),
      row("c" * 64, "different.example:443")
    )
    val result = output(rows)
    assertEquals(output(rows.reverse ++ rows), result)
    assertEquals(pairs(result), Set(("a" * 64, "b" * 64)))
    val pair = matches(result(rows.head.key)).head.hcursor
    assertEquals(
      pair.get[BigDecimal]("jaccard_similarity"),
      Right(BigDecimal(1))
    )
    assertEquals(pair.get[Int]("shared_destination_count"), Right(2))
    assertEquals(pair.get[String]("window_start"), Right(start.toString))
    assertEquals(pair.get[String]("pair_id").toOption.map(_.length), Some(64))
    assertEquals(
      result(rows.head.key).hcursor.get[String]("ownership"),
      Right("unavailable")
    )
    assert(
      result.values.forall(
        _.hcursor.downField("embedding_model").focus.exists(_.isNull)
      )
    )

  test("direct matches never create transitive cohort membership"):
    val rows = List(
      row("a" * 64, "a"),
      row("b" * 64, "a"),
      row("b" * 64, "b"),
      row("c" * 64, "b")
    )
    assertEquals(
      pairs(output(rows)),
      Set(("a" * 64, "b" * 64), ("b" * 64, "c" * 64))
    )
    assertEquals(matches(output(rows)(rows.head.key)).size, 1)

  test(
    "deployment window bounds and all preparation versions constrain compatibility"
  ):
    val left = row("a" * 64, "api")
    val right = row("b" * 64, "api")
    val incompatible = List(
      row("b" * 64, "api", deployment = "deployment-b"),
      row("b" * 64, "api", at = end),
      change(right, "window_end" -> end.plusSeconds(1).toString.asJson),
      change(right, "normalization_version" -> "different-v1".asJson),
      change(right, "calculation_version" -> "different-v1".asJson),
      change(right, "policy_version" -> "different-v1".asJson)
    )
    incompatible.foreach(other =>
      assertEquals(
        pairs(output(List(left, other))),
        Set.empty[(String, String)]
      )
    )
    val mixedScope = change(
      row("b" * 64, "other"),
      "calculation_version" -> "different-v1".asJson
    )
    assertEquals(
      pairs(output(List(left, right, mixedScope))),
      Set.empty[(String, String)]
    )

  test(
    "unsupported peers wireless rows and partial claim or coverage cannot establish membership"
  ):
    val left = row("a" * 64, "api")
    val right = row("b" * 64, "api")
    val excluded = List(
      change(right, "scope_kind" -> "peer".asJson),
      change(right, "scope_kind" -> "wireless".asJson),
      right.copy(disposition = "attribution_interval_limited"),
      change(right, "valid_until" -> end.minusSeconds(1).toString.asJson),
      change(right, "coverage_start" -> start.plusSeconds(1).toString.asJson),
      change(right, "ownership" -> "physical_owner".asJson)
    )
    excluded.foreach(other =>
      assertEquals(
        pairs(output(List(left, other))),
        Set.empty[(String, String)]
      )
    )
    // A partial endpoint invalidates the entire epoch/window destination set.
    val partialEndpoint = change(
      row("b" * 64, "other"),
      "coverage_end" -> end.minusSeconds(1).toString.asJson
    )
    assertEquals(
      pairs(output(List(left, right, partialEndpoint))),
      Set.empty[(String, String)]
    )

  test(
    "configured minimum destinations and observations exclude sparse or empty windows"
  ):
    val rows = List(row("a" * 64, "api"), row("b" * 64, "api"))
    assertEquals(
      pairs(
        output(
          rows,
          policy.deepMerge(Json.obj("minimum_observations" -> 2.asJson))
        )
      ),
      Set.empty[(String, String)]
    )
    assertEquals(
      pairs(
        output(
          rows,
          policy.deepMerge(Json.obj("minimum_destinations" -> 2.asJson))
        )
      ),
      Set.empty[(String, String)]
    )
    assertEquals(
      pairs(output(rows.map(p => change(p, "observed_count" -> 0.asJson)))),
      Set.empty[(String, String)]
    )

  test(
    "unattested conflicts suppress positive matches despite full coverage and registration"
  ):
    val rows = List(row("a" * 64, "api"), row("b" * 64, "api"))
    assertEquals(pairs(output(rows)), Set(("a" * 64, "b" * 64)))
    val result =
      ProxyBehaviorCohorts.evidence(rows, policy, conflictsAttested = false)
    assertEquals(pairs(result), Set.empty[(String, String)])
    assert(
      result.values.forall(
        _.hcursor
          .get[String]("disposition")
          .contains("ineligible_or_insufficient_samples")
      )
    )

  test("missing invalid unknown and nonnumeric configuration is fail closed"):
    val rows = List(row("a" * 64, "api"), row("b" * 64, "api"))
    val invalid = List(
      Json.Null,
      policy.mapObject(_.remove("version")),
      policy.deepMerge(Json.obj("unexpected" -> true.asJson)),
      policy.deepMerge(Json.obj("minimum_similarity" -> Json.Null)),
      policy.deepMerge(Json.obj("minimum_similarity" -> "NaN".asJson)),
      policy.deepMerge(Json.obj("minimum_similarity" -> 0.asJson)),
      policy.deepMerge(Json.obj("max_pairs" -> 257.asJson)),
      policy.deepMerge(Json.obj("calibration_reference" -> "".asJson)),
      policy.deepMerge(Json.obj("b11_acceptance_reference" -> "".asJson))
    )
    assert(
      output(rows, Json.obj()).values.forall(
        _.hcursor
          .get[String]("disposition")
          .contains("disabled_no_reviewed_policy")
      )
    )
    invalid.foreach { settings =>
      val result = output(rows, settings)
      assertEquals(pairs(result), Set.empty[(String, String)])
      assert(
        result.values.forall(
          _.hcursor.get[String]("disposition").contains("invalid_policy")
        )
      )
    }

  test(
    "conflicting or superseded projection versions cannot enter a partial positive set"
  ):
    val left = row("a" * 64, "api")
    val right = row("b" * 64, "api")
    val conflict = right.copy(fingerprint = "f" * 64)
    val result = output(List(left, right, conflict))
    assert(
      result.values.forall(
        _.hcursor
          .get[String]("disposition")
          .contains("conflicting_projection_versions")
      )
    )
    assertEquals(pairs(result), Set.empty[(String, String)])
    val sameEndpoint = right.copy(key = right.key + "-superseded")
    assertEquals(
      pairs(output(List(left, right, sameEndpoint))),
      Set.empty[(String, String)]
    )

  test(
    "source correction expiry removal and changed registration epochs replace old pairs"
  ):
    val left = row("a" * 64, "api")
    val right = row("b" * 64, "api")
    val original = output(List(left, right))
    val originalId = matches(original(left.key)).head.hcursor
      .get[String]("pair_id")
      .toOption
      .get
    assertEquals(
      pairs(output(List(left, row("b" * 64, "corrected")))),
      Set.empty[(String, String)]
    )
    assertEquals(pairs(output(List(left))), Set.empty[(String, String)])
    assertEquals(
      pairs(output(List(left, right.copy(disposition = "expired")))),
      Set.empty[(String, String)]
    )
    val rotated = output(List(left, row("c" * 64, "api")))
    val newId = matches(rotated(left.key)).head.hcursor
      .get[String]("pair_id")
      .toOption
      .get
    assertNotEquals(originalId, newId)
    val correction = right.copy(fingerprint = "e" * 64, sourceWatermark = 2)
    val correctedPair = matches(output(List(left, correction))(left.key)).head
    assertEquals(
      correctedPair.hcursor
        .downField("right")
        .downField("source_projections")
        .downArray
        .get[String]("input_fingerprint"),
      Right("e" * 64)
    )

  test(
    "source references are capped while exact endpoint and observation counts remain"
  ):
    val rows = List("a" * 64, "b" * 64).flatMap(scope =>
      (0 until 40).map(index => row(scope, s"endpoint-$index"))
    )
    val pair =
      matches(output(rows)(rows.head.key)).head.hcursor.downField("left")
    assertEquals(pair.get[Int]("destination_count"), Right(40))
    assertEquals(pair.get[Int]("observation_count"), Right(40))
    assertEquals(
      pair.get[List[Json]]("source_projections").toOption.get.size,
      32
    )
    assertEquals(pair.get[Int]("omitted_projection_reference_count"), Right(8))

  test(
    "pair and window bounds fail closed instead of publishing partial memberships"
  ):
    val rows =
      List("a" * 64, "b" * 64, "c" * 64).map(scope => row(scope, "api"))
    val result =
      output(rows, policy.deepMerge(Json.obj("max_pairs" -> 2.asJson)))
    assert(
      result.values.forall(
        _.hcursor.get[String]("disposition").contains("pair_bound_exceeded")
      )
    )
    assertEquals(pairs(result), Set.empty[(String, String)])
    val oversized = output(List.fill(169)(rows.head))
    assert(
      oversized.values.forall(
        _.hcursor.get[String]("disposition").contains("window_bound_exceeded")
      )
    )

  test(
    "filtered exact-reference benchmark has complete recall under identical restrictions"
  ):
    val definitions = List(
      "a" * 64 -> Set("a", "b"),
      "b" * 64 -> Set("a", "b"),
      "c" * 64 -> Set("b", "c"),
      "d" * 64 -> Set("d")
    )
    val rows = definitions.flatMap((scope, destinations) =>
      destinations.toList.map(destination => row(scope, destination))
    )
    val ineligible = List(
      row("e" * 64, "a", deployment = "other"),
      row("f" * 64, "a", at = end),
      row("0" * 64, "a").copy(disposition = "coverage_limited")
    )
    val expected = definitions.flatMap { (left, a) =>
      definitions.collect {
        case (right, b)
            if left < right &&
              a.intersect(b).size.toDouble / a.union(b).size >= 0.5 =>
          (left, right)
      }
    }.toSet
    val actual = pairs(output(rows ++ ineligible))
    assertEquals(actual, expected)
    assertEquals(actual.intersect(expected).size.toDouble / expected.size, 1.0)
