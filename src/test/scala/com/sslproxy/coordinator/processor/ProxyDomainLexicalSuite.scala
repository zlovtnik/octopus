package com.sslproxy.coordinator.processor

import io.circe.Json
import io.circe.syntax.*
import java.time.Instant
import munit.FunSuite

class ProxyDomainLexicalSuite extends FunSuite:
  private val start = Instant.parse("2026-11-01T12:00:00Z")
  private def row(id: String, host: String, offset: Long = 0): Json = Json.obj(
    "event_id" -> id.asJson, "host" -> host.asJson,
    "authority_source" -> "http_authority".asJson,
    "time" -> start.plusSeconds(offset).toString.asJson, "type" -> "http_proxied".asJson
  )
  private def evidence(current: List[Json], history: List[Json] = Nil): Json =
    ProxyDomainLexical.evidence(current, history, true, current, history)
  private val detectorPolicy = Json.obj(
    "version" -> "fixture-policy-v1".asJson,
    "calibration_reference" -> "synthetic-review-only".asJson,
    "heldout_dataset" -> "lexical-controls-v1".asJson,
    "trained_until" -> start.minusSeconds(3600).toString.asJson,
    "min_label_length" -> 24.asJson, "min_entropy" -> 3.0.asJson,
    "min_digit_fraction" -> 0.2.asJson, "max_new_subdomains" -> 2.asJson,
    "minimum_samples" -> 1.asJson)
  private def detected(current: List[Json], history: List[Json] = Nil,
      policy: Json = detectorPolicy, complete: Boolean = true): Json =
    ProxyDomainLexical.evidence(current, history, complete, current, history, policy)

  test("label features use normalized ASCII with finite exact lengths and fractions"):
    val result = evidence(List(row("one", "Abc123.Example.COM.")))
    val host = result.hcursor.downField("hosts").downArray
    val label = host.downField("labels").downArray
    assertEquals(host.get[String]("host_normalized"), Right("abc123.example.com"))
    assertEquals(host.get[Int]("maximum_label_length"), Right(7))
    assertEquals(label.get[Int]("length"), Right(6))
    assertEquals(label.get[Double]("digit_fraction"), Right(0.5))
    assertEqualsDouble(label.get[Double]("shannon_entropy_bits_per_character").toOption.get, math.log(6.0) / math.log(2.0), 0.000001)
    assert(!result.noSpaces.contains("NaN"))
    assert(!result.noSpaces.contains("Infinity"))

  test("invalid sanitized and IP-literal hosts remain unavailable"):
    val inputs = List(row("ip4", "192.0.2.1"), row("ip6", "[2001:db8::1]"),
      row("invalid", "bad host.example"), row("token", "hmac:opaque"),
      row("sanitized", "safe.example.com").deepMerge(Json.obj("sanitization_status" -> "sanitized".asJson)))
    val result = evidence(inputs)
    assertEquals(result.hcursor.get[Int]("eligible_dns_observation_count"), Right(0))
    assertEquals(result.hcursor.get[String]("disposition"), Right("dns_input_unavailable"))
    assertEquals(result.hcursor.get[List[Json]]("hosts"), Right(Nil))

  test("unicode and private PSL boundaries use the existing normalization contract"):
    val result = evidence(List(row("unicode", "b\u00fccher.example"), row("private", "a.tenant.github.io")))
    val hosts = result.hcursor.get[List[Json]]("hosts").toOption.get
    assert(hosts.exists(_.hcursor.get[String]("host_normalized").contains("xn--bcher-kva.example")))
    val domains = result.hcursor.get[List[Json]]("subdomain_churn").toOption.get
    assert(domains.exists(_.hcursor.get[String]("registrable_domain").contains("tenant.github.io")))

  test("subdomain churn compares bounded domain scope to earlier retained observations"):
    val endpoint = row("current", "new.example.com", 60)
    val otherCurrent = row("other", "other.example.com", 120)
    val prior = row("prior", "old.example.com", -60)
    val otherDomain = row("outside", "unrelated.example.net", -60)
    val result = ProxyDomainLexical.evidence(List(endpoint), List(prior), true,
      List(endpoint, otherCurrent), List(prior, otherDomain))
    val churn = result.hcursor.downField("subdomain_churn").downArray
    assertEquals(churn.get[Int]("current_subdomain_count"), Right(2))
    assertEquals(churn.get[Int]("prior_subdomain_count"), Right(1))
    assertEquals(churn.get[List[String]]("new_to_retained_history_subdomains"), Right(List("new.example.com", "other.example.com")))
    assertEquals(churn.get[List[String]]("baseline_event_ids"), Right(List("prior")))
    assertEquals(churn.get[Boolean]("new_to_scope_ever").toOption, None)

  test("missing domain inputs and missing history do not manufacture churn baselines"):
    val current = List(row("now", "new.example.com"))
    val missingScope = ProxyDomainLexical.evidence(current, Nil, true)
    val churn = missingScope.hcursor.downField("subdomain_churn").downArray
    assertEquals(churn.get[String]("disposition"), Right("domain_scope_unavailable"))
    assertEquals(churn.get[Int]("current_subdomain_count").toOption, None)
    val missingHistory = evidence(current).hcursor.downField("subdomain_churn").downArray
    assertEquals(missingHistory.get[String]("disposition"), Right("insufficient_retained_history"))
    assertEquals(missingHistory.get[List[String]]("new_to_retained_history_subdomains").toOption, None)

  test("future or equal-time baseline rows cannot leak into prior-host novelty"):
    val current = row("current", "new.example.com", 60)
    val future = row("future", "new.example.com", 120)
    val equal = row("equal", "new.example.com", 60)
    val prior = row("prior", "old.example.com", -60)
    val result = evidence(List(current), List(future, equal, prior))
    assertEquals(result.hcursor.get[List[String]]("baseline_event_ids"), Right(List("prior")))
    assertEquals(result.hcursor.downField("subdomain_churn").downArray.get[List[String]]("new_to_retained_history_subdomains"), Right(List("new.example.com")))

  test("replay input order and duplicate rows preserve evidence; conflicting IDs are excluded"):
    val current = List(row("a", "a.example.com"), row("b", "b.example.com", 10))
    val history = List(row("c", "c.example.com", -60))
    assertEquals(evidence(current, history), evidence(current.reverse ++ current, history ++ history))
    val conflict = evidence(List(row("same", "a.example.com"), row("same", "b.example.com")))
    assertEquals(conflict.hcursor.get[Int]("eligible_dns_observation_count"), Right(0))

  test("CDN and telemetry holdouts remain descriptive despite high entropy and digits"):
    val controls = List("a9f032e1b8c4d567890abcdef12345678.cdn.example.com", "012345678901234567890123456789.telemetry.example.net")
    controls.zipWithIndex.foreach { (host, index) =>
      val result = evidence(List(row(s"control-$index", host)))
      assertEquals(result.hcursor.get[Boolean]("candidate").toOption, None)
      assertEquals(result.hcursor.get[String]("calibration_status"), Right("no_reviewed_detector_threshold"))
      assert(result.hcursor.downField("false_positive_rate").focus.exists(_.isNull))
      assertEquals(result.hcursor.get[String]("false_positive_disposition"), Right("unavailable_without_detector_and_reviewed_labels"))
      assertEquals(result.hcursor.get[Int]("eligible_dns_observation_count"), Right(1))
    }

  test("source bounds and collection censoring remain explicit"):
    val now = row("now", "new.example.com")
    val prior = row("prior", "old.example.com", -60)
    val censored = ProxyDomainLexical.evidence(List(now), List(prior), false, List(now), List(prior))
    assertEquals(censored.hcursor.downField("subdomain_churn").downArray.get[String]("disposition"), Right("collection_censored"))
    assertEquals(ProxyDomainLexical.evidence(List.fill(10001)(now), Nil, true).hcursor.get[String]("disposition"), Right("input_bound_exceeded"))

  test("reference and host lists are bounded without truncating exact counts"):
    val current = (1 to 150).map(index => row(s"now-$index", s"new$index.example.com", index.toLong)).toList
    val prior = (1 to 140).map(index => row(s"prior-$index", s"old$index.example.com", -index.toLong)).toList
    val result = evidence(current, prior)
    val churn = result.hcursor.downField("subdomain_churn").downArray
    assertEquals(result.hcursor.get[Int]("eligible_dns_observation_count"), Right(150))
    assertEquals(result.hcursor.get[Int]("distinct_host_count"), Right(150))
    assertEquals(result.hcursor.get[List[String]]("source_event_ids").toOption.get.size, 64)
    assertEquals(result.hcursor.get[Int]("source_event_ids_omitted"), Right(86))
    assertEquals(result.hcursor.get[List[String]]("baseline_event_ids").toOption.get.size, 64)
    assertEquals(result.hcursor.get[Int]("baseline_event_ids_omitted"), Right(76))
    assertEquals(result.hcursor.get[List[Json]]("hosts").toOption.get.size, 128)
    assertEquals(result.hcursor.get[Int]("hosts_omitted"), Right(22))
    assertEquals(churn.get[Int]("current_distinct_host_count"), Right(150))
    assertEquals(churn.get[Int]("prior_distinct_host_count"), Right(140))
    assertEquals(churn.get[Int]("current_subdomain_count"), Right(150))
    assertEquals(churn.get[Int]("prior_subdomain_count"), Right(140))
    assertEquals(churn.get[List[String]]("current_distinct_hosts").toOption.get.size, 128)
    assertEquals(churn.get[Int]("current_distinct_hosts_omitted"), Right(22))
    assertEquals(churn.get[List[String]]("prior_distinct_hosts").toOption.get.size, 128)
    assertEquals(churn.get[Int]("prior_distinct_hosts_omitted"), Right(12))
    assertEquals(churn.get[Int]("new_to_retained_history_subdomain_count"), Right(150))
    assertEquals(churn.get[List[String]]("new_to_retained_history_subdomains").toOption.get.size, 128)
    assertEquals(churn.get[Int]("new_to_retained_history_subdomains_omitted"), Right(22))
    assertEquals(churn.get[List[String]]("source_event_ids").toOption.get.size, 64)
    assertEquals(churn.get[Int]("source_event_ids_omitted"), Right(86))
    assertEquals(churn.get[List[String]]("baseline_event_ids").toOption.get.size, 64)
    assertEquals(churn.get[Int]("baseline_event_ids_omitted"), Right(76))

  test("opt-in detector reports versioned label candidates without threat confidence"):
    val longHost = "abc123def456ghi789jkl012mno345pqr678.example.org"
    val result = detected(List(row("candidate", longHost)))
    val detector = result.hcursor.downField("detector")
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(true))
    assertEquals(detector.get[String]("disposition"), Right("evaluated_candidate_only"))
    assertEquals(detector.get[String]("policy_version"), Right("fixture-policy-v1"))
    assertEquals(detector.get[String]("calibration_reference"), Right("synthetic-review-only"))
    assertEquals(detector.get[Int]("matched_label_count"), Right(1))
    assertEquals(detector.get[Int]("sample_size"), Right(1))
    assertEquals(detector.downField("thresholds").get[Int]("min_label_length"), Right(24))
    assert(result.hcursor.downField("false_positive_rate").focus.exists(_.isNull))
    assertEquals(detected(List(row("normal", "download.updates.example.com"))).hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(result.hcursor.get[String]("interpretation"), Right("lexical_anomaly_candidate_only_not_confirmed_threat"))
    assertEquals(evidence(List(row("disabled", longHost))).hcursor.downField("detector").get[String]("disposition"), Right("disabled_no_policy"))

  test("invalid missing unknown or nonfinite policy thresholds fail closed"):
    val input = List(row("candidate", "abc123def456ghi789jkl012mno345pqr678.example.org"))
    val malformed = List(
      detectorPolicy.mapObject(_.remove("version")),
      detectorPolicy.deepMerge(Json.obj("unexpected" -> true.asJson)),
      detectorPolicy.deepMerge(Json.obj("min_label_length" -> 64.asJson)),
      detectorPolicy.deepMerge(Json.obj("min_label_length" -> "24".asJson)),
      detectorPolicy.deepMerge(Json.obj("min_entropy" -> Json.fromDoubleOrNull(Double.PositiveInfinity))),
      detectorPolicy.deepMerge(Json.obj("min_entropy" -> "3.0".asJson)),
      detectorPolicy.deepMerge(Json.obj("min_entropy" -> (-0.1).asJson)),
      detectorPolicy.deepMerge(Json.obj("min_digit_fraction" -> 1.1.asJson)),
      detectorPolicy.deepMerge(Json.obj("minimum_samples" -> 0.asJson)),
      detectorPolicy.deepMerge(Json.obj("max_new_subdomains" -> 10001.asJson)),
      detectorPolicy.deepMerge(Json.obj("trained_until" -> "not-a-time".asJson)),
      detectorPolicy.deepMerge(Json.obj("calibration_reference" -> "".asJson)))
    malformed.foreach { policy =>
      val result = detected(input, policy = policy)
      assert(result.hcursor.downField("candidate").focus.exists(_.isNull))
      assertEquals(result.hcursor.downField("detector").get[String]("disposition"), Right("invalid_policy"))
    }

  test("training leakage coverage and minimum samples prevent candidate claims"):
    val input = List(row("candidate", "abc123def456ghi789jkl012mno345pqr678.example.org"))
    val futureTraining = detectorPolicy.deepMerge(Json.obj("trained_until" -> start.toString.asJson))
    val small = detectorPolicy.deepMerge(Json.obj("minimum_samples" -> 2.asJson))
    val censored = detected(input, complete = false)
    assertEquals(detected(input, policy = futureTraining).hcursor.downField("detector")
      .get[String]("disposition"), Right("training_cutoff_overlaps_evaluation"))
    assertEquals(detected(input, policy = small).hcursor.downField("detector")
      .get[String]("disposition"), Right("insufficient_samples"))
    assertEquals(censored.hcursor.downField("detector").get[String]("disposition"), Right("collection_censored"))
    assert(censored.hcursor.downField("candidate").focus.exists(_.isNull))

  test("new-subdomain rule requires earlier history and both domain slice sample bounds"):
    val policy = detectorPolicy.deepMerge(Json.obj("min_label_length" -> 63.asJson, "minimum_samples" -> 3.asJson))
    val current = List("a", "b", "c").zipWithIndex.map((label, index) => row(s"now-$index", s"$label.example.com", index.toLong))
    val prior = (1 to 3).map(index => row(s"prior-$index", "old.example.com", -index.toLong)).toList
    val result = detected(current, prior, policy)
    assertEquals(result.hcursor.get[Boolean]("candidate"), Right(true))
    assertEquals(result.hcursor.downField("detector").get[Int]("churn_match_count"), Right(1))
    assertEquals(detected(current, prior.take(2), policy).hcursor.get[Boolean]("candidate"), Right(false))
    assertEquals(detected(current, Nil, policy).hcursor.get[Boolean]("candidate"), Right(false))
    val replay = detected(current.reverse ++ current, prior.reverse ++ prior, policy)
    assertEquals(replay, result)

  test("heldout CDN telemetry and software controls report actual false positives"):
    val cases = List(
      ProxyDomainLexical.EvaluationCase("cdn", List(row("cdn", "a9f032e1b8c4d567890abcdef12345678.cdn.example.com")), Nil, false, "cdn"),
      ProxyDomainLexical.EvaluationCase("telemetry", List(row("telemetry", "012345678901234567890123456789.telemetry.example.net")), Nil, false, "telemetry"),
      ProxyDomainLexical.EvaluationCase("software", List(row("software", "download.updates.example.com")), Nil, false, "software_update"),
      ProxyDomainLexical.EvaluationCase("positive", List(row("positive", "abc123def456ghi789jkl012mno345pqr678.example.org")), Nil, true, "synthetic_positive"),
      ProxyDomainLexical.EvaluationCase("miss", List(row("miss", "short.example.org")), Nil, true, "synthetic_positive"))
    val report = ProxyDomainLexical.evaluate(detectorPolicy, cases)
    val metrics = report.hcursor.downField("metrics")
    assertEquals(metrics.get[Int]("evaluated_count"), Right(5))
    assertEquals(metrics.get[Int]("negative_count"), Right(3))
    assertEquals(metrics.get[Int]("false_positive"), Right(2))
    assertEquals(metrics.get[Int]("true_negative"), Right(1))
    assertEquals(metrics.get[Int]("true_positive"), Right(1))
    assertEquals(metrics.get[Int]("false_negative"), Right(1))
    assertEqualsDouble(metrics.get[Double]("false_positive_rate").toOption.get, 2.0 / 3.0, 0.000001)
    assertEquals(report.hcursor.downField("groups").downField("cdn").get[Int]("false_positive"), Right(1))
    assertEquals(report.hcursor.downField("groups").downField("telemetry").get[Int]("false_positive"), Right(1))
    assertEquals(report.hcursor.get[String]("label_provenance"), Right("caller_supplied_not_independently_reviewed"))
    assertEquals(ProxyDomainLexical.evaluate(detectorPolicy, cases.reverse), report)

  test("evaluation excludes unavailable cases and never invents a zero negative rate"):
    val unavailable = ProxyDomainLexical.EvaluationCase("missing", List(row("missing", "192.0.2.1")), Nil, false, "missing")
    val positive = ProxyDomainLexical.EvaluationCase("positive", List(row("positive", "abc123def456ghi789jkl012mno345pqr678.example.org")), Nil, true, "positive")
    val report = ProxyDomainLexical.evaluate(detectorPolicy, List(unavailable, positive)).hcursor.downField("metrics")
    assertEquals(report.get[Int]("excluded_count"), Right(1))
    assertEquals(report.get[Int]("negative_count"), Right(0))
    assert(report.downField("false_positive_rate").focus.exists(_.isNull))
    assertEquals(ProxyDomainLexical.evaluate(detectorPolicy, List(positive, positive)).hcursor.get[String]("disposition"), Right("invalid_evaluation_input"))
    assertEquals(ProxyDomainLexical.evaluate(Json.obj(), List(positive)).hcursor.get[String]("disposition"), Right("invalid_evaluation_input"))

  test("matched label details are bounded while exact match counts remain available"):
    val policy = detectorPolicy.deepMerge(Json.obj("min_label_length" -> 1.asJson,
      "min_entropy" -> 0.asJson, "min_digit_fraction" -> 0.asJson))
    val current = (1 to 150).map(index => row(s"row-$index", s"host$index.example.com", index.toLong)).toList
    val result = detected(current, policy = policy).hcursor.downField("detector")
    assertEquals(result.get[Int]("matched_label_count"), Right(450))
    assertEquals(result.get[List[Json]]("matched_labels").toOption.get.size, 128)
    assertEquals(result.get[Int]("matched_labels_omitted"), Right(322))
