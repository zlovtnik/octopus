package com.sslproxy.coordinator.processor

import munit.FunSuite

class ProjectionFunctionsSuite extends FunSuite:
  test("percentiles and jitter are deterministic and order independent"):
    val values = Vector(40.0d, 10.0d, 30.0d, 20.0d)
    assertEquals(ProjectionFunctions.percentile(values, 0.5d), Some(25.0d))
    assertEquals(
      ProjectionFunctions.percentile(values, 0.95d),
      ProjectionFunctions.percentile(values.reverse, 0.95d)
    )
    assertEquals(ProjectionFunctions.medianAbsoluteDeviation(values), Some(10.0d))

  test("frame normalization stays inside the locked 13-token vocabulary"):
    val tokens = for
      frameType <- Vector(None, Some("data"), Some("management"))
      subtype <- Vector(None, Some("probe-req"), Some("beacon"), Some("deauth"))
    yield FrameToken.normalize(frameType, subtype)

    assertEquals(FrameToken.values.length, 13)
    assert(tokens.forall(FrameToken.values.contains))
    assertEquals(FrameToken.normalize(Some("data"), None), FrameToken.Data)
    assertEquals(FrameToken.normalize(None, Some("beacon")), FrameToken.Beacon)

  test("transition probabilities sum to one for every previous token"):
    val probabilities = ProjectionFunctions.transitionProbabilities(
      Vector(
        FrameToken.Beacon,
        FrameToken.ProbeRequest,
        FrameToken.Beacon,
        FrameToken.ProbeResponse,
        FrameToken.Beacon
      )
    )
    probabilities.groupMap(_._1._1)(_._2).foreachEntry { (_, values) =>
      assertEqualsDouble(values.sum, 1.0d, 0.0000001d)
    }

  test("connected components are stable across edge ordering and direction"):
    val edges = Vector("b" -> "c", "a" -> "b", "z" -> "y")
    val expected = Vector(Vector("a", "b", "c"), Vector("y", "z"))
    assertEquals(ProjectionFunctions.connectedComponents(edges), expected)
    assertEquals(ProjectionFunctions.connectedComponents(edges.reverse.map(_.swap)), expected)

  test("risk and cosine calculations are bounded"):
    assertEquals(ProjectionFunctions.weightedRisk(Vector(200.0d -> 0.5d, 80.0d -> 0.75d)), 100.0d)
    assertEquals(ProjectionFunctions.cosineSimilarity(Vector(1.0d, 0.0d), Vector(1.0d, 0.0d)), Some(1.0d))
    assertEquals(ProjectionFunctions.cosineSimilarity(Vector.empty, Vector.empty), None)

  test("stable identifiers are repeatable and namespace separated"):
    val first = ProjectionFunctions.stableId("cluster", Vector("aa", "bb"))
    assertEquals(first, ProjectionFunctions.stableId("cluster", Vector("aa", "bb")))
    assertNotEquals(first, ProjectionFunctions.stableId("pair", Vector("aa", "bb")))
    assertEquals(first.length, 36)
