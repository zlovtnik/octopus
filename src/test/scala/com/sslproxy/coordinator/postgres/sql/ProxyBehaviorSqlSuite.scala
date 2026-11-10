package com.sslproxy.coordinator.postgres.sql

import io.circe.Json
import java.time.Instant
import munit.FunSuite

class ProxyBehaviorSqlSuite extends FunSuite:
  private val now = Instant.parse("2031-01-02T12:00:00Z")
  private def detail(at: Instant): Option[String] =
    Some(Json.obj("generation_as_of" -> Json.fromString(at.toString)).noSpaces)

  test("recent generations resume through the inclusive one-hour boundary"):
    List(now, now.minusSeconds(10), now.minusSeconds(3600)).foreach { prior =>
      assertEquals(
        ProxyBehaviorSql.resumeGenerationAsOf(detail(prior), now),
        prior
      )
    }

  test("expired and future generations start at the current cutoff"):
    List(
      now.minusSeconds(3600).minusNanos(1),
      now.minusSeconds(86400),
      now.plusNanos(1)
    )
      .foreach { prior =>
        assertEquals(
          ProxyBehaviorSql.resumeGenerationAsOf(detail(prior), now),
          now
        )
      }

  test(
    "missing or malformed generation timestamps start at the current cutoff"
  ):
    List(
      None,
      Some("invalid json"),
      Some("{}"),
      Some("""{"generation_as_of":42}"""),
      Some("""{"generation_as_of":"not-an-instant"}""")
    ).foreach { previous =>
      assertEquals(ProxyBehaviorSql.resumeGenerationAsOf(previous, now), now)
    }
