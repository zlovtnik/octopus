package com.sslproxy.coordinator.http

import cats.effect.IO
import com.sslproxy.coordinator.observability.*
import io.circe.parser.parse
import munit.CatsEffectSuite
import org.http4s.implicits.*
import org.http4s.{Header, Headers, Method, Request, Status, Uri}
import org.typelevel.ci.CIString

import scala.concurrent.duration.*

class PublicStatsRoutesSuite extends CatsEffectSuite:

  private def stubService: IO[OperationalStatsService] =
    val metrics = CoordinatorMetrics()
    val peaks = new PeaksSource:
      def peakRecordsDay = IO.pure(Some((6048436L, "2026-10-07")))
      def peakRecordsWeek = IO.pure(Some((10077299L, "2026-10-05", "2026-10-11")))
    OperationalStatsService.create(peaks, metrics, 60.seconds)

  private def routes(allowedOrigins: List[String] = List("https://rclabs.uk")): IO[PublicStatsRoutes] =
    stubService.map(service => new PublicStatsRoutes(service, allowedOrigins))

  private def get(routes: IO[PublicStatsRoutes], path: Uri, origin: Option[String] = None): IO[(Status, String, Headers)] =
    val headers = origin.map(o => Headers(Header.Raw(CIString("Origin"), o))).getOrElse(Headers.empty)
    routes.flatMap(_.routes.orNotFound.run(Request[IO](Method.GET, path, headers = headers))).flatMap { response =>
      response.bodyText.compile.string.map(body => (response.status, body, response.headers))
    }

  test("GET /public/stats returns stats JSON with expected fields"):
    get(routes(), uri"/public/stats").map { case (status, body, _) =>
      assertEquals(status, Status.Ok)
      val cursor = parse(body).toOption.map(_.hcursor)
      assert(cursor.isDefined, s"invalid JSON: $body")
      val c = cursor.get
      assert(c.get[String]("asOf").toOption.nonEmpty)
      assert(c.get[Long]("peakRecordsDay").toOption.nonEmpty)
      assert(c.get[Long]("peakRecordsWeek").toOption.nonEmpty)
      assert(c.downField("liveStrip").focus.exists(_.isNull))
    }

  test("GET /public/stats sets Cache-Control header"):
    get(routes(), uri"/public/stats").map { case (_, _, headers) =>
      assert(headers.get(CIString("Cache-Control")).map(_.head.value).contains("no-store"))
    }

  test("GET /public/stats echoes CORS origin for allowed origins"):
    get(routes(), uri"/public/stats", origin = Some("https://rclabs.uk")).map { case (_, _, headers) =>
      assert(headers.get(CIString("Access-Control-Allow-Origin")).map(_.head.value).contains("https://rclabs.uk"))
    }

  test("GET /public/stats does not echo CORS origin for disallowed origins"):
    get(routes(), uri"/public/stats", origin = Some("https://evil.example")).map { case (_, _, headers) =>
      assert(headers.get(CIString("Access-Control-Allow-Origin")).isEmpty)
    }

  test("OPTIONS /public/stats responds with CORS headers for allowed origin"):
    routes().flatMap(_.routes.orNotFound
      .run(Request[IO](Method.OPTIONS, uri"/public/stats", headers = Headers(Header.Raw(CIString("Origin"), "https://rclabs.uk")))))
      .flatMap { response =>
        IO {
          assertEquals(response.status, Status.NoContent)
          assert(response.headers.get(CIString("Access-Control-Allow-Origin")).map(_.head.value).contains("https://rclabs.uk"))
          assert(response.headers.get(CIString("Access-Control-Allow-Methods")).map(_.head.value).contains("GET, OPTIONS"))
        }
      }

  test("database failure returns sanitized unavailable JSON and no cache"):
    val source = new PeaksSource:
      def peakRecordsDay = IO.raiseError(RuntimeException("private database details"))
      def peakRecordsWeek = IO.pure(None)
    val failed = OperationalStatsService.create(source, CoordinatorMetrics(), 60.seconds)
      .map(service => new PublicStatsRoutes(service, List("https://rclabs.uk")))
    get(failed, uri"/public/stats", Some("https://rclabs.uk")).map { case (status, body, headers) =>
      assertEquals(status, Status.ServiceUnavailable)
      assertEquals(body, "{\"error\":\"Metrics unavailable\"}")
      assert(headers.get(CIString("Cache-Control")).map(_.head.value).contains("no-store"))
      assert(headers.get(CIString("Access-Control-Allow-Origin")).nonEmpty)
    }
