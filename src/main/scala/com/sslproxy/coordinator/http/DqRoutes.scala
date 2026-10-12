package com.sslproxy.coordinator.http

import cats.effect.IO
import cats.syntax.all.*
import com.sslproxy.coordinator.dq.*
import io.circe.Json
import io.circe.parser.parse
import org.http4s.{HttpRoutes, Request, Response, Status}
import org.http4s.circe.*
import org.http4s.dsl.io.*
import org.typelevel.ci.*
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** The manager authenticates users and forwards scoped identity over service
  * authentication.
  */
final class DqRoutes(
    store: DqStore,
    controlToken: String,
    artifactAccess: Json => IO[Json] = _ =>
      IO.raiseError(DqError(503, "artifact_access_unavailable"))
):
  private def header(
      request: Request[IO],
      name: org.typelevel.ci.CIString
  ): Option[String] =
    request.headers.get(name).map(_.head.value)
  private def principal(request: Request[IO]): IO[DqPrincipal] = IO {
    val supplied = header(request, ci"Authorization").getOrElse("")
    if controlToken.isBlank || !MessageDigest.isEqual(
        supplied.getBytes(StandardCharsets.UTF_8),
        ("Bearer " + controlToken).getBytes(StandardCharsets.UTF_8)
      )
    then throw DqError(401, "service_auth_required")
    header(request, ci"X-DQ-Actor") match
      case None        => DqPrincipal.internal
      case Some(actor) =>
        if !DqJson.hash(actor) then throw DqError(403, "invalid_actor")
        val roles = header(request, ci"X-DQ-Roles")
          .getOrElse("")
          .split(",")
          .map(_.trim)
          .filter(_.nonEmpty)
          .toSet
        val scopes = header(request, ci"X-DQ-Scopes")
          .getOrElse("")
          .split(",")
          .map(_.trim)
          .filter(_.nonEmpty)
          .toSet
        if roles.contains("service") || scopes.isEmpty then
          throw DqError(403, "invalid_principal")
        DqPrincipal(actor, roles, scopes)
  }
  private def body(request: Request[IO], maximum: Int = 65536): IO[Json] =
    request.body.take(maximum.toLong + 1).compile.toVector.flatMap { bytes =>
      if bytes.size > maximum then IO.raiseError(DqError(413, "record_too_large"))
      else
        IO.fromEither(
          parse(new String(bytes.toArray, StandardCharsets.UTF_8))
            .leftMap(_ => DqError(400, "invalid_json"))
        )
    }
  private def service(p: DqPrincipal): IO[Unit] = IO(
    p.requireRole(Set("service"))
  )
  private def queryScope(request: Request[IO]): IO[Unit] = IO {
    val q = request.uri.query.params
    if q.get("installation_id") != Some(store.settings.installationId) || q.get(
        "namespace"
      ) != Some(store.settings.namespace)
    then throw DqError(403, "forbidden_installation")
  }
  private def scopedBody(p: DqPrincipal, value: Json): IO[Unit] = IO(
    DqJson.scoped(p, value, store.settings)
  )
  private def execute(request: Request[IO], status: Status = Status.Ok)(
      run: DqPrincipal => IO[Json]
  ): IO[Response[IO]] =
    principal(request)
      .flatMap(run)
      .flatMap(result => IO.pure(Response[IO](status).withEntity(result)))
      .handleErrorWith {
        case DqError(code, error) =>
          IO.pure(
            Response[IO](
              Status.fromInt(code).getOrElse(Status.InternalServerError)
            )
              .withEntity(Json.obj("error" -> Json.fromString(error)))
          )
        case _ =>
          IO.pure(
            Response[IO](Status.ServiceUnavailable).withEntity(
              Json.obj("error" -> Json.fromString("control_unavailable"))
            )
          )
      }
  val routes: HttpRoutes[IO] = HttpRoutes.of[IO] {
    case req @ GET -> Root / "internal" / "dq" / "v1" / "invocations" / key =>
      execute(req)(p => queryScope(req) *> store.invocation(p, key))
    case req @ GET -> Root / "internal" / "dq" / "v1" / "processors" =>
      execute(req)(p => queryScope(req) *> store.catalog(p))
    case req @ POST -> Root / "internal" / "dq" / "v1" / "processor-versions" =>
      execute(req, Status.Created)(p => body(req).flatMap(store.register(p, _)))
    case req @ GET -> Root / "internal" / "dq" / "v1" / "processor-versions" / processor / version =>
      execute(req)(p =>
        queryScope(req) *> store.manifest(p, processor, version)
      )
    case req @ POST -> Root / "internal" / "dq" / "v1" / "runs" =>
      execute(req, Status.Accepted)(p =>
        IO(p.requireRole(Set("operator", "admin", "automation"))) *> body(req)
          .flatMap(store.admit(p, _))
      )
    case req @ GET -> Root / "internal" / "dq" / "v1" / "runs" / runId =>
      execute(req)(p => queryScope(req) *> store.getRun(p, runId))
    case req @ POST -> Root / "internal" / "dq" / "v1" / "runs" / runId / "cancel" =>
      execute(req)(p =>
        body(req).flatMap(value =>
          scopedBody(p, value) *> store.cancel(p, runId)
        )
      )
    case req @ POST -> Root / "internal" / "dq" / "v1" / "runs" / runId / "replay" =>
      execute(req, Status.Accepted)(p =>
        body(req).flatMap(value =>
          scopedBody(p, value) *> store.replay(p, runId, value)
        )
      )
    case req @ POST -> Root / "internal" / "dq" / "v1" / "receipts" =>
      execute(req)(p => service(p) *> body(req, 71680).flatMap(store.receiveReceipt))
    case req @ POST -> Root / "internal" / "dq" / "v1" / "workers" / worker / "heartbeat" =>
      execute(req)(p =>
        service(p) *> body(req).flatMap { value =>
          if DqJson.text(value, "worker_instance_id") != worker then
            IO.raiseError(DqError(400, "worker_identity_mismatch"))
          else store.heartbeat(value)
        }
      )
    case req @ GET -> Root / "internal" / "dq" / "v1" / "dispatches" =>
      execute(req)(p => service(p) *> queryScope(req) *> store.dispatches)
    case req @ POST -> Root / "internal" / "dq" / "v1" / "runs" / runId / "attempts" / "reserve" =>
      execute(req)(p =>
        service(p) *> body(req).flatMap(store.reserve(runId, _))
      )
    case req @ POST -> Root / "internal" / "dq" / "v1" / "attempts" / attempt / "accepted" =>
      execute(req)(p =>
        service(p) *> body(req).flatMap(store.accepted(attempt, _))
      )
    case req @ POST -> Root / "internal" / "dq" / "v1" / "attempts" / attempt / "renew" =>
      execute(req)(p =>
        service(p) *> body(req).flatMap(store.renew(attempt, _))
      )
    case req @ GET -> Root / "internal" / "dq" / "v1" / "artifacts" / artifactId =>
      execute(req)(p => queryScope(req) *> store.artifact(p, artifactId, false))
    case req @ POST -> Root / "internal" / "dq" / "v1" / "artifacts" / artifactId / "access" =>
      execute(req)(p =>
        body(req).flatMap(value =>
          scopedBody(p, value) *> store
            .artifact(p, artifactId, true)
            .flatMap(artifactAccess)
        )
      )
  }
