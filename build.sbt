ThisBuild / organization := "com.sslproxy"
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "3.3.8"
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision
ThisBuild / evictionErrorLevel := Level.Error

ThisBuild / wartremoverErrors ++= Warts.allBut(
  Wart.Any,
  Wart.Nothing,
  Wart.ImplicitParameter,
  Wart.DefaultArguments,
  Wart.NonUnitStatements,
  Wart.Null,
  Wart.ToString,
  Wart.AsInstanceOf,
  Wart.IsInstanceOf,
  Wart.MutableDataStructures,
  Wart.Overloading,
  Wart.Var,
  Wart.While,
  Wart.Throw,
  Wart.Return,
  Wart.OptionPartial,
  Wart.EitherProjectionPartial,
  Wart.StringPlusAny,
  Wart.FinalCaseClass,
  Wart.ArrayEquals,
  Wart.JavaSerializable,
  Wart.Serializable,
  Wart.Product,
  Wart.SortedMaxMin,
  Wart.LeakingSealed,
  Wart.PlatformDefault,
  Wart.Equals,
  Wart.ListAppend,
  Wart.Recursion,
  Wart.Option2Iterable,
  Wart.IterableOps,
  Wart.SeqApply,
  Wart.SizeIs,
  Wart.RedundantIsInstanceOf
)

val catsEffectVersion = "3.7.0"
val fs2Version = "3.13.0"
val fs2KafkaVersion = "3.5.1"
val declineVersion = "2.5.0"
val circeVersion = "0.14.14"
val doobieVersion = "1.0.0-RC10"
val http4sVersion = "0.23.34"
val mysqlJdbcVersion = "9.2.0"

val hikariCpVersion = "6.2.1"
val log4CatsVersion = "2.8.0"
val slf4jVersion = "2.0.18"
val pureconfigVersion = "0.17.8"
val micrometerVersion = "1.13.2"
val testcontainersVersion = "1.20.6"
val minioVersion = "9.0.3"
val openTelemetryVersion = "1.62.0"
val snakeYamlVersion = "2.3"

lazy val root = (project in file("."))
  .settings(
    name := "octopus",
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % catsEffectVersion,
      "co.fs2" %% "fs2-core" % fs2Version,
      "co.fs2" %% "fs2-io" % fs2Version,
      "com.github.fd4s" %% "fs2-kafka" % fs2KafkaVersion,
      "org.tpolecat" %% "doobie-core" % doobieVersion,
      "org.tpolecat" %% "doobie-hikari" % doobieVersion,
      "com.monovore" %% "decline" % declineVersion,
      "com.monovore" %% "decline-effect" % declineVersion,
      "com.github.pureconfig" %% "pureconfig-core" % pureconfigVersion,
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "org.http4s" %% "http4s-ember-server" % http4sVersion,
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "com.mysql" % "mysql-connector-j" % mysqlJdbcVersion,
      "com.zaxxer" % "HikariCP" % hikariCpVersion,
      "io.micrometer" % "micrometer-core" % micrometerVersion,
      "io.minio" % "minio" % minioVersion,
      "com.squareup.okhttp3" % "okhttp-jvm" % "5.3.2",
      "io.opentelemetry" % "opentelemetry-sdk" % openTelemetryVersion,
      "io.opentelemetry" % "opentelemetry-exporter-otlp" % openTelemetryVersion,
      "io.opentelemetry" % "opentelemetry-sdk-extension-autoconfigure" % openTelemetryVersion,
      "org.typelevel" %% "log4cats-slf4j" % log4CatsVersion,
      "ch.qos.logback" % "logback-classic" % "1.5.38",
      "net.logstash.logback" % "logstash-logback-encoder" % "9.0",
      "org.scalameta" %% "munit" % "1.3.4" % Test,
      "org.typelevel" %% "munit-cats-effect" % "2.1.0" % Test,
      "org.yaml" % "snakeyaml" % snakeYamlVersion % Test,
      "org.testcontainers" % "kafka" % testcontainersVersion % Test,
      "org.testcontainers" % "testcontainers" % testcontainersVersion % Test
    ),
    Compile / unmanagedSources / excludeFilter := "*.java",
    Test / unmanagedSources / excludeFilter := "*.java",
    scalacOptions ++= Seq(
      "-Yfuture-lazy-vals",
      "-release:21",
      "-deprecation",
      "-Wvalue-discard",
      "-Wunused:all",
      "-Werror"
    ),
    Compile / mainClass := Some("com.sslproxy.coordinator.Main"),
    assembly / assemblyJarName := "octopus.jar",
    assembly / mainClass := Some("com.sslproxy.coordinator.Main"),
    Test / javaOptions += "-Dapi.version=1.44",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    },
    Compile / run / fork := true,
    Test / fork := true
  )
