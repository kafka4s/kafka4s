import sbt._

ThisBuild / scalaVersion           := "2.12.10"
ThisBuild / version                := "0.1.0"
ThisBuild / organization           := "io.kafka4s"
ThisBuild / organizationName       := "Kafka4s"
ThisBuild / turbo                  := true

Global / concurrentRestrictions := Seq(Tags.limitAll(1))

val Dependencies = new {
  val kafkaClients     = "org.apache.kafka" % "kafka-clients"       % "2.3.0"
  val catsCore         = "org.typelevel"    %% "cats-core"          % "1.6.0"
  val catsEffect       = "org.typelevel"    %% "cats-effect"        % "1.4.0"
  val catsRetry        = "com.github.cb372" %% "cats-retry"         % "1.1.0"
  val config           = "com.typesafe"     % "config"              % "1.4.0"
  val slf4j            = "org.slf4j"        % "slf4j-api"           % "1.7.25"
  val logback          = "ch.qos.logback"   % "logback-classic"     % "1.2.3"
  val scalaTest        = "org.scalatest"    %% "scalatest"          % "3.1.1"
  val scalaMock        = "org.scalamock"    %% "scalamock"          % "4.4.0"
  val scalaReflect     = "org.scala-lang"    % "scala-reflect"
  val betterMonadicFor = "com.olegpy"       %% "better-monadic-for" % "0.3.1"
  val kindProjector    = "org.typelevel"    %% "kind-projector"     % "0.10.3"
}

lazy val kafka4s = project.in(file("."))
  .settings(
    // Root project
    name := "kafka4s",
    skip in publish := true,
    description := "A minimal Scala-idiomatic library for Kafka",
  )
  .aggregate(core, effect)

lazy val docs = project.in(file("docs"))
  .dependsOn(kafka4s)
  .enablePlugins(MicrositesPlugin)
  .settings(
    micrositeName := "Kafka4s",
    micrositeDescription := "",
    micrositeUrl := ""
  )

lazy val core = project.in(file("core"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.kafkaClients,
      Dependencies.catsCore,
      Dependencies.catsRetry,
    )
  )

lazy val effect = project.in(file("effect"))
  .dependsOn(core)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      Dependencies.catsEffect,
      Dependencies.config,
      Dependencies.slf4j,
      Dependencies.logback % IntegrationTest,
      Dependencies.scalaTest % IntegrationTest,
    )
  )

lazy val commonSettings = Seq(
  fork in Test := true,
  fork in IntegrationTest := true,
  parallelExecution in Test := false,
  parallelExecution in IntegrationTest := false,
  libraryDependencies ++= Seq(
    Dependencies.scalaReflect % scalaVersion.value,
    Dependencies.scalaMock % Test,
    Dependencies.scalaTest % Test,
    compilerPlugin(Dependencies.betterMonadicFor),
    compilerPlugin(Dependencies.kindProjector),
  ),
  scalacOptions ++= Seq(
    "-deprecation",
    "-encoding",
    "utf-8",
    "-explaintypes",
    "-feature",
    "-language:existentials",
    "-language:experimental.macros",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-unchecked",
    "-Xcheckinit",
//    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint:adapted-args",
    "-Xlint:by-name-right-associative",
    "-Xlint:constant",
    "-Xlint:delayedinit-select",
    "-Xlint:doc-detached",
    "-Xlint:inaccessible",
    "-Xlint:infer-any",
    "-Xlint:missing-interpolator",
    "-Xlint:nullary-override",
    "-Xlint:nullary-unit",
    "-Xlint:option-implicit",
    "-Xlint:package-object-classes",
    "-Xlint:poly-implicit-overload",
    "-Xlint:private-shadow",
    "-Xlint:stars-align",
    "-Xlint:type-parameter-shadow",
    "-Xlint:unsound-match",
    "-Yno-adapted-args",
    "-Ypartial-unification",
    "-Ywarn-dead-code",
    "-Ywarn-extra-implicit",
    "-Ywarn-inaccessible",
    "-Ywarn-infer-any",
    "-Ywarn-nullary-override",
    "-Ywarn-nullary-unit",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused:implicits",
    "-Ywarn-unused:imports",
    "-Ywarn-unused:locals",
    "-Ywarn-unused:params",
    "-Ywarn-unused:patvars",
    "-Ywarn-unused:privates",
    "-Ywarn-value-discard",
  ),
  javacOptions ++= Seq(
    "-source", "1.9",
    "-target", "1.9"
  )
)
