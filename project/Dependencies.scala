import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.10"

  lazy val kubernetesClient =
    "com.goyeau" %% "kubernetes-client" % "0.7.0-2-6fbf23b-SNAPSHOT"

  lazy val circeVersion = "0.14.1"
  lazy val circeExtra = "io.circe" %% "circe-generic-extras" % circeVersion
  lazy val circeCore = "io.circe" %% "circe-core" % circeVersion

  lazy val logbackClassic =
    "ch.qos.logback" % "logback-classic" % "1.3.0-alpha10"

  val pureConfigVersion = "0.16.0"
  lazy val pureConfig =
    "com.github.pureconfig" %% "pureconfig" % pureConfigVersion

  val betterMonadicVersion = "0.3.1"
  lazy val betterMonadicFor =
    "com.olegpy" %% "better-monadic-for" % betterMonadicVersion
}
