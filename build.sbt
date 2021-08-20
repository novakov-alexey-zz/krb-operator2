import Dependencies._

Global / onChangedBuildSource := ReloadOnSourceChanges
ThisBuild / scalaVersion     := "2.13.6"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "org.novakov-alexey"
ThisBuild / organizationName := "krb-operator2"

lazy val root = (project in file("."))
  .settings(
    name := "krb-operator2",
    addCompilerPlugin(betterMonadicFor),
    libraryDependencies ++= Seq(
      kubernetesClient,
      circeExtra,
      circeCore,
      pureConfig,
      logbackClassic,
      scalaTest % Test
    )
  )
