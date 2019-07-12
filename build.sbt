import Dependencies._

ThisBuild / scalaVersion     := "2.12.0"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

val pulsarVersion = "2.3.2"
val flinkVersion = "1.8.0"
val avro4sVersion = "2.0.4"

lazy val root = (project in file("."))
  .settings(
    name := "Flink Pulsar Source",
    libraryDependencies ++=
      Seq(scalaTest % Test)
        ++ Dependencies.pulsar(pulsarVersion)
        ++ Dependencies.flink(flinkVersion)
        ++ Dependencies.avro(avro4sVersion)
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
