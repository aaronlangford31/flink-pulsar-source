import sbt._

object Dependencies {
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.8"

  def pulsar(pulsarVersion: String): Seq[ModuleID] = Seq(
    ("org.apache.pulsar" % "pulsar-client-admin" % pulsarVersion).excludeAll(
      ExclusionRule("javax.xml.bind", "activation")
    ),
    "org.apache.pulsar" % "pulsar-client" % pulsarVersion
  )

  def flink(flinkVersion: String): Seq[ModuleID] = Seq(
    "org.apache.flink" % "flink-core" % flinkVersion,
    "org.apache.flink" %% "flink-streaming-java" % flinkVersion
  )

  def avro(avro4sVersion: String): Seq[ModuleID] = Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % avro4sVersion
  )
}
