import sbt._
import Keys._

object ScalaFlowBuild extends Build {

  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    // project settings
    version := "0.1.0",
    organization := "me.juhanlol",
    scalaVersion := "2.10.4",
    // dependencies
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "0.3.141216"
    )
  )

  lazy val project = Project("scala-flow", file("."),
    settings = buildSettings)
}
