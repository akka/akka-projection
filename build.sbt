import akka.projections.Dependencies

scalaVersion := "2.12.6"

val commonSettings = Seq(
  organization := "com.lightbend.akka",

  scalacOptions ++= List(
    "-unchecked",
    "-deprecation",
    "-language:_",
    "-encoding", "UTF-8"
  ),
  javacOptions ++= List(
    "-Xlint:unchecked",
    "-Xlint:deprecation"
  )
)

lazy val akkaProjectionsCore = Project(
    id = "akka-projections-core",
    base = file("akka-projections-core")
  ).settings(Dependencies.core)



lazy val root = Project(
    id = "akka-projections",
    base = file(".")
  ).aggregate(akkaProjectionsCore)