import akka.projections.Dependencies

scalaVersion := "2.12.7"

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

lazy val akkaProjectionCore = Project(
    id = "akka-projection-core",
    base = file("akka-projection-core")
  ).settings(Dependencies.core)



lazy val root = Project(
    id = "akka-projection",
    base = file(".")
  ).aggregate(akkaProjectionCore)