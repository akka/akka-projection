import akka.projections.Dependencies

scalaVersion := "2.12.10"

val commonSettings = Seq(
  organization := "com.lightbend.akka",
  scalacOptions ++= List("-unchecked", "-deprecation", "-language:_", "-encoding", "UTF-8"),
  javacOptions ++= List("-Xlint:unchecked", "-Xlint:deprecation"))

lazy val akkaProjectionCore =
  Project(id = "akka-projection-core", base = file("akka-projection-core")).settings(Dependencies.core)

lazy val akkaProjectionPoc = Project(id = "akka-projection-poc", base = file("akka-projection-poc"))
  .settings(Dependencies.core)
  .dependsOn(akkaProjectionCore)

lazy val akkaProjectionPoc2 = Project(id = "akka-projection-poc2", base = file("akka-projection-poc2"))
  .settings(Dependencies.poc2)
  .dependsOn(akkaProjectionCore)

lazy val root = Project(id = "akka-projection", base = file(".")).aggregate(akkaProjectionCore, akkaProjectionPoc)
