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


// provides Sources backed by Alpakka 
// and Runners that commits on Topic (at-least-once, at-most-once)
lazy val akkaProjectionAlpakkaKafka = Project(
  id = "akka-projection-alpakka-kafka",
  base = file("akka-projection-alpakka-kafka")
).settings(Dependencies.alpakkaKafka)
  .dependsOn(akkaProjectionCore, akkaProjectionTestkit)


lazy val akkaProjectionSamples = Project(
  id = "akka-projection-samples",
  base = file("akka-projection-samples")
).settings(Dependencies.sample)
  .dependsOn(akkaProjectionCore, akkaProjectionAlpakkaKafka, akkaProjectionTestkit)

lazy val akkaProjectionTestkit = Project(
  id = "akka-projection-testkit",
  base = file("akka-projection-testkit")
).settings(libraryDependencies ++= Seq(Dependencies.Test.scalaTest, Dependencies.Compile.logback))
  .dependsOn(akkaProjectionCore)


lazy val root = Project(
    id = "akka-projection",
    base = file(".")
  ).aggregate(akkaProjectionCore, akkaProjectionAlpakkaKafka, akkaProjectionTestkit)