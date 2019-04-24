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


// provides Runner and OffsetStore for transactional projections
// lazy val akkaProjectionJdbc = Project(
//   id = "akka-projection-jdbc",
//   base = file("akka-projection-jdbc")
// ).settings(Dependencies.jdbc)

// provides Sources backed by Alpakka 
// and Runners that commits on Topic (at-least-once, at-most-once)
lazy val akkaProjectionAlpakkaKafka = Project(
  id = "akka-projection-alpakka-kafka",
  base = file("akka-projection-alpakka-kafka")
).settings(Dependencies.alpakkaKafka)
  .settings(libraryDependencies ++= Seq(Dependencies.Test.scalaTest))
  .dependsOn(akkaProjectionCore, akkaProjectionMocks)


lazy val akkaProjectionMocks = Project(
  id = "akka-projection-mocks",
  base = file("akka-projection-mocks")
).settings(libraryDependencies ++= Seq(Dependencies.Test.scalaTest))
  .dependsOn(akkaProjectionCore)

// // provides Sources backed Akka Persistence Query
// lazy val akkaProjectionAkkaPersistence = Project(
//   id = "akka-projection-akka-persistence",
//   base = file("akka-projection-akka-persistence")
//   ).settings(Dependencies.akkaPersistence)

lazy val root = Project(
    id = "akka-projection",
    base = file(".")
  ).aggregate(akkaProjectionCore, akkaProjectionAlpakkaKafka, akkaProjectionMocks)