package akka.projections

import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val akka = "2.6.3"

    val scalaTest = "3.1.1"
  }

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % Versions.akka
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val persistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % Versions.akka
    val persistenceCassandra = "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.103"
    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % "2.0.2"
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % "test" // ApacheV2
  }

  private val deps = libraryDependencies

  val core = deps ++= Seq(Compile.akkaStream)

  val kafka = deps ++= Seq(Compile.akkaStream)

  val poc2 = deps ++= Seq(
        Compile.akkaActorTyped,
        Compile.akkaStream,
        Compile.persistenceQuery,
        Compile.persistenceCassandra,
        Compile.alpakkaKafka)
}
