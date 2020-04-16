package akka.projections

import sbt._
import sbt.Keys._

object Dependencies {

  val Scala213 = "2.13.1"
  val ScalaVersions = Seq(Scala213)

  val AkkaVersionInDocs = "2.6"
  val AlpakkaVersionInDocs = "2.0"
  val AlpakkaKafkaVersionInDocs = "2.0"

  object Versions {
    val akka = "2.6.4"
    val scalaTest = "3.1.1"
  }

  object Compile {
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka

    // TestKit in compile scope for ProjectionTestKit
    val akkaTypedTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % Versions.akka
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % Versions.akka
  }

  object Test {
    val akkaTypedTestkit = Compile.akkaTypedTestkit % sbt.Test
    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % sbt.Test
  }

  private val deps = libraryDependencies

  val core = deps ++= Seq(Compile.akkaStream)

  val testKit = deps ++= Seq(Compile.akkaTypedTestkit, Compile.akkaStreamTestkit, Test.scalaTest)
}
