package akka.projections

import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val akka = "2.5.17"

    val scalaTest = "3.0.5"
    val scalaJava8Compat = "0.9.0"
  }

  object Compile {
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % "1.0.1"
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % "test" // ApacheV2
  }

  private val deps = libraryDependencies

  val core = deps ++= Seq(
    Compile.akkaStream
  )

  val alpakkaKafka = deps ++= Seq(
    Compile.akkaStream,
    Compile.alpakkaKafka
  )
}
