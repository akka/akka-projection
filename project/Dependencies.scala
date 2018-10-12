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
    val akkaStream        = "com.typesafe.akka" %% "akka-stream" % Versions.akka
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % "test" // ApacheV2
    val scalaJava8Compat = "org.scala-lang.modules" %% "scala-java8-compat" % Versions.scalaJava8Compat % "test" // BSD 3-clause
    val junit = "junit" % "junit" % "4.12" % "test" // Common Public License 1.0
  }

  private val deps = libraryDependencies

  val core = deps ++= Seq(
    Compile.akkaStream
  )

  val kafka = deps ++= Seq(
    Compile.akkaStream
  )
}
