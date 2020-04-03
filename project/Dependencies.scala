package akka.projections

import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val akka = "2.6.4"
    val scalaTest = "3.1.1"
  }

  object Compile {
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % sbt.Test
  }

  private val deps = libraryDependencies

  val core = deps ++= Seq(Compile.akkaStream)

}
