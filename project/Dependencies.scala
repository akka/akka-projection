package akka.projections

import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val akka = "2.6.4"
    val scalaJava8Compat = "0.9.0"
    val alpakkaKafka = "2.0.2"
    val scalaTest = "3.0.5"
  }

  object Compile {
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  }

  object Test {
    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest %  sbt.Test
  }

  private val deps = libraryDependencies


  val core = deps ++= Seq(
    Compile.akkaStream
  )


}

