package akka.projections

import akka.projections.Dependencies.Libraries
import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val akka = "2.5.23"

    val scalaTest = "3.0.5"
    val scalaJava8Compat = "0.9.0"
    val alpakkaKafka = "1.0.3"
    val testContainers = "1.11.2"

  }

  object Libraries {
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka
    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka
    val aplakkaKafkaTestkit = "com.typesafe.akka" %% "akka-stream-kafka-testkit" % Versions.alpakkaKafka % Test


    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
    val logbackTest = logback % Test

    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest % Test
    val testContainers = "org.testcontainers" % "kafka" % Versions.testContainers % Test
  }


  val core = libraryDependencies ++= Seq(
    Libraries.akkaStream
  )

  val alpakkaKafka = libraryDependencies ++= Seq(
    Libraries.akkaStream,
    Libraries.alpakkaKafka,
    // test
    Libraries.akkaSlf4j % Test,
    Libraries.aplakkaKafkaTestkit,
    Libraries.testContainers,
    Libraries.scalaTest
  )
}
