package akka.projections

import sbt._
import sbt.Keys._

object Dependencies {

  object Versions {
    val akka = "2.6.4"

    val scalaJava8Compat = "0.9.0"
    val alpakkaKafka = "2.0.2"

    val scalaTest = "3.0.5"
    val testContainers = "1.11.2"
  }

  object Compile {
    val persistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % Versions.akka

    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % Versions.akka

    val persistenceJDBC = "com.github.dnvriend" %% "akka-persistence-jdbc" % "3.5.3"
    val persistenceCassandra = "com.typesafe.akka" %% "akka-persistence-cassandra" % "0.103"

    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  }

  object Test {
    val aplakkaKafkaTestkit = "com.typesafe.akka" %% "akka-stream-kafka-testkit" % Versions.alpakkaKafka % sbt.Test

    val scalaTest = "org.scalatest" %% "scalatest" % Versions.scalaTest %  sbt.Test
    val testContainers = "org.testcontainers" % "kafka" % Versions.testContainers % sbt.Test
  }

  private val deps = libraryDependencies


  val core = deps ++= Seq(
    Compile.akkaStream
  )

  val sample = deps ++= Seq(
    Compile.persistenceQuery,
    Compile.persistenceJDBC,
    Compile.persistenceCassandra
  )

  val alpakkaKafka = deps ++= Seq(
    Compile.akkaStream,
    Compile.alpakkaKafka,
    // test
    Compile.akkaSlf4j % sbt.Test,
    Test.aplakkaKafkaTestkit,
    Test.testContainers,
    Test.scalaTest
  )
}
