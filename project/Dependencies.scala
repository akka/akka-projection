package akka.projections

import sbt.Keys._
import sbt._

object Dependencies {

  val Scala213 = "2.13.1"
  val ScalaVersions = Seq(Scala213)

  val AkkaVersionInDocs = "2.6"
  val AlpakkaVersionInDocs = "2.0"
  val AlpakkaKafkaVersionInDocs = "2.0"

  object Versions {
    val akka = "2.6.4"
    val alpakka = "2.0.0-RC2"
    val alpakkaKafka = "2.0.2+21-0427b570"
    val slick = "3.3.2"
    val scalaTest = "3.1.1"
    val testContainersScala = "0.36.1"
    val junit = "4.12"
  }

  object Compile {
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % Versions.akka

    // TestKit in compile scope for ProjectionTestKit
    val akkaTypedTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % Versions.akka
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % Versions.akka

    val slick = "com.typesafe.slick" %% "slick" % Versions.slick

    val alpakkaCassandra = "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % Versions.alpakka

    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka
  }

  object Test {
    val akkaTypedTestkit = Compile.akkaTypedTestkit % sbt.Test
    val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTest % sbt.Test
    val scalatestJUnit = "org.scalatestplus" %% "junit-4-12" % (Versions.scalaTest + ".0") % sbt.Test
    val junit = "junit" % "junit" % Versions.junit % sbt.Test
    val h2Driver = "com.h2database" % "h2" % "1.4.200" % sbt.Test
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" % sbt.Test
    val testContainers = "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.testContainersScala % sbt.Test
    val cassandraContainer =
      "com.dimafeng" %% "testcontainers-scala-cassandra" % Versions.testContainersScala % sbt.Test
    val akkaStreamTestkit = Compile.akkaStreamTestkit % sbt.Test
    val alpakkaKafkaTestkit = "com.typesafe.akka" %% "akka-stream-kafka-testkit" % Versions.alpakkaKafka % sbt.Test
  }

  private val deps = libraryDependencies

  val core = deps ++= Seq(Compile.akkaStream, Compile.akkaPersistenceQuery, Test.scalatest)

  val testKit =
    deps ++= Seq(Compile.akkaTypedTestkit, Compile.akkaStreamTestkit, Test.scalatest, Test.scalatestJUnit, Test.junit)

  val eventSourced =
    deps ++= Seq(Compile.akkaPersistenceQuery)

  val slick =
    deps ++= Seq(Compile.slick, Test.akkaTypedTestkit, Test.h2Driver, Test.logback)

  val cassandra =
    deps ++= Seq(
        Compile.alpakkaCassandra,
        Test.akkaTypedTestkit,
        Test.logback,
        Test.testContainers,
        Test.cassandraContainer,
        Test.scalatestJUnit)

  val kafka =
    deps ++= Seq(
        Compile.alpakkaKafka,
        Test.scalatest,
        Test.akkaTypedTestkit,
        Test.akkaStreamTestkit,
        Test.alpakkaKafkaTestkit,
        Test.logback,
        Test.testContainers,
        Test.scalatestJUnit)
}
