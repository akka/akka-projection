package akka.projections

import sbt.Keys._
import sbt._

object Dependencies {

  val Scala213 = "2.13.2"
  val ScalaVersions = Seq(Scala213)

  val AkkaVersionInDocs = "2.6"
  val AlpakkaVersionInDocs = "2.0"
  val AlpakkaKafkaVersionInDocs = "2.0"

  object Versions {
    val akka = "2.6.5"
    val alpakka = "2.0.0-RC2"
    val alpakkaKafka = "2.0.2+21-0427b570"
    val slick = "3.3.2"
    val scalaTest = "3.1.1"
    val testContainersScala = "0.36.1"
    val junit = "4.12"
  }

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % Versions.akka
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
    val akkaStreamTestkit = Compile.akkaStreamTestkit % sbt.Test

    val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTest % sbt.Test
    val scalatestJUnit = "org.scalatestplus" %% "junit-4-12" % (Versions.scalaTest + ".0") % sbt.Test
    val junit = "junit" % "junit" % Versions.junit % sbt.Test
    val h2Driver = "com.h2database" % "h2" % "1.4.200" % sbt.Test
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" % sbt.Test
    val testContainers = "com.dimafeng" %% "testcontainers-scala-scalatest" % Versions.testContainersScala % sbt.Test
    val cassandraContainer =
      "com.dimafeng" %% "testcontainers-scala-cassandra" % Versions.testContainersScala % sbt.Test

    val alpakkaKafkaTestkit = "com.typesafe.akka" %% "akka-stream-kafka-testkit" % Versions.alpakkaKafka % sbt.Test
  }

  object Examples {
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % Versions.akka
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka
    val akkaPersistenceCassandra = "com.typesafe.akka" %% "akka-persistence-cassandra" % "1.0.0"
    val akkaPersistenceJdbc = "com.lightbend.akka" %% "akka-persistence-jdbc" % "4.0.0-RC2"
  }

  private val deps = libraryDependencies

  val core =
    deps ++= Seq(
        Compile.akkaStream,
        Compile.akkaActorTyped,
        // akka-persistence-query is only needed for OffsetSerialization, but that is always used together
        // with more specific modules, such as akka-projection-cassandra, which defines the required
        // dependency on akka-persistence-query
        Compile.akkaPersistenceQuery % "optional;provided",
        Test.akkaTypedTestkit,
        Test.logback,
        Test.scalatest)

  val testKit =
    deps ++= Seq(Compile.akkaTypedTestkit, Compile.akkaStreamTestkit, Test.scalatest, Test.scalatestJUnit, Test.junit)

  val eventsourced =
    deps ++= Seq(Compile.akkaPersistenceQuery)

  val slick =
    deps ++= Seq(Compile.slick, Compile.akkaPersistenceQuery, Test.akkaTypedTestkit, Test.h2Driver, Test.logback)

  val cassandra =
    deps ++= Seq(
        Compile.alpakkaCassandra,
        Compile.akkaPersistenceQuery,
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

  val examples =
    deps ++= Seq(
        Examples.akkaPersistenceTyped,
        Examples.akkaClusterShardingTyped,
        Examples.akkaPersistenceCassandra,
        Examples.akkaPersistenceJdbc,
        Test.akkaTypedTestkit,
        Test.logback)
}
