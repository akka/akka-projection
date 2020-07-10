package akka.projections

import sbt.Keys._
import sbt._

object Dependencies {

  val Scala213 = "2.13.3"
  val Scala212 = "2.12.11"
  val ScalaVersions = Seq(Scala213, Scala212)

  val AkkaVersionInDocs = "2.6"
  val AlpakkaVersionInDocs = "2.0"
  val AlpakkaKafkaVersionInDocs = "2.0"

  object Versions {
    val akka = sys.props.getOrElse("build.akka.version", "2.6.6")
    val alpakka = "2.0.0"
    val alpakkaKafka = sys.props.getOrElse("build.alpakka.kafka.version", "2.0.3")
    val slick = "3.3.2"
    val scalaTest = "3.1.1"
    val testContainersScala = "0.37.0"
    val testContainers = "1.14.3"
    val junit = "4.12"
    val h2Driver = "1.4.200"
  }

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % Versions.akka
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val akkaProtobufV3 = "com.typesafe.akka" %% "akka-protobuf-v3" % Versions.akka
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % Versions.akka

    // TestKit in compile scope for ProjectionTestKit
    val akkaTypedTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % Versions.akka
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % Versions.akka

    val slick = "com.typesafe.slick" %% "slick" % Versions.slick

    val alpakkaCassandra = "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % Versions.alpakka

    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka

    // not really used in lib code, but in example and test
    val h2Driver = "com.h2database" % "h2" % Versions.h2Driver
  }

  object Test {

    val akkaTypedTestkit = Compile.akkaTypedTestkit % sbt.Test
    val akkaStreamTestkit = Compile.akkaStreamTestkit % sbt.Test

    val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTest % sbt.Test
    val scalatestJUnit = "org.scalatestplus" %% "junit-4-12" % (Versions.scalaTest + ".0") % sbt.Test
    val junit = "junit" % "junit" % Versions.junit % sbt.Test

    val h2Driver = Compile.h2Driver % sbt.Test
    val postgresDriver = "org.postgresql" % "postgresql" % "42.2.12" % sbt.Test
    val mysqlDriver = "mysql" % "mysql-connector-java" % "8.0.20" % sbt.Test
    val msSQLServerDriver = "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8" % sbt.Test
    val oracleDriver = "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0" % sbt.Test

    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3" % sbt.Test

    val cassandraContainer =
      "org.testcontainers" % "cassandra" % Versions.testContainers % sbt.Test
    val postgresContainer =
      "org.testcontainers" % "postgresql" % Versions.testContainers % sbt.Test
    val mysqlContainer =
      "org.testcontainers" % "mysql" % Versions.testContainers % sbt.Test
    val msSQLServerContainer =
      "org.testcontainers" % "mssqlserver" % Versions.testContainers % sbt.Test

    val oracleDbContainer =
      "org.testcontainers" % "oracle-xe" % Versions.testContainers % sbt.Test

    val alpakkaKafkaTestkit = "com.typesafe.akka" %% "akka-stream-kafka-testkit" % Versions.alpakkaKafka % sbt.Test
  }

  object Examples {
    val hibernate = "org.hibernate" % "hibernate-core" % "5.4.18.Final"

    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % Versions.akka
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka
    val akkaPersistenceCassandra = "com.typesafe.akka" %% "akka-persistence-cassandra" % "1.0.0"
    val akkaPersistenceJdbc = "com.lightbend.akka" %% "akka-persistence-jdbc" % "4.0.0"
  }

  private val deps = libraryDependencies

  val core =
    deps ++= Seq(
        Compile.akkaStream,
        Compile.akkaActorTyped,
        Compile.akkaProtobufV3,
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

  val jdbc =
    deps ++= Seq(
        Compile.akkaPersistenceQuery,
        Test.akkaTypedTestkit,
        Test.h2Driver,
        Test.postgresDriver,
        Test.postgresContainer,
        Test.mysqlDriver,
        Test.mysqlContainer,
        Test.msSQLServerDriver,
        Test.msSQLServerContainer,
        Test.oracleDriver,
        Test.oracleDbContainer,
        Test.logback)

  val slick =
    deps ++= Seq(
        Compile.slick,
        Compile.akkaPersistenceQuery,
        Test.akkaTypedTestkit,
        Test.h2Driver,
        Test.postgresDriver,
        Test.postgresContainer,
        Test.mysqlDriver,
        Test.mysqlContainer,
        Test.msSQLServerDriver,
        Test.msSQLServerContainer,
        Test.oracleDriver,
        Test.oracleDbContainer,
        Test.logback)

  val cassandra =
    deps ++= Seq(
        Compile.alpakkaCassandra,
        Compile.akkaPersistenceQuery,
        Test.akkaTypedTestkit,
        Test.logback,
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
        Test.scalatestJUnit)

  val examples =
    deps ++= Seq(
        Examples.akkaPersistenceTyped,
        Examples.akkaClusterShardingTyped,
        Examples.akkaPersistenceCassandra,
        Examples.akkaPersistenceJdbc,
        Examples.hibernate,
        Test.h2Driver,
        Test.akkaTypedTestkit,
        Test.logback,
        Test.cassandraContainer)
}
