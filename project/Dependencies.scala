package akka.projections

import sbt.Keys._
import sbt._

object Dependencies {

  val Scala213 = "2.13.8"
  val Scala212 = "2.12.16"
  val ScalaVersions = Seq(Scala213, Scala212)

  val AkkaVersionInDocs = "2.7"
  val AlpakkaVersionInDocs = "5.0"
  val AlpakkaKafkaVersionInDocs = "3.1"
  val AkkaGrpcVersionInDocs = "2.2"
  val AkkaPersistenceR2dbcVersionInDocs = Versions.akkaPersistenceR2dbc

  object Versions {
    val akka = sys.props.getOrElse("build.akka.version", "2.7.0-M3")
    val akkaPersistenceJdbc = "5.2.0-M1"
    val akkaPersistenceR2dbc = "1.0.0-M2"
    val alpakka = "5.0.0-M1"
    val alpakkaKafka = sys.props.getOrElse("build.alpakka.kafka.version", "3.1.0-M1")
    val slick = "3.4.1"
    val scalaTest = "3.1.1"
    val testContainers = "1.15.3"
    val junit = "4.13.2"
    val h2Driver = "1.4.200"
    val jackson = "2.13.4.1" // this should match the version of jackson used by akka-serialization-jackson
  }

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % Versions.akka
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence" % Versions.akka
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % Versions.akka
    val akkaProtobufV3 = "com.typesafe.akka" %% "akka-protobuf-v3" % Versions.akka
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % Versions.akka

    // TestKit in compile scope for ProjectionTestKit
    val akkaTypedTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % Versions.akka
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % Versions.akka

    val slick = "com.typesafe.slick" %% "slick" % Versions.slick

    val alpakkaCassandra = "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % Versions.alpakka

    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka

    // must be provided on classpath when using Apache Kafka 2.6.0+
    val jackson = "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson

    // not really used in lib code, but in example and test
    val h2Driver = "com.h2database" % "h2" % Versions.h2Driver

    val collectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % "2.7.0"
  }

  object Test {
    private val allTestConfig = "test,it"

    val akkaTypedTestkit = Compile.akkaTypedTestkit % allTestConfig
    val akkaStreamTestkit = Compile.akkaStreamTestkit % allTestConfig
    val akkaShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka % allTestConfig
    val akkaSerializationJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % Versions.akka % allTestConfig
    val persistenceTestkit = "com.typesafe.akka" %% "akka-persistence-testkit" % Versions.akka % allTestConfig
    val akkaDiscovery = "com.typesafe.akka" %% "akka-discovery" % Versions.akka % allTestConfig

    // FIXME remove semi-circular depenendency
    val akkaProjectionR2dbc =
      "com.lightbend.akka" %% "akka-projection-r2dbc" % Versions.akkaPersistenceR2dbc % allTestConfig

    val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTest % allTestConfig
    val scalatestJUnit = "org.scalatestplus" %% "junit-4-12" % (Versions.scalaTest + ".0") % allTestConfig
    val junit = "junit" % "junit" % Versions.junit % allTestConfig

    val h2Driver = Compile.h2Driver % allTestConfig
    val postgresDriver = "org.postgresql" % "postgresql" % "42.5.0" % allTestConfig
    val mysqlDriver = "mysql" % "mysql-connector-java" % "8.0.30" % allTestConfig
    val msSQLServerDriver = "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8" % allTestConfig
    val oracleDriver = "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0" % allTestConfig

    val logback = "ch.qos.logback" % "logback-classic" % "1.2.11" % allTestConfig

    val cassandraContainer =
      "org.testcontainers" % "cassandra" % Versions.testContainers % allTestConfig
    val postgresContainer =
      "org.testcontainers" % "postgresql" % Versions.testContainers % allTestConfig
    val mysqlContainer =
      "org.testcontainers" % "mysql" % Versions.testContainers % allTestConfig
    val msSQLServerContainer =
      "org.testcontainers" % "mssqlserver" % Versions.testContainers % allTestConfig

    val oracleDbContainer =
      "org.testcontainers" % "oracle-xe" % Versions.testContainers % allTestConfig

    val alpakkaKafkaTestkit =
      "com.typesafe.akka" %% "akka-stream-kafka-testkit" % Versions.alpakkaKafka % allTestConfig
  }

  object Examples {
    val hibernate = "org.hibernate" % "hibernate-core" % "5.6.11.Final"

    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % Versions.akka
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka
    val akkaPersistenceCassandra = "com.typesafe.akka" %% "akka-persistence-cassandra" % "1.1.0-M1"
    val akkaPersistenceJdbc = "com.lightbend.akka" %% "akka-persistence-jdbc" % Versions.akkaPersistenceJdbc
    val akkaSerializationJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % Versions.akka
  }

  private val deps = libraryDependencies

  val core =
    deps ++= Seq(
        Compile.akkaStream,
        Compile.akkaActorTyped,
        Compile.akkaProtobufV3,
        // akka-persistence-query is only needed for OffsetSerialization and to provide a typed EventEnvelope that
        // references the Offset type from akka-persistence.
        Compile.akkaPersistenceQuery,
        Test.akkaTypedTestkit,
        Test.logback,
        Test.scalatest)

  val coreTest =
    deps ++= Seq(
        Test.akkaTypedTestkit,
        Test.akkaStreamTestkit,
        Test.scalatest,
        Test.scalatestJUnit,
        Test.junit,
        Test.logback)

  val testKit =
    deps ++= Seq(
        Compile.akkaTypedTestkit,
        Compile.akkaStreamTestkit,
        Compile.collectionCompat,
        Test.scalatest,
        Test.scalatestJUnit,
        Test.junit,
        Test.logback)

  val eventsourced =
    deps ++= Seq(Compile.akkaPersistenceQuery)

  val state =
    deps ++= Seq(Compile.akkaPersistenceQuery, Test.persistenceTestkit, Test.akkaStreamTestkit, Test.scalatest)

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
        Compile.jackson,
        Test.scalatest,
        Test.akkaTypedTestkit,
        Test.akkaStreamTestkit,
        Test.alpakkaKafkaTestkit,
        Test.logback,
        Test.scalatestJUnit)

  val grpc = deps ++= Seq(
        Compile.akkaActorTyped,
        Compile.akkaStream,
        Compile.akkaPersistenceTyped,
        Compile.akkaPersistenceQuery,
        Test.akkaProjectionR2dbc,
        Test.postgresDriver,
        Test.akkaShardingTyped,
        Test.akkaStreamTestkit,
        Test.akkaSerializationJackson,
        Test.akkaTypedTestkit,
        Test.logback,
        Test.scalatest,
        Test.akkaDiscovery,
        Test.postgresContainer)

  val examples =
    deps ++= Seq(
        Examples.akkaPersistenceTyped,
        Examples.akkaClusterShardingTyped,
        Examples.akkaPersistenceCassandra,
        Examples.akkaPersistenceJdbc,
        Examples.akkaSerializationJackson,
        Examples.hibernate,
        Test.h2Driver,
        Test.akkaTypedTestkit,
        Test.logback,
        Test.cassandraContainer)
}
