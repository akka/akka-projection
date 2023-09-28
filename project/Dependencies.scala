package akka.projections

import sbt.Keys._
import sbt._

object Dependencies {

  val Scala213 = "2.13.11"
  val Scala212 = "2.12.18"
  val Scala3 = "3.3.1"

  val Scala2Versions = Seq(Scala213, Scala212)
  val ScalaVersions = Dependencies.Scala2Versions :+ Dependencies.Scala3

  val AkkaVersionInDocs = "2.9"
  val AlpakkaVersionInDocs = "5.0"
  val AlpakkaKafkaVersionInDocs = "4.0"
  val AkkaGrpcVersionInDocs = "2.3"
  val AkkaPersistenceR2dbcVersionInDocs = Versions.akkaPersistenceR2dbc
  val AkkaProjectionVersionInDocs = "1.5"

  object Versions {
    val akka = sys.props.getOrElse("build.akka.version", "2.9.0-M1")
    val akkaPersistenceCassandra = "1.1.0"
    val akkaPersistenceJdbc = "5.2.0"
    val akkaPersistenceR2dbc = "1.2.0-M5"
    val alpakka = "6.0.1"
    val alpakkaKafka = sys.props.getOrElse("build.alpakka.kafka.version", "4.0.2")
    val slick = "3.4.1"
    val scalaTest = "3.2.16"
    val testContainers = "1.19.0"
    val junit = "4.13.2"
    val jacksonDatabind = "2.13.5" // this should match the version of jackson used by akka-serialization-jackson
  }

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % Versions.akka
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.akka
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence" % Versions.akka
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % Versions.akka
    val akkaStreamTyped = "com.typesafe.akka" %% "akka-stream-typed" % Versions.akka
    val akkaProtobufV3 = "com.typesafe.akka" %% "akka-protobuf-v3" % Versions.akka
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % Versions.akka
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka

    // TestKit in compile scope for ProjectionTestKit
    val akkaTypedTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % Versions.akka
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % Versions.akka

    val akkaPersistenceR2dbc =
      "com.lightbend.akka" %% "akka-persistence-r2dbc" % Versions.akkaPersistenceR2dbc
    val akkaPersistenceR2dbcState =
      "com.lightbend.akka" %% "akka-persistence-r2dbc" % Versions.akkaPersistenceR2dbc

    val h2 = "com.h2database" % "h2" % "2.1.210" % Provided // EPL 1.0
    val r2dbcH2 = "io.r2dbc" % "r2dbc-h2" % "1.0.0.RELEASE" % Provided // ApacheV2

    val slick = "com.typesafe.slick" %% "slick" % Versions.slick

    val alpakkaCassandra = "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % Versions.alpakka

    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka

    // must be provided on classpath when using Apache Kafka 2.6.0+
    val jackson = "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jacksonDatabind

    // FIXME remove when updating to AKka HTTP version without dependency to ssl-config-core
    val sslConfig = "com.typesafe" %% "ssl-config-core" % "0.6.1"

  }

  object Test {
    private val allTestConfig = "test,it"

    val akkaTypedTestkit = Compile.akkaTypedTestkit % allTestConfig
    val akkaStreamTestkit = Compile.akkaStreamTestkit % allTestConfig
    val akkaShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka % allTestConfig
    val akkaSerializationJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % Versions.akka % allTestConfig
    val persistenceTestkit = "com.typesafe.akka" %% "akka-persistence-testkit" % Versions.akka % allTestConfig
    val akkaDiscovery = "com.typesafe.akka" %% "akka-discovery" % Versions.akka % allTestConfig
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka % allTestConfig

    val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTest % allTestConfig
    val scalatestJUnit = "org.scalatestplus" %% "junit-4-13" % (Versions.scalaTest + ".0") % allTestConfig
    val junit = "junit" % "junit" % Versions.junit % allTestConfig

    val h2Driver = "com.h2database" % "h2" % "2.2.224" % allTestConfig
    val postgresDriver = "org.postgresql" % "postgresql" % "42.6.0" % allTestConfig
    val mysqlDriver = "com.mysql" % "mysql-connector-j" % "8.1.0" % allTestConfig
    val msSQLServerDriver = "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8" % allTestConfig
    val oracleDriver = "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0" % allTestConfig

    val logback = "ch.qos.logback" % "logback-classic" % "1.3.6" % allTestConfig

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
    val hibernate = "org.hibernate" % "hibernate-core" % "5.6.15.Final"

    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % Versions.akka
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.akka
    val akkaPersistenceCassandra =
      "com.typesafe.akka" %% "akka-persistence-cassandra" % Versions.akkaPersistenceCassandra
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
        Test.logback)

  val kafkaTests =
    deps ++= Seq(
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
        // Only needed for Replicated Event Sourcing over gRPC,
        // and ConsumerFilter with Cluster on consumer side
        Compile.akkaClusterShardingTyped % "provided",
        // FIXME remove when updating to AKka HTTP version without dependency to ssl-config-core
        Compile.sslConfig)

  val grpcTest = deps ++= Seq(
        Test.postgresDriver,
        Test.h2Driver,
        Compile.r2dbcH2,
        Compile.akkaPersistenceTyped,
        Compile.akkaStreamTyped,
        Compile.akkaPersistenceQuery,
        Test.akkaShardingTyped,
        Test.akkaStreamTestkit,
        Test.akkaSerializationJackson,
        Test.akkaTypedTestkit,
        Test.logback,
        Test.scalatest,
        Test.akkaDiscovery,
        Test.postgresContainer)

  val r2dbc = deps ++= Seq(
        Compile.akkaPersistenceQuery,
        Compile.akkaPersistenceR2dbc,
        Compile.h2, // provided
        Compile.r2dbcH2, // provided
        Compile.akkaPersistenceTyped,
        Compile.akkaStreamTyped,
        Test.akkaStreamTestkit,
        Test.akkaTypedTestkit,
        Test.akkaClusterShardingTyped,
        Test.akkaSerializationJackson,
        Test.akkaDiscovery,
        Test.logback,
        Test.scalatest)

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
