package akka.projections

import sbt.Keys._
import sbt._

object Dependencies {

  val Scala213 = "2.13.15"
  val Scala3 = "3.3.4"

  val Scala2Versions = Seq(Scala213)
  val ScalaVersions = Dependencies.Scala2Versions :+ Dependencies.Scala3

  object Versions {
    val Akka = sys.props.getOrElse("build.akka.version", "2.10.0-M1")
    val AkkaVersionInDocs = VersionNumber(Akka).numbers match { case Seq(major, minor, _*) => s"$major.$minor" }

    val Alpakka = "9.0.0-M1"
    val AlpakkaVersionInDocs = VersionNumber(Alpakka).numbers match { case Seq(major, minor, _*) => s"$major.$minor" }
    val AlpakkaKafkaVersionInDocs = "7.0.0-M1"
    val AkkaGrpcVersionInDocs = "2.5.0-M1"

    val AkkaProjectionVersionInDocs = "1.6.0"

    val AkkaPersistenceCassandra = "1.2.1"
    val AkkaPersistenceJdbc = "5.5.0-M1"

    val AkkaPersistenceR2dbc = "1.3.0-M1"
    val AkkaPersistenceR2dbcVersionInDocs = VersionNumber(AkkaPersistenceR2dbc).numbers match {
      case Seq(major, minor, _*) => s"$major.$minor"
    }

    val alpakkaKafka = sys.props.getOrElse("build.alpakka.kafka.version", "7.0.0-M1")
    val slick = "3.5.2"
    val scalaTest = "3.2.18"
    val testContainers = "1.19.3"
    val junit = "4.13.2"
    val jacksonDatabind = "2.17.2" // this should match the version of jackson used by akka-serialization-jackson
  }

  object Compile {
    val akkaActorTyped = "com.typesafe.akka" %% "akka-actor-typed" % Versions.Akka
    val akkaStream = "com.typesafe.akka" %% "akka-stream" % Versions.Akka
    val akkaPersistence = "com.typesafe.akka" %% "akka-persistence" % Versions.Akka
    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % Versions.Akka
    val akkaStreamTyped = "com.typesafe.akka" %% "akka-stream-typed" % Versions.Akka
    val akkaProtobufV3 = "com.typesafe.akka" %% "akka-protobuf-v3" % Versions.Akka
    val akkaPersistenceQuery = "com.typesafe.akka" %% "akka-persistence-query" % Versions.Akka
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.Akka

    // TestKit in compile scope for ProjectionTestKit
    val akkaTypedTestkit = "com.typesafe.akka" %% "akka-actor-testkit-typed" % Versions.Akka
    val akkaStreamTestkit = "com.typesafe.akka" %% "akka-stream-testkit" % Versions.Akka

    val akkaPersistenceR2dbc =
      "com.lightbend.akka" %% "akka-persistence-r2dbc" % Versions.AkkaPersistenceR2dbc
    val akkaPersistenceR2dbcState =
      "com.lightbend.akka" %% "akka-persistence-r2dbc" % Versions.AkkaPersistenceR2dbc

    val h2 = "com.h2database" % "h2" % "2.2.224" % Provided // EPL 1.0
    val r2dbcH2 = "io.r2dbc" % "r2dbc-h2" % "1.0.0.RELEASE" % Provided // ApacheV2

    val r2dbcSqlServer = "io.r2dbc" % "r2dbc-mssql" % "1.0.2.RELEASE" % Provided // ApacheV2

    val sl4j = "org.slf4j" % "slf4j-api" % "2.0.16"
    val slick = "com.typesafe.slick" %% "slick" % Versions.slick

    val alpakkaCassandra = "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % Versions.Alpakka

    val alpakkaKafka = "com.typesafe.akka" %% "akka-stream-kafka" % Versions.alpakkaKafka

    // must be provided on classpath when using Apache Kafka 2.6.0+
    val jackson = "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jacksonDatabind

  }

  object Test {
    val akkaTypedTestkit = Compile.akkaTypedTestkit % sbt.Test
    val akkaStreamTestkit = Compile.akkaStreamTestkit % sbt.Test
    val akkaShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.Akka % sbt.Test
    val akkaSerializationJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % Versions.Akka % sbt.Test
    val persistenceTestkit = "com.typesafe.akka" %% "akka-persistence-testkit" % Versions.Akka % sbt.Test
    val akkaDiscovery = "com.typesafe.akka" %% "akka-discovery" % Versions.Akka % sbt.Test
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.Akka % sbt.Test

    val scalatest = "org.scalatest" %% "scalatest" % Versions.scalaTest % sbt.Test
    val scalatestJUnit = "org.scalatestplus" %% "junit-4-13" % (Versions.scalaTest + ".0") % sbt.Test
    val junit = "junit" % "junit" % Versions.junit % sbt.Test

    val h2Driver = "com.h2database" % "h2" % "2.2.224" % sbt.Test
    val postgresDriver = "org.postgresql" % "postgresql" % "42.7.1" % sbt.Test
    val mysqlDriver = "com.mysql" % "mysql-connector-j" % "8.2.0" % sbt.Test
    val msSQLServerDriver = "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8" % sbt.Test
    val oracleDriver = "com.oracle.ojdbc" % "ojdbc8" % "19.3.0.0" % sbt.Test

    val logback = "ch.qos.logback" % "logback-classic" % "1.5.7" % sbt.Test

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

    val alpakkaKafkaTestkit =
      "com.typesafe.akka" %% "akka-stream-kafka-testkit" % Versions.alpakkaKafka % sbt.Test

  }

  object Examples {
    val hibernate = "org.hibernate" % "hibernate-core" % "6.4.1.Final"

    val akkaPersistenceTyped = "com.typesafe.akka" %% "akka-persistence-typed" % Versions.Akka
    val akkaClusterShardingTyped = "com.typesafe.akka" %% "akka-cluster-sharding-typed" % Versions.Akka
    val akkaPersistenceCassandra =
      "com.typesafe.akka" %% "akka-persistence-cassandra" % Versions.AkkaPersistenceCassandra
    val akkaPersistenceJdbc = "com.lightbend.akka" %% "akka-persistence-jdbc" % Versions.AkkaPersistenceJdbc
    val akkaSerializationJackson = "com.typesafe.akka" %% "akka-serialization-jackson" % Versions.Akka
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
    deps ++= Seq(Compile.akkaPersistenceQuery, Test.akkaTypedTestkit, Test.logback)

  val jdbcIntegration =
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
        Compile.sl4j,
        Compile.akkaPersistenceQuery,
        Test.akkaTypedTestkit,
        Test.h2Driver,
        Test.logback)

  val slickIntegration =
    deps ++= Seq(
        Compile.slick,
        Compile.sl4j,
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
    deps ++= Seq(Compile.alpakkaCassandra, Compile.akkaPersistenceQuery)

  val cassandraIntegration =
    deps ++= Seq(Test.scalatest, Test.akkaTypedTestkit, Test.logback, Test.cassandraContainer, Test.scalatestJUnit)

  val kafka =
    deps ++= Seq(
        Compile.alpakkaKafka,
        Compile.jackson,
        Test.scalatest,
        Test.akkaTypedTestkit,
        Test.akkaStreamTestkit,
        Test.logback)

  val kafkaIntegration =
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
        Compile.akkaClusterShardingTyped % "provided")

  val grpcTest = deps ++= Seq(
        Compile.akkaPersistenceTyped,
        Compile.akkaStreamTyped,
        Compile.akkaPersistenceQuery,
        Test.akkaShardingTyped,
        Test.akkaStreamTestkit,
        Test.akkaSerializationJackson,
        Test.akkaTypedTestkit,
        Test.logback,
        Test.scalatest,
        Test.akkaDiscovery)

  val grpcIntegration = deps ++= Seq(
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
        Test.postgresDriver,
        Test.h2Driver,
        Compile.r2dbcH2,
        Test.postgresContainer)

  val r2dbc = deps ++= Seq(
        Compile.akkaPersistenceQuery,
        Compile.akkaPersistenceR2dbc,
        Compile.h2, // provided
        Compile.r2dbcH2, // provided
        Compile.r2dbcSqlServer, // provided
        Compile.akkaPersistenceTyped,
        Compile.akkaStreamTyped,
        Test.akkaStreamTestkit,
        Test.akkaTypedTestkit,
        Test.akkaClusterShardingTyped,
        Test.akkaSerializationJackson,
        Test.akkaDiscovery,
        Test.logback,
        Test.scalatest)

  val r2dbcIntegration = deps ++= Seq(
        Compile.akkaPersistenceQuery,
        Compile.akkaPersistenceR2dbc,
        Compile.h2, // provided
        Compile.r2dbcH2, // provided
        Compile.r2dbcSqlServer, // provided
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
