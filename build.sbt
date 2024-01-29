import akka.projections.Dependencies
import akka.projections.IntegrationTests

// avoid + in snapshot versions
ThisBuild / dynverSeparator := "-"
// append -SNAPSHOT to version when isSnapshot
ThisBuild / dynverSonatypeSnapshots := true
ThisBuild / resolvers += "Akka library repository".at("https://repo.akka.io/maven")

lazy val core =
  Project(id = "akka-projection-core", base = file("akka-projection-core"))
    .settings(Dependencies.core)
    .settings(
      name := "akka-projection-core",
      Compile / packageBin / packageOptions += Package.ManifestAttributes(
          "Automatic-Module-Name" -> "akka.projection.core"))
    .settings(Protobuf.settings)
    .settings(Scala3.settings)
    .disablePlugins(CiReleasePlugin)

lazy val coreTest =
  Project(id = "akka-projection-core-test", base = file("akka-projection-core-test"))
    .disablePlugins(MimaPlugin, CiReleasePlugin)
    .settings(Dependencies.coreTest)
    .settings(publish / skip := true)
    .settings(Scala3.settings)
    .dependsOn(core)
    .dependsOn(testkit % Test)

lazy val testkit =
  Project(id = "akka-projection-testkit", base = file("akka-projection-testkit"))
    .settings(Dependencies.testKit)
    .settings(Scala3.settings)
    .dependsOn(core)
    .disablePlugins(CiReleasePlugin)

// provides offset storage backed by a JDBC table
lazy val jdbc =
  Project(id = "akka-projection-jdbc", base = file("akka-projection-jdbc"))
    .settings(Dependencies.jdbc)
    .settings(Scala3.settings)
    .dependsOn(core)
    .dependsOn(coreTest % "test->test")
    .dependsOn(testkit % Test)
    .disablePlugins(CiReleasePlugin)

lazy val jdbcIntegration =
  Project(id = "akka-projection-jdbc-integration", base = file("akka-projection-jdbc-integration"))
    .settings(IntegrationTests.settings)
    .settings(Dependencies.jdbcIntegration)
    .settings(Scala3.settings)
    .dependsOn(jdbc)
    .dependsOn(coreTest % "test->test")
    .dependsOn(testkit % Test)
    .disablePlugins(CiReleasePlugin)

// provides offset storage backed by a JDBC (Slick) table
lazy val slick =
  Project(id = "akka-projection-slick", base = file("akka-projection-slick"))
    .settings(Dependencies.slick)
    .dependsOn(jdbc, core)
    .disablePlugins(CiReleasePlugin)

lazy val slickIntegration =
  Project(id = "akka-projection-slick-integration", base = file("akka-projection-slick-integration"))
    .settings(IntegrationTests.settings)
    .settings(Dependencies.slickIntegration)
    .dependsOn(slick)
    .dependsOn(coreTest % "test->test")
    .dependsOn(testkit % Test)
    .disablePlugins(CiReleasePlugin)

// provides offset storage backed by a Cassandra table
lazy val cassandra =
  Project(id = "akka-projection-cassandra", base = file("akka-projection-cassandra"))
    .settings(Dependencies.cassandra)
    .dependsOn(core)
    .settings(Scala3.settings)
    .disablePlugins(CiReleasePlugin)

lazy val cassandraIntegration =
  Project(id = "akka-projection-cassandra-integration", base = file("akka-projection-cassandra-integration"))
    .settings(IntegrationTests.settings)
    .settings(Dependencies.cassandraIntegration)
    .settings(Scala3.settings)
    .dependsOn(coreTest % "test->test", testkit, cassandra)
    .disablePlugins(CiReleasePlugin, AkkaDisciplinePlugin)

// provides source providers for akka-persistence-query
lazy val eventsourced =
  Project(id = "akka-projection-eventsourced", base = file("akka-projection-eventsourced"))
    .settings(Dependencies.eventsourced)
    .settings(Scala3.settings)
    .dependsOn(core)
    .dependsOn(testkit % Test)
    .disablePlugins(CiReleasePlugin)

// provides offset storage backed by Kafka managed offset commits
lazy val kafka =
  Project(id = "akka-projection-kafka", base = file("akka-projection-kafka"))
    .settings(Dependencies.kafka)
    .settings(Scala3.settings)
    .dependsOn(testkit)
    .dependsOn(core)
    .disablePlugins(CiReleasePlugin)

lazy val kafkaIntegration =
  Project(id = "akka-projection-kafka-integration", base = file("akka-projection-kafka-integration"))
    .settings(IntegrationTests.settings)
    .settings(Dependencies.kafkaIntegration)
    .settings(Scala3.settings)
    .dependsOn(kafka, testkit % "test->test")
    .dependsOn(slick)
    .dependsOn(slickIntegration % "test->test")
    .disablePlugins(CiReleasePlugin)

// provides source providers for durable state changes
lazy val `durable-state` =
  Project(id = "akka-projection-durable-state", base = file("akka-projection-durable-state"))
    .settings(Dependencies.state)
    .settings(Scala3.settings)
    .dependsOn(core)
    .dependsOn(testkit % Test)
    .disablePlugins(CiReleasePlugin)

lazy val grpc =
  Project(id = "akka-projection-grpc", base = file("akka-projection-grpc"))
    .settings(Dependencies.grpc)
    .settings(akkaGrpcCodeGeneratorSettings += "server_power_apis")
    .settings(Scala3.settings)
    .dependsOn(core)
    .dependsOn(eventsourced)
    .enablePlugins(AkkaGrpcPlugin)
    .disablePlugins(CiReleasePlugin)

lazy val grpcTests =
  Project(id = "akka-projection-grpc-tests", base = file("akka-projection-grpc-tests"))
    .disablePlugins(MimaPlugin)
    .settings(Dependencies.grpcTest)
    .settings(publish / skip := true)
    .settings(akkaGrpcCodeGeneratorSettings += "server_power_apis")
    .settings(Scala3.settings)
    .dependsOn(grpc)
    .dependsOn(r2dbc) // for compile only tests
    .dependsOn(testkit % Test)
    .enablePlugins(AkkaGrpcPlugin)
    .disablePlugins(CiReleasePlugin)

lazy val grpcIntegration =
  Project(id = "akka-projection-grpc-integration", base = file("akka-projection-grpc-integration"))
    .settings(IntegrationTests.settings)
    .settings(Dependencies.grpcIntegration)
    .settings(akkaGrpcCodeGeneratorSettings += "server_power_apis")
    // FIXME test coverage with Scala 3 ? .settings(Scala3.settings)
    .dependsOn(grpc, r2dbc)
    .enablePlugins(AkkaGrpcPlugin)
    .disablePlugins(CiReleasePlugin)
    .dependsOn(testkit % Test)
    .dependsOn(r2dbcIntegration % Test) // uses test infra from r2dbc tests

// provides offset storage backed by akka-persistence-r2dbc
lazy val r2dbc =
  Project(id = "akka-projection-r2dbc", base = file("akka-projection-r2dbc"))
    .settings(Scala3.settings)
    .settings(Dependencies.r2dbc)
    .dependsOn(core, grpc, eventsourced, `durable-state`)
    .disablePlugins(CiReleasePlugin)

lazy val r2dbcIntegration =
  Project(id = "akka-projection-r2dbc-integration", base = file("akka-projection-r2dbc-integration"))
    .settings(IntegrationTests.settings)
    .settings(Dependencies.r2dbcIntegration)
    .settings(Scala3.settings)
    .dependsOn(r2dbc)
    .dependsOn(testkit % Test)
    .disablePlugins(CiReleasePlugin)

// note that this is in the integration test aggregate
// rather than root since it depends on other integration test modules
lazy val examples = project
  .disablePlugins(MimaPlugin, CiReleasePlugin)
  .settings(IntegrationTests.settings)
  .settings(Dependencies.examples)
  .dependsOn(slick % "test->test")
  .dependsOn(jdbc % "test->test")
  .dependsOn(cassandra)
  .dependsOn(cassandraIntegration % "test->test")
  .dependsOn(eventsourced)
  .dependsOn(`durable-state`)
  .dependsOn(kafka % "test->test")
  .dependsOn(testkit % Test)
  .settings(publish / skip := true, scalacOptions += "-feature", javacOptions += "-parameters")

lazy val commonParadoxProperties = Def.settings(
  Compile / paradoxProperties ++= Map(
      // Akka
      "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
      "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersionInDocs}/",
      "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/${Dependencies.AkkaVersionInDocs}/",
      "javadoc.akka.link_style" -> "direct",
      // Alpakka
      "extref.alpakka.base_url" -> s"https://doc.akka.io/docs/alpakka/${Dependencies.AlpakkaVersionInDocs}/%s",
      "scaladoc.akka.stream.alpakka.base_url" -> s"https://doc.akka.io/api/alpakka/${Dependencies.AlpakkaVersionInDocs}/",
      "javadoc.akka.stream.alpakka.base_url" -> "",
      // Alpakka Kafka
      "extref.alpakka-kafka.base_url" -> s"https://doc.akka.io/docs/alpakka-kafka/${Dependencies.AlpakkaKafkaVersionInDocs}/%s",
      "scaladoc.akka.kafka.base_url" -> s"https://doc.akka.io/api/alpakka-kafka/${Dependencies.AlpakkaKafkaVersionInDocs}/",
      "javadoc.akka.kafka.base_url" -> "",
      // Akka gRPC
      "extref.akka-grpc.base_url" -> s"https://doc.akka.io/docs/akka-grpc/${Dependencies.AkkaGrpcVersionInDocs}/%s",
      // Akka persistence R2DBC plugin
      "extref.akka-persistence-r2dbc.base_url" -> s"https://doc.akka.io/docs/akka-persistence-r2dbc/${Dependencies.AkkaPersistenceR2dbcVersionInDocs}/%s",
      // Akka Guide
      "extref.akka-guide.base_url" -> "https://developer.lightbend.com/docs/akka-guide/microservices-tutorial/",
      // Java
      "javadoc.base_url" -> "https://docs.oracle.com/javase/8/docs/api/",
      // Scala
      "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/${scalaBinaryVersion.value}.x/",
      "scaladoc.akka.projection.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
      "javadoc.akka.projection.base_url" -> "", // no Javadoc is published
      // Misc
      "extref.samples.base_url" -> "https://developer.lightbend.com/start/?group=akka&amp;project=%s",
      "extref.platform-guide.base_url" -> "https://developer.lightbend.com/docs/akka-guide/%s"))

lazy val docs = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, SitePreviewPlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin, CiReleasePlugin)
  .dependsOn(core, testkit)
  .settings(
    name := "Akka Projections",
    publish / skip := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-projection/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Preprocess / preprocessRules := Seq(
        ( // package duplication errors
          (s"https://doc\\.akka\\.io/api/akka-grpc/${akka.grpc.gen.BuildInfo.version}/akka/grpc/akka/grpc").r,
          _ => s"https://doc\\.akka\\.io/api/akka-grpc/${akka.grpc.gen.BuildInfo.version}/akka/grpc/")),
    Paradox / siteSubdirName := s"docs/akka-projection/${projectInfoVersion.value}",
    Compile / paradoxProperties ++= Map(
        "project.url" -> "https://doc.akka.io/docs/akka-projection/current/",
        "canonical.base_url" -> "https://doc.akka.io/docs/akka-projection/current",
        "github.base_url" -> "https://github.com/akka/akka-projection",
        "akka.version" -> Dependencies.Versions.akka,
        "akka.r2dbc.version" -> Dependencies.Versions.akkaPersistenceR2dbc,
        "extref.akka-distributed-cluster.base_url" -> s"https://doc.akka.io/docs/akka-distributed-cluster/${Dependencies.AkkaProjectionVersionInDocs}/%s",
        "extref.akka-edge.base_url" -> s"https://doc.akka.io/docs/akka-edge/${Dependencies.AkkaProjectionVersionInDocs}/%s"),
    commonParadoxProperties,
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxRoots := List("index.html", "getting-started/event-generator-app.html"),
    ApidocPlugin.autoImport.apidocRootPackage := "akka",
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += (makeSite.value -> "www/"),
    publishRsyncHost := "akkarepo@gustav.akka.io",
    apidocRootPackage := "akka")

lazy val `akka-distributed-cluster-docs` = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, SitePreviewPlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin, CiReleasePlugin)
  .dependsOn(core, testkit)
  .settings(
    name := "Akka Distributed Cluster",
    publish / skip := true,
    previewPath := (Paradox / siteSubdirName).value,
    Paradox / siteSubdirName := s"docs/akka-distributed-cluster/${projectInfoVersion.value}",
    commonParadoxProperties,
    Compile / paradoxProperties ++= Map(
        "project.url" -> "https://doc.akka.io/docs/akka-distributed-cluster/current/",
        "canonical.base_url" -> "https://doc.akka.io/docs/akka-distributed-cluster/current",
        "github.base_url" -> "https://github.com/akka/akka-projection",
        "akka.version" -> Dependencies.Versions.akka,
        "akka.r2dbc.version" -> Dependencies.Versions.akkaPersistenceR2dbc,
        "extref.akka-projection.base_url" -> s"https://doc.akka.io/docs/akka-projection/${Dependencies.AkkaProjectionVersionInDocs}/%s",
        "scaladoc.akka.projection.base_url" -> s"https://doc.akka.io/api/akka-projection/${Dependencies.AkkaProjectionVersionInDocs}/"),
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxRoots := List("index.html"),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += (makeSite.value -> "www/"),
    publishRsyncHost := "akkarepo@gustav.akka.io",
    apidocRootPackage := "akka")

lazy val `akka-edge-docs` = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, SitePreviewPlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin, CiReleasePlugin)
  .dependsOn(core, testkit)
  .settings(
    name := "Akka Edge",
    publish / skip := true,
    previewPath := (Paradox / siteSubdirName).value,
    Paradox / siteSubdirName := s"docs/akka-edge/${projectInfoVersion.value}",
    commonParadoxProperties,
    Compile / paradoxProperties ++= Map(
        "project.url" -> "https://doc.akka.io/docs/akka-edge/current/",
        "canonical.base_url" -> "https://doc.akka.io/docs/akka-edge/current",
        "github.base_url" -> "https://github.com/akka/akka-projection",
        "akka.version" -> Dependencies.Versions.akka,
        "akka.r2dbc.version" -> Dependencies.Versions.akkaPersistenceR2dbc,
        "h2.version" -> Dependencies.Compile.h2.revision,
        "r2dbc-h2.version" -> Dependencies.Compile.r2dbcH2.revision,
        "extref.akka-projection.base_url" -> s"https://doc.akka.io/docs/akka-projection/${Dependencies.AkkaProjectionVersionInDocs}/%s",
        "scaladoc.akka.projection.base_url" -> s"https://doc.akka.io/api/akka-projection/${Dependencies.AkkaProjectionVersionInDocs}/",
        "extref.akka-distributed-cluster.base_url" -> s"https://doc.akka.io/docs/akka-distributed-cluster/${Dependencies.AkkaProjectionVersionInDocs}/%s",
        "extref.akka-persistence-r2dbc.base_url" -> s"https://doc.akka.io/docs/akka-persistence-r2dbc/${Dependencies.AkkaPersistenceR2dbcVersionInDocs}/%s",
        // API docs for akka-edge-rs
        "extref.akka-edge-rs-api.base_url" -> s"https://doc.akka.io/api/akka-edge-rs/current/%s"),
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxRoots := List("index.html"),
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += (makeSite.value -> "www/"),
    publishRsyncHost := "akkarepo@gustav.akka.io",
    apidocRootPackage := "akka")

lazy val root = Project(id = "akka-projection", base = file("."))
  .aggregate(
    core,
    coreTest,
    testkit,
    jdbc,
    slick,
    cassandra,
    eventsourced,
    kafka,
    `durable-state`,
    grpc,
    grpcTests,
    r2dbc,
    docs,
    `akka-distributed-cluster-docs`,
    `akka-edge-docs`)
  .settings(publish / skip := true)
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(SitePlugin, MimaPlugin, CiReleasePlugin)

// separate aggregate for integration tests, note that this will create a directory when used (which is then gitignored)
lazy val integrationTests = Project(id = "akka-projection-integration", base = file("akka-projection-integration"))
  .aggregate(
    cassandraIntegration,
    examples,
    grpcIntegration,
    jdbcIntegration,
    kafkaIntegration,
    r2dbcIntegration,
    slickIntegration)
  .settings(publish / skip := true)
  .disablePlugins(SitePlugin, MimaPlugin, CiReleasePlugin)

// check format and headers
TaskKey[Unit]("verifyCodeFmt") := {
  javafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Java code found. Please run 'javafmtAll' and commit the reformatted code")
  }

  scalafmtCheckAll.all(ScopeFilter(inAnyProject)).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted Scala code found. Please run 'scalafmtAll' and commit the reformatted code")
  }

  (Compile / scalafmtSbtCheck).result.value.toEither.left.foreach { _ =>
    throw new MessageOnlyException(
      "Unformatted sbt code found. Please run 'scalafmtSbt' and commit the reformatted code")
  }
}

addCommandAlias("verifyCodeStyle", "headerCheckAll; verifyCodeFmt")

val isJdk11orHigher: Boolean = {
  val result = VersionNumber(sys.props("java.specification.version")).matchesSemVer(SemanticSelector(">=11"))
  if (!result)
    throw new IllegalArgumentException("JDK 11 or higher is required")
  result
}
