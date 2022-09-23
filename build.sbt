import akka.projections.Dependencies

lazy val core =
  Project(id = "akka-projection-core", base = file("akka-projection-core"))
    .configs(IntegrationTest)
    .settings(headerSettings(IntegrationTest))
    .settings(Defaults.itSettings)
    .settings(Dependencies.core)
    .settings(
      name := "akka-projection-core",
      Compile / packageBin / packageOptions += Package.ManifestAttributes(
          "Automatic-Module-Name" -> "akka.projection.core"))
    .settings(Protobuf.settings)

lazy val coreTest =
  Project(id = "akka-projection-core-test", base = file("akka-projection-core-test"))
    .configs(IntegrationTest)
    .settings(headerSettings(IntegrationTest))
    .disablePlugins(MimaPlugin)
    .settings(Defaults.itSettings)
    .settings(Dependencies.coreTest)
    .settings(publish / skip := true)
    .dependsOn(core)
    .dependsOn(testkit % Test)

lazy val testkit =
  Project(id = "akka-projection-testkit", base = file("akka-projection-testkit"))
    .configs(IntegrationTest)
    .settings(headerSettings(IntegrationTest))
    .settings(Defaults.itSettings)
    .settings(Dependencies.testKit)
    .dependsOn(core)

// provides offset storage backed by a JDBC table
lazy val jdbc =
  Project(id = "akka-projection-jdbc", base = file("akka-projection-jdbc"))
    .configs(IntegrationTest.extend(Test))
    .settings(headerSettings(IntegrationTest))
    .settings(Defaults.itSettings)
    .settings(Dependencies.jdbc)
    .dependsOn(core)
    .dependsOn(coreTest % "test->test")
    .dependsOn(testkit % Test)

// provides offset storage backed by a JDBC (Slick) table
lazy val slick =
  Project(id = "akka-projection-slick", base = file("akka-projection-slick"))
    .configs(IntegrationTest.extend(Test))
    .settings(headerSettings(IntegrationTest))
    .settings(Defaults.itSettings)
    .settings(Dependencies.slick)
    .dependsOn(jdbc)
    .dependsOn(core)
    .dependsOn(coreTest % "test->test")
    .dependsOn(testkit % Test)

// provides offset storage backed by a Cassandra table
lazy val cassandra =
  Project(id = "akka-projection-cassandra", base = file("akka-projection-cassandra"))
    .configs(IntegrationTest)
    .settings(headerSettings(IntegrationTest))
    .settings(Defaults.itSettings)
    .settings(Dependencies.cassandra)
    .dependsOn(core)
    // strictly speaking it is not needed to have test->test here.
    // Cassandra module doesn't have tests, only integration tests
    // however, without it the generated pom.xml doesn't get this test dependencies
    .dependsOn(coreTest % "test->test;it->test")
    .dependsOn(testkit % "test->compile;it->compile")

// provides source providers for akka-persistence-query
lazy val eventsourced =
  Project(id = "akka-projection-eventsourced", base = file("akka-projection-eventsourced"))
    .settings(Dependencies.eventsourced)
    .dependsOn(core)
    .dependsOn(testkit % Test)

// provides offset storage backed by Kafka managed offset commits
lazy val kafka =
  Project(id = "akka-projection-kafka", base = file("akka-projection-kafka"))
    .configs(IntegrationTest)
    .settings(headerSettings(IntegrationTest))
    .settings(Defaults.itSettings)
    .settings(Dependencies.kafka)
    .dependsOn(core)
    .dependsOn(testkit % Test)
    .dependsOn(slick % "test->test;it->it")

// provides source providers for durable state changes
lazy val `durable-state` =
  Project(id = "akka-projection-durable-state", base = file("akka-projection-durable-state"))
    .configs(IntegrationTest)
    .settings(Dependencies.state)
    .dependsOn(core)
    .dependsOn(testkit % Test)
    .settings(
      // no previous artifact so must disable MiMa until this is released at least once.
      mimaPreviousArtifacts := Set.empty)

lazy val grpc =
  Project(id = "akka-projection-grpc", base = file("akka-projection-grpc"))
    .configs(IntegrationTest)
    .settings(headerSettings(IntegrationTest))
    .settings(Defaults.itSettings)
    .settings(Dependencies.grpc)
    .dependsOn(core)
    .dependsOn(eventsourced)
    .dependsOn(testkit % Test)
    .enablePlugins(AkkaGrpcPlugin)

lazy val examples = project
  .configs(IntegrationTest.extend(Test))
  .settings(headerSettings(IntegrationTest))
  .disablePlugins(MimaPlugin)
  .settings(Defaults.itSettings)
  .settings(Dependencies.examples)
  .dependsOn(slick % "test->test")
  .dependsOn(jdbc % "test->test")
  .dependsOn(cassandra % "test->test;test->it")
  .dependsOn(eventsourced)
  .dependsOn(`durable-state`)
  .dependsOn(kafka % "test->test")
  .dependsOn(testkit % Test)
  .settings(publish / skip := true, scalacOptions += "-feature", javacOptions += "-parameters")

lazy val docs = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .disablePlugins(MimaPlugin)
  .dependsOn(core, testkit)
  .settings(
    name := "Akka Projections",
    publish / skip := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-projection/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Paradox / siteSubdirName := s"docs/akka-projection/${projectInfoVersion.value}",
    Compile / paradoxProperties ++= Map(
        "project.url" -> "https://doc.akka.io/docs/akka-projection/current/",
        "canonical.base_url" -> "https://doc.akka.io/docs/akka-projection/current",
        "github.base_url" -> "https://github.com/akka/akka-projection",
        "akka.version" -> Dependencies.Versions.akka,
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
        // Java
        "javadoc.base_url" -> "https://docs.oracle.com/javase/8/docs/api/",
        // Scala
        "scaladoc.scala.base_url" -> s"https://www.scala-lang.org/api/${scalaBinaryVersion.value}.x/",
        "scaladoc.akka.projection.base_url" -> s"/${(Preprocess / siteSubdirName).value}/",
        "javadoc.akka.projection.base_url" -> "", // no Javadoc is published
        // Misc
        "extref.samples.base_url" -> "https://developer.lightbend.com/start/?group=akka&amp;project=%s",
        "extref.platform-guide.base_url" -> "https://developer.lightbend.com/docs/akka-platform-guide/%s"),
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    paradoxRoots := List("index.html", "getting-started/event-generator-app.html"),
    ApidocPlugin.autoImport.apidocRootPackage := "akka",
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += (makeSite.value -> "www/"),
    publishRsyncHost := "akkarepo@gustav.akka.io",
    apidocRootPackage := "akka")

lazy val root = Project(id = "akka-projection", base = file("."))
  .aggregate(core, coreTest, testkit, jdbc, slick, cassandra, eventsourced, kafka, `durable-state`, examples, docs)
  .settings(publish / skip := true)
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(SitePlugin, MimaPlugin)

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
