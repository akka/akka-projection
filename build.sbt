import akka.projections.Dependencies

lazy val core =
  Project(id = "akka-projection-core", base = file("akka-projection-core"))
    .settings(Dependencies.core)
    .settings(
      name := "akka-projection-core",
      Compile / packageBin / packageOptions += Package.ManifestAttributes(
          "Automatic-Module-Name" -> "akka.projection.core"))
    .settings(Protobuf.settings)

lazy val testkit =
  Project(id = "akka-projection-testkit", base = file("akka-projection-testkit"))
    .settings(Dependencies.testKit)
    .dependsOn(core)

// provides offset storage backed by a JDBC table
lazy val jdbc =
  Project(id = "akka-projection-jdbc", base = file("akka-projection-jdbc"))
    .settings(Dependencies.jdbc)
    .dependsOn(core % "compile->compile;test->test")
    .dependsOn(testkit % "test->test")

// provides offset storage backed by a JDBC (Slick) table
lazy val slick =
  Project(id = "akka-projection-slick", base = file("akka-projection-slick"))
    .settings(Dependencies.slick)
    .dependsOn(core % "compile->compile;test->test")
    .dependsOn(testkit % "test->test")

// provides offset storage backed by a Cassandra table
lazy val cassandra =
  Project(id = "akka-projection-cassandra", base = file("akka-projection-cassandra"))
    .settings(Dependencies.cassandra)
    .settings(Test / parallelExecution := false)
    .dependsOn(core % "compile->compile;test->test")
    .dependsOn(testkit % "test->test")

// provides source providers for akka-persistence-query
lazy val eventsourced =
  Project(id = "akka-projection-eventsourced", base = file("akka-projection-eventsourced"))
    .settings(Dependencies.eventsourced)
    .dependsOn(core)
    .dependsOn(testkit % "test->test")

// provides offset storage backed by Kafka managed offset commits
lazy val kafka =
  Project(id = "akka-projection-kafka", base = file("akka-projection-kafka"))
    .settings(Dependencies.kafka)
    .settings(Test / parallelExecution := false)
    .dependsOn(core)
    .dependsOn(testkit % "test->test")
    .dependsOn(slick % "test->test;test->compile")

lazy val examples = project
  .settings(Dependencies.examples)
  .dependsOn(slick % "test->test")
  .dependsOn(jdbc % "test->test")
  .dependsOn(cassandra % "test->test")
  .dependsOn(eventsourced)
  .dependsOn(kafka % "test->test")
  .dependsOn(testkit % "test->test")
  .settings(Test / parallelExecution := false, publish / skip := true, scalacOptions += "-feature")

lazy val docs = project
  .enablePlugins(AkkaParadoxPlugin, ParadoxSitePlugin, PreprocessPlugin, PublishRsyncPlugin)
  .dependsOn(core, testkit)
  .settings(
    name := "Akka Projection",
    publish / skip := true,
    whitesourceIgnore := true,
    makeSite := makeSite.dependsOn(LocalRootProject / ScalaUnidoc / doc).value,
    previewPath := (Paradox / siteSubdirName).value,
    Preprocess / siteSubdirName := s"api/akka-projection/${projectInfoVersion.value}",
    Preprocess / sourceDirectory := (LocalRootProject / ScalaUnidoc / unidoc / target).value,
    Paradox / siteSubdirName := s"docs/akka-projection/${projectInfoVersion.value}",
    Compile / paradoxProperties ++= Map(
        "project.url" -> "https://doc.akka.io/docs/akka-projection/current/",
        "canonical.base_url" -> "https://doc.akka.io/docs/akka-projection/current",
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
        "javadoc.akka.projection.base_url" -> ""
      ), // no Javadoc is published
    paradoxGroups := Map("Language" -> Seq("Java", "Scala")),
    ApidocPlugin.autoImport.apidocRootPackage := "akka",
    resolvers += Resolver.jcenterRepo,
    publishRsyncArtifacts += (makeSite.value -> "www/"),
    publishRsyncHost := "akkarepo@gustav.akka.io",
    apidocRootPackage := "akka")

lazy val root = Project(id = "akka-projection", base = file("."))
  .aggregate(core, testkit, jdbc, slick, cassandra, eventsourced, kafka, examples, docs)
  .settings(publish / skip := true, whitesourceIgnore := true)
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(SitePlugin)

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
