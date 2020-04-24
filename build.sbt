import akka.projections.Dependencies

scalaVersion := Dependencies.Scala213

lazy val core = project
  .in(file("akka-projection-core"))
  .settings(Dependencies.core)
  .settings(
    name := "akka-projection-core",
    Compile / packageBin / packageOptions += Package.ManifestAttributes(
        "Automatic-Module-Name" -> "akka.projection.core"))

lazy val testkit =
  Project(id = "akka-projection-testkit", base = file("akka-projection-testkit"))
    .settings(Dependencies.testKit)
    .dependsOn(core)

// provides offset storage backed by a JDBC (Slick) table
lazy val slick =
  Project(id = "akka-projection-slick", base = file("akka-projection-slick"))
    .settings(Dependencies.slick)
    .dependsOn(core)
    .dependsOn(testkit % "test->test")

// provides offset storage backed by a Cassandra table
lazy val cassandra =
  Project(id = "akka-projection-cassandra", base = file("akka-projection-cassandra"))
    .settings(Dependencies.cassandra)
    .settings(Test / parallelExecution := false)
    .dependsOn(core)
    .dependsOn(testkit % "test->test")

// provides source providers for akka-persistence-query
lazy val eventSourced =
  Project(id = "akka-projection-eventsourced", base = file("akka-projection-eventsourced"))
    .settings(Dependencies.eventSourced)
    .dependsOn(core)
    .dependsOn(testkit % "test->test")

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
    paradoxProperties ++= Map(
        "akka.version" -> Dependencies.Versions.akka,
        // Akka
        "extref.akka.base_url" -> s"https://doc.akka.io/docs/akka/${Dependencies.AkkaVersionInDocs}/%s",
        "scaladoc.akka.base_url" -> s"https://doc.akka.io/api/akka/${Dependencies.AkkaVersionInDocs}/",
        "javadoc.akka.base_url" -> s"https://doc.akka.io/japi/akka/${Dependencies.AkkaVersionInDocs}/",
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
  .aggregate(core, testkit, slick, cassandra, eventSourced, docs)
  .enablePlugins(ScalaUnidocPlugin)
  .disablePlugins(SitePlugin)

// check format and headers
TaskKey[Unit]("verifyCodeFmt") := {
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
