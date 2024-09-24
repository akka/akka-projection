import akka.projections.Dependencies
import sbtdynver.DynVerPlugin.autoImport._
import com.lightbend.paradox.projectinfo.ParadoxProjectInfoPluginKeys._
import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbt.Keys._
import sbt._
import sbt.plugins.JvmPlugin
import com.typesafe.tools.mima.plugin.MimaKeys._

object Common extends AutoPlugin {

  override def trigger = allRequirements

  override def requires = JvmPlugin

  override def globalSettings =
    Seq(
      organization := "com.lightbend.akka",
      organizationName := "Lightbend Inc.",
      organizationHomepage := Some(url("https://www.lightbend.com/")),
      startYear := Some(2020),
      homepage := Some(url("https://akka.io")),
      // apiURL defined in projectSettings because version.value is not correct here
      scmInfo := Some(
          ScmInfo(url("https://github.com/akka/akka-projection"), "git@github.com:akka/akka-projection.git")),
      developers += Developer(
          "contributors",
          "Contributors",
          "https://gitter.im/akka/dev",
          url("https://github.com/akka/akka-projection/graphs/contributors")),
      licenses := {
        val tagOrBranch =
          if (version.value.endsWith("SNAPSHOT")) "main"
          else "v" + version.value
        Seq(("BUSL-1.1", url(s"https://raw.githubusercontent.com/akka/akka-projection/${tagOrBranch}/LICENSE")))
      },
      description := "Akka Projection.",
      excludeLintKeys += scmInfo,
      excludeLintKeys += mimaPreviousArtifacts,
      excludeLintKeys += testOptions,
      excludeLintKeys += logBuffered)

  override lazy val projectSettings = Seq(
    projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
    crossVersion := CrossVersion.binary,
    crossScalaVersions := Dependencies.ScalaVersions,
    scalaVersion := Dependencies.Scala213,
    Compile / javacOptions ++= List("-Xlint:unchecked", "-Xlint:deprecation", "--release", "11"),
    Compile / scalacOptions ++= Seq("-release", "11"),
    Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
        "-doc-title",
        "Akka Projection",
        "-doc-version",
        version.value,
        "-sourcepath",
        (ThisBuild / baseDirectory).value.toString,
        "-doc-source-url", {
          val branch = if (isSnapshot.value) "main" else s"v${version.value}"
          s"https://github.com/akka/akka-projection/tree/${branch}€{FILE_PATH_EXT}#L€{FILE_LINE}"
        })
      ++ {
        if (scalaBinaryVersion.value.startsWith("3")) {
          Seq(
            s"-external-mappings:https://docs.oracle.com/en/java/javase/${Dependencies.JavaDocLinkVersion}/docs/api/java.base/",
            "-skip-packages:akka.pattern")
        } else {
          Seq(
            "-jdk-api-doc-base",
            s"https://docs.oracle.com/en/java/javase/${Dependencies.JavaDocLinkVersion}/docs/api/java.base/",
            "-skip-packages",
            "akka.pattern")
        }
      },
    scalafmtOnCompile := System.getenv("CI") != "true",
    autoAPIMappings := true,
    apiURL := Some(url(s"https://doc.akka.io/api/akka-projection/${projectInfoVersion.value}")),
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
    Test / parallelExecution := false,
    // -a Show stack traces and exception class name for AssertionErrors.
    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -q Suppress stdout for successful tests.
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"),
    Test / logBuffered := false,
    mimaPreviousArtifacts := {
      moduleName.value match {
        case name if name.endsWith("-tests") => Set.empty
        case _ =>
          Set(
            organization.value %% moduleName.value % previousStableVersion.value
              .getOrElse(throw new Error("Unable to determine previous version")))

      }
    })

}
