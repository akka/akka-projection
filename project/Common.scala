import akka.projections.Dependencies
import bintray.BintrayPlugin.autoImport._
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
      licenses := Seq(("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0"))),
      description := "Akka Projection."
      //resolvers += Resolver.bintrayRepo("akka", "snapshots")
    )

  override lazy val projectSettings = Seq(
    projectInfoVersion := (if (isSnapshot.value) "snapshot" else version.value),
    crossVersion := CrossVersion.binary,
    crossScalaVersions := Dependencies.ScalaVersions,
    scalaVersion := Dependencies.Scala213,
    javacOptions ++= List("-Xlint:unchecked", "-Xlint:deprecation"),
    Compile / doc / scalacOptions := scalacOptions.value ++ Seq(
        "-doc-title",
        "Akka Projection",
        "-doc-version",
        version.value,
        "-sourcepath",
        (baseDirectory in ThisBuild).value.toString,
        "-doc-source-url", {
          val branch = if (isSnapshot.value) "master" else s"v${version.value}"
          s"https://github.com/akka/akka-projection/tree/${branch}€{FILE_PATH_EXT}#L€{FILE_LINE}"
        },
        "-skip-packages",
        "akka.pattern" // for some reason Scaladoc creates this
      ),
    scalafmtOnCompile := true,
    autoAPIMappings := true,
    apiURL := Some(url(s"https://doc.akka.io/api/akka-projection/${projectInfoVersion.value}")),
    // show full stack traces and test case durations
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF"),
    // -a Show stack traces and exception class name for AssertionErrors.
    // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
    // -q Suppress stdout for successful tests.
    Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-a", "-v", "-q"),
    Test / logBuffered := false,
    bintrayOrganization := Some("akka"),
    bintrayPackage := "akka-projection",
    bintrayRepository := (if (isSnapshot.value) "snapshots" else "maven"),

    mimaPreviousArtifacts := Set(organization.value %% moduleName.value % "1.0.0"))
    
}
