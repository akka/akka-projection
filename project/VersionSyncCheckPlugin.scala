import akka.projections.Dependencies
import org.eclipse.jgit.diff.RawText
import sbt.*
import sbt.Keys.*

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import scala.jdk.CollectionConverters.*
import scala.sys.process.*
import scala.util.matching.UnanchoredRegex

object VersionSyncCheckPlugin extends AutoPlugin {
  override def trigger = allRequirements

  object autoImport {
    val akkaVersionSyncCheck = taskKey[Unit]("")
    val akkaGrpcVersionSyncCheck = taskKey[Unit]("")
    val akkaProjectionVersionSyncCheck = taskKey[Unit]("")
    val allVersionSyncChecks = taskKey[Unit]("Runs all version sync checks")
  }
  import autoImport.*

  // FIXME auto discover these from the filesystem instead
  private def allSampleSbtBuildFiles =
    Seq(
      "samples/replicated/shopping-cart-service-scala/build.sbt",
      "samples/grpc/shopping-cart-service-scala/build.sbt",
      "samples/grpc/shopping-analytics-service-scala/build.sbt",
      "samples/grpc/iot-service-scala/build.sbt",
      "samples/grpc/restaurant-drone-deliveries-service-scala/build.sbt",
      "samples/grpc/local-drone-control-scala/build.sbt").map(Paths.get(_))

  private def allSampleMavenBuildFiles =
    Seq(
      "samples/replicated/shopping-cart-service-java/pom.xml",
      "samples/grpc/shopping-cart-service-java/pom.xml",
      "samples/grpc/restaurant-drone-deliveries-service-java/pom.xml",
      "samples/grpc/local-drone-control-java/pom.xml",
      "samples/grpc/shopping-analytics-service-java/pom.xml",
      "samples/grpc/iot-service-java/pom.xml").map(Paths.get(_))

  private def allSampleSbtPluginFiles =
    Seq(
      "samples/replicated/shopping-cart-service-scala/project/plugins.sbt",
      "samples/grpc/shopping-cart-service-scala/project/plugins.sbt",
      "samples/grpc/shopping-analytics-service-scala/project/plugins.sbt",
      "samples/grpc/iot-service-scala/project/plugins.sbt",
      "samples/grpc/restaurant-drone-deliveries-service-scala/project/plugins.sbt",
      "samples/grpc/local-drone-control-scala/project/plugins.sbt").map(Paths.get(_))

  override def globalSettings =
    Seq(
      akkaVersionSyncCheck := versionSyncCheckImpl(
          "Akka core",
          Dependencies.Versions.Akka,
          raw"""(?i)<?akka\.?version.{1,9}(\d+\.\d+\.\d+)""".r.unanchored,
          allSampleSbtBuildFiles ++ allSampleMavenBuildFiles).value,
      akkaGrpcVersionSyncCheck := {
        versionSyncCheckImpl(
          "Akka gRPC",
          Dependencies.Versions.AkkaGrpcVersion,
          raw""""sbt-akka-grpc" % "(\d+\.\d+\.\d+)""".r.unanchored,
          allSampleSbtPluginFiles).value
        versionSyncCheckImpl(
          "Akka gRPC",
          Dependencies.Versions.AkkaGrpcVersion,
          raw"""<akka-grpc-maven-plugin\.version>(\d+\.\d+\.\d+)""".r.unanchored,
          allSampleMavenBuildFiles).value
        versionSyncCheckImpl(
          "Akka gRPC",
          Dependencies.Versions.AkkaGrpcVersion,
          raw"""<akka-grpc\.version>(\d+\.\d+\.\d+)""".r.unanchored,
          allSampleMavenBuildFiles).value
      },
      akkaProjectionVersionSyncCheck := akkaProjectionVersionSyncCheckImpl.value,
      allVersionSyncChecks := {
        akkaVersionSyncCheck.value
        akkaGrpcVersionSyncCheck.value
        akkaProjectionVersionSyncCheck.value
      })

  // Verifies that every sample build file pins the same akka-projection version
  // literal. Unlike the akka/akka-grpc checks there is no single source of
  // truth in build.sbt for the released akka-projection version, so we just
  // ensure all samples agree with each other (catches a missed file in the
  // release-prep bump).
  private def akkaProjectionVersionSyncCheckImpl = Def.task[Unit] {
    val log = state.value.log
    val SbtRegex = raw"""sys\.props\.getOrElse\("akka-projection\.version", "([^"]+)"\)""".r.unanchored
    val PomRegex = raw"""<akka-projection\.version>([^<]+)</akka-projection\.version>""".r.unanchored

    def extract(path: Path, regex: UnanchoredRegex): Option[String] =
      Files
        .lines(path)
        .iterator
        .asScala
        .collectFirst({ case regex(v) => v })

    val sbtVersions = allSampleSbtBuildFiles.map(p => p -> extract(p, SbtRegex))
    val pomVersions = allSampleMavenBuildFiles.map(p => p -> extract(p, PomRegex))
    val all = sbtVersions ++ pomVersions

    val missing = all.collect { case (p, None) => p }
    if (missing.nonEmpty) {
      missing.foreach(p => log.error(s"Could not find akka-projection version literal in $p"))
      fail("Akka Projection version sync check failed: missing version literal")
    }

    val versions = all.collect { case (p, Some(v)) => p -> v }
    val distinct = versions.map(_._2).toSet
    if (distinct.size > 1) {
      versions.foreach { case (p, v) => log.error(s"$p -> $v") }
      fail(s"Akka Projection version sync check failed: samples disagree, found versions $distinct")
    }
    log.info(s"Akka Projection version sync check success: all samples on ${distinct.headOption.getOrElse("?")}")
  }

  def versionSyncCheckImpl(
      name: String,
      expectedVersion: String,
      VersionRegex: UnanchoredRegex,
      knownFiles: Seq[Path],
      ignoredFiles: Set[Path] = Set.empty) =
    Def.task[Unit] {
      val log = state.value.log
      log.info(s"Running $name version sync check, expecting version $expectedVersion")

      def versions(path: Path): (Path, Seq[String]) =
        (
          path,
          Files
            .lines(path)
            .iterator
            .asScala
            .collect({
              case VersionRegex(version) => version
            })
            .toSeq)

      log.info("Sanity checking regex extraction against known files")
      val mismatchVersions =
        knownFiles.filterNot(ignoredFiles).map(versions).filterNot(_._2.toSet == Set(expectedVersion)).toVector
      if (mismatchVersions.isEmpty) {
        log.info("Sanity check passed")
      } else {
        mismatchVersions.foreach {
          case (path, versions) =>
            log.error(s"Found sanity check $name version mismatch: $path -> $versions")
        }
        fail("Sanity check failed")
      }

      val buildBase = (ThisBuild / baseDirectory).value
      val process = Process("git ls-tree -z --full-tree -r --name-only HEAD", buildBase)
      val paths = (process !! log).trim
        .split('\u0000')
        .iterator
        .map(path => Paths.get(path))
        .filter(Files.exists(_))
        .filterNot(ignoredFiles)
        .filterNot(path => RawText.isBinary(Files.newInputStream(path)))
        .filterNot(path => path.toString.endsWith(".enc")) // encrypted blob

      var mismatch = false

      for ((path, versions) <- paths.map(versions(_)).filter(_._2.nonEmpty)) {
        if (versions.forall(_ == expectedVersion)) {
          log.info(s"Found matching $name version $expectedVersion in $path")
        } else {
          log.error(s"Found $name version mismatch: $path -> $versions")
          mismatch = true
        }
      }

      if (mismatch) {
        fail(s"$name version sync check failed, expected $expectedVersion")
      }

      log.info(s"$name version sync check success")
    }

  private def fail(message: String): Nothing = {
    val fail = new MessageOnlyException(message)
    fail.setStackTrace(new Array[StackTraceElement](0))
    throw fail
  }
}
