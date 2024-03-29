import sbt.Keys._
import sbt._

/**
 * This plugins conditionally adds Akka snapshot repository.
 */
object AkkaSnapshotRepositories extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  // If using a snapshot version of either Akka or Alpakka, add both snapshot repos
  // in case there are transitive dependencies to other snapshot artifacts
  override def projectSettings: Seq[Def.Setting[_]] = {
    resolvers ++= (sys.props
      .get("build.akka.version")
      .orElse(sys.props.get("build.alpakka.kafka.version")) match {
      case Some(_) =>
        Seq("Akka library snapshot repository".at("https://repo.akka.io/snapshots"))
      case None => Seq.empty
    })
  }
}
