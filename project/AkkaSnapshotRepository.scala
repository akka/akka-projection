import sbt.Keys._
import sbt._

/**
 * This plugins conditionally adds Akka snapshot repository.
 */
object AkkaSnapshotRepositories extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements
  override def projectSettings: Seq[Def.Setting[_]] = {
    resolvers ++= (sys.props.get("build.akka.version") match {
      case Some(_) =>
        Seq(
          "akka-snapshot-repository".at("https://repo.akka.io/snapshots")
        )
      case None => Seq.empty
    })
  }
}