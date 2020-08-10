import sbt.Def
import sbt.Keys._
import sbt._

/**
 * This plugin disables parallel execution of integration tests by limiting 1 sbt task to run at a time.
 * Any unit tests in test configuration use default concurrentRestrictions parallelExecution settings.
 */
object SequentialTestExecution extends AutoPlugin {
  val IntegrationTestTag = Tags.Tag("IntegrationTest")

  override def trigger: PluginTrigger = allRequirements

  override def globalSettings: Seq[Def.Setting[_]] = {
    Seq(
      tags in test in IntegrationTest := Seq(IntegrationTestTag -> 1),
      concurrentRestrictions in Global := Seq(Tags.limit(IntegrationTestTag, 1)))
  }
}
