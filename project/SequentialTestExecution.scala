import sbt.Def
import sbt.Keys._
import sbt._

/**
 * This plugin disables parallel execution of integration tests by limiting 1 sbt task to run at a time in the
 * `IntegrationTest` sbt configuration. The `parallelExecution` setting is normally used to limit test task
 * concurrency, but it only applies per project. Since projections is a multi-project build with container-based
 * integration tests in most sub-project's, this could result in as many test tasks running concurrently as there were
 * sub-projects with integration tests and cores available on the system.
 *
 * Unit tests in the `Test` configuration will continue to use default `concurrentRestrictions` and `parallelExecution`
 * settings.
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
