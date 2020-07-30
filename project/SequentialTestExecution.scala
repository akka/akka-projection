import sbt.Keys._
import sbt._

/**
 * On travis this plugin Disables parallel execution by limiting 1 task to run at a time.
 * Outside of travis this plugin uses default task concurrency configuration, but disables parallel execution of tests
 * within a task.
 */
object SequentialTestExecution extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = {
    (sys.env
      .get("TRAVIS") match {
      case Some("true") => Seq(concurrentRestrictions in Global += Tags.limit(Tags.Test, 1))
      // NOTE:
      // When `concurrentRestrictions in Global` and `*Test/parallelExecution` are used together it always appears to
      // invalidate the `concurrentRestriction` setting and run multiple project tests at once (one per task):
      //
      //        Seq(
      //          IntegrationTest / parallelExecution := false,
      //          Test / parallelExecution := false,
      //          concurrentRestrictions in Global += Tags.limit(Tags.Test, 1))
      case _ => Seq(IntegrationTest / parallelExecution := false, Test / parallelExecution := false)
    })
  }
}
