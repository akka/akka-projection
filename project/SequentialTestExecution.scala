import sbt.Keys._
import sbt._

/**
 * On travis this plugin Disables parallel execution by limiting 1 sbt task to run at a time.
 * Outside of travis this plugin uses sbt's default task concurrency configuration and disables parallel execution of
 * tests within a task.
 */
object SequentialTestExecution extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = {
    sys.env
      .get("TRAVIS") match {
      case Some("true") => Seq(concurrentRestrictions in Global += Tags.limit(Tags.Test, 1))
      /*
          Observations:

          1. `parallelExecution := false` will only run tests in parallel per sbt task. When multiple cores are
          available sbt's default configuration will run 1 task per sub-project, but each task will run each test method
          in sequence

          2. When `concurrentRestrictions` and `parallelExecution` are used together sbt always appears to
          invalidate the `concurrentRestriction` setting and run multiple project tests at once (one per task).

          ```
          Seq(
            IntegrationTest / parallelExecution := false,
            Test / parallelExecution := false,
            concurrentRestrictions in Global += Tags.limit(Tags.Test, 1))
          ```
       */
      case _ => Seq(IntegrationTest / parallelExecution := false, Test / parallelExecution := false)
    }
  }
}
