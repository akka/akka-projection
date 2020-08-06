import sbt.Def
import sbt.Keys._
import sbt._

/**
 * On travis this plugin Disables parallel execution by limiting 1 sbt task to run at a time.
 * Outside of travis this plugin uses sbt's default task concurrency configuration and disables parallel execution of
 * tests within a task.
 */
object SequentialTestExecution extends AutoPlugin {
  val IntegrationTestTag = Tags.Tag("IntegrationTest")

  override def trigger: PluginTrigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = {
    /*
      1. `parallelExecution := false` will only run tests in parallel per sbt task. When multiple cores are
      available sbt's default configuration will run 1 task per sub-project, but each task will run each test method
      in sequence

      2. When `concurrentRestrictions` and `parallelExecution` are used together sbt always appears to
      invalidate the `concurrentRestriction` setting and run multiple project tests at once (one per task):

      ```
      Seq(
        IntegrationTest / parallelExecution := false,
        Test / parallelExecution := false,
        concurrentRestrictions in Global += Tags.limit(Tags.Test, 1))
      ```
     */
    sys.env.get("TRAVIS") match {
      case Some("true") => Nil
      case _            => Seq(IntegrationTest / parallelExecution := false, Test / parallelExecution := false)
    }

  }

  override def globalSettings: Seq[Def.Setting[_]] = {
    Seq(tags in test in IntegrationTest := Seq(IntegrationTestTag -> 1)) ++
    (sys.env.get("TRAVIS") match {
      case Some("true") => Seq(concurrentRestrictions in Global := Seq(Tags.limit(IntegrationTestTag, 1)))
      case _            => Nil
    })
  }
}
