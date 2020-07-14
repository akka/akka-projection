import sbt.Keys._
import sbt._

/**
 * This plugin disables parallel execution in travis
 */
object SequentialTestExecutionInCI extends AutoPlugin {
  override def trigger: PluginTrigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = {
    (sys.env
      .get("TRAVIS") match {
      case Some("true") =>
        Seq(IntegrationTest / parallelExecution := false, Test / parallelExecution := false)
      case _ => Seq.empty
    })
  }
}
