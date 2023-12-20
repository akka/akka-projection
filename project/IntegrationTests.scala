package akka.projections

import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.headerSettings
import sbt.Keys.*
import sbt.*

object IntegrationTests {

  val IntegrationTestTag = Tags.Tag("IntegrationTest")

  def settings: Seq[Def.Setting[_]] =
    Seq(
      publish / skip := true,
      doc / sources := Seq.empty,
      Test / fork := true,
      Test / javaOptions ++= {
        import scala.collection.JavaConverters._
        // include all passed -Dakka. properties to the javaOptions for forked tests
        // useful to switch DB dialects for example
        val akkaProperties = System.getProperties.stringPropertyNames.asScala.toList.collect {
          case key: String if key.startsWith("akka.") || key.startsWith("config") =>
            "-D" + key + "=" + System.getProperty(key)
        }
        akkaProperties
      },
      // run one integration test at a time
      // FIXME is this really needed?
      test / tags := Seq(IntegrationTestTag -> 1),
      Global / concurrentRestrictions := Seq(Tags.limit(IntegrationTestTag, 1))) ++ headerSettings(Test)

}
