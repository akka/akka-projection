package akka.projections

import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport.headerSettings
import sbt.Keys.*
import sbt.*

object IntegrationTests {

  def settings: Seq[Def.Setting[_]] =
    Seq(publish / skip := true, doc / sources := Seq.empty, Test / fork := true, Test / javaOptions ++= {
      import scala.collection.JavaConverters._
      // include all passed -Dakka. properties to the javaOptions for forked tests
      // useful to switch DB dialects for example
      val akkaProperties = System.getProperties.stringPropertyNames.asScala.toList.collect {
        case key: String if key.startsWith("akka.") || key.startsWith("config") =>
          "-D" + key + "=" + System.getProperty(key)
      }
      akkaProperties
    }) ++ headerSettings(Test)

}
