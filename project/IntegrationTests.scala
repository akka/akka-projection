package akka.projections

import sbt.Keys._
import sbt._

object IntegrationTests {
  def settings: Seq[Def.Setting[_]] = Seq(publish / skip := true, doc / sources := Seq.empty, Test / fork := true)

}
