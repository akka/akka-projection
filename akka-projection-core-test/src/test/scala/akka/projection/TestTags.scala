/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import org.scalatest.Tag

object TestTags {

  object InMemoryDb extends Tag("InMemoryDb")
  object ContainerDb extends Tag("ContainerDb")
  object FlakyDb extends Tag("FlakyDb")

}
