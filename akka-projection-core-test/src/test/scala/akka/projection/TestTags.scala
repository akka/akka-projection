/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection

import org.scalatest.Tag

object TestTags {

  object InMemoryDb extends Tag("InMemoryDb")
  object ContainerDb extends Tag("ContainerDb")
  object FlakyDb extends Tag("FlakyDb")

}
