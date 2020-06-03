/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.typesafe.config.Config
import org.scalatest.wordspec.AnyWordSpecLike

class JdbcSpec(config: Config) extends ScalaTestWithActorTestKit(config) with AnyWordSpecLike with LogCapturing
