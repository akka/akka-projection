/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.r2dbc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config =
    ConfigFactory
      .parseString("""
    akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
    akka.persistence.snapshot-store.plugin = "akka.persistence.r2dbc.snapshot"
    akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
      }
    }
    akka.actor.testkit.typed.default-timeout = 10s
    """)
      .withFallback(ConfigFactory.load()) // dialect chosen through different application-[...].conf files
}
