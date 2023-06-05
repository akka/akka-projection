/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config = {
    val defaultConfig = ConfigFactory.load()
    val dialect = defaultConfig.getString("akka.persistence.r2dbc.connection-factory.dialect")

    val dialectConfig = dialect match {
      case "h2" =>
        // we need a new file each time or else we get file lock problems
        // FIXME why is that?
        ConfigFactory.parseString(s"""
          akka.persistence.r2dbc.connection-factory {
            protocol = "file"
            database = "./target/h2-test-db-${System.currentTimeMillis}"
          }
          """)
      case _ =>
        // defaults are fine
        ConfigFactory.empty()
    }

    // using load here so that connection-factory can be overridden
    dialectConfig
      .withFallback(
        ConfigFactory
          .parseString("""
    akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
    akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
      }
    }
    akka.actor.testkit.typed.default-timeout = 10s
    """))
      .withFallback(ConfigFactory.load())
  }
}
