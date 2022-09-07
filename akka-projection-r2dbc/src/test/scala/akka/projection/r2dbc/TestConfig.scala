/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config = {
    val defaultConfig = ConfigFactory.load()
    val dialect = defaultConfig.getString("akka.projection.r2dbc.dialect")

    val dialectConfig = dialect match {
      case "postgres" =>
        ConfigFactory.parseString("""
          akka.persistence.r2dbc.connection-factory {
            driver = "postgres"
            host = "localhost"
            port = 5432
            user = "postgres"
            password = "postgres"
            database = "postgres"
          }
          """)
      case "yugabyte" =>
        ConfigFactory.parseString("""
          akka.persistence.r2dbc.connection-factory {
            driver = "postgres"
            host = "localhost"
            port = 5433
            user = "yugabyte"
            password = "yugabyte"
            database = "yugabyte"
          }
          """)
    }

    // using load here so that connection-factory can be overridden
    ConfigFactory.load(dialectConfig.withFallback(ConfigFactory.parseString("""
    akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
    akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
      }
    }
    akka.actor.testkit.typed.default-timeout = 10s
    """)))
  }
}
