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
      case "postgres" =>
        // defaults are fine
        ConfigFactory.empty()
      case "yugabyte" =>
        // defaults are fine
        ConfigFactory.empty()
      case "h2" =>
        val tq = "\"\"\""
        val schema =
          """
            |;CREATE TABLE IF NOT EXISTS akka_projection_offset_store (
            |  projection_name VARCHAR(255) NOT NULL,
            |  projection_key VARCHAR(255) NOT NULL,
            |  current_offset VARCHAR(255) NOT NULL,
            |  manifest VARCHAR(32) NOT NULL,
            |  mergeable BOOLEAN NOT NULL,
            |  last_updated BIGINT NOT NULL,
            |  PRIMARY KEY(projection_name, projection_key)
            |);CREATE TABLE IF NOT EXISTS akka_projection_timestamp_offset_store (
            |  projection_name VARCHAR(255) NOT NULL,
            |  projection_key VARCHAR(255) NOT NULL,
            |  slice INT NOT NULL,
            |  persistence_id VARCHAR(255) NOT NULL,
            |  seq_nr BIGINT NOT NULL,
            |  timestamp_offset timestamp with time zone NOT NULL,
            |  timestamp_consumed timestamp with time zone NOT NULL,
            |  PRIMARY KEY(slice, projection_name, timestamp_offset, persistence_id, seq_nr)
            |);
            |CREATE TABLE IF NOT EXISTS akka_projection_management (
            |  projection_name VARCHAR(255) NOT NULL,
            |  projection_key VARCHAR(255) NOT NULL,
            |  paused BOOLEAN NOT NULL,
            |  last_updated BIGINT NOT NULL,
            |  PRIMARY KEY(projection_name, projection_key)
            |)""".stripMargin.replace("\n", "") // FIXME move shape-up-string-logic to r2dbc plugin
        ConfigFactory.parseString(s"""
          akka.persistence.r2dbc.connection-factory {
            protocol = "file"
            database = "./target/h2-test-db-${System.currentTimeMillis}"
            trace-logging = on

            additional-init = $tq$schema$tq
          }
          """)
    }

    // using load here so that connection-factory can be overridden
    ConfigFactory
      .load(
        dialectConfig.withFallback(
          ConfigFactory.parseString("""
    akka.persistence.journal.plugin = "akka.persistence.r2dbc.journal"
    akka.persistence.state.plugin = "akka.persistence.r2dbc.state"
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
      }
    }
    akka.actor.testkit.typed.default-timeout = 10s
    """)))
      .withFallback(defaultConfig)
  }
}
