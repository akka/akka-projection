/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.dynamodb

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

object TestConfig {
  lazy val config: Config = {
    val defaultConfig = ConfigFactory.load()

    ConfigFactory
      .parseString("""
      akka.loglevel = DEBUG
      akka.persistence.journal.plugin = "akka.persistence.dynamodb.journal"
      akka.persistence.snapshot-store.plugin = "akka.persistence.dynamodb.snapshot"
      akka.persistence.dynamodb {
        query {
          refresh-interval = 1s
        }
        client.local.enabled = true
      }
      akka.actor.testkit.typed.default-timeout = 10s
      """)
      .withFallback(defaultConfig)
  }

}
