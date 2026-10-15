/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

/**
 * Keep alive enabled for consumers and replication, to run the regular integration suites with keep alives flowing.
 */
object KeepAliveTestConfig {
  val config: Config = ConfigFactory.parseString("""
    akka.projection.grpc.consumer {
      keep-alive-interval = 1s
      keep-alive-timeout = 3s
    }
    akka.projection.grpc.replication {
      keep-alive-interval = 1s
      keep-alive-timeout = 3s
    }
    """)
}
