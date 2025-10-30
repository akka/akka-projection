/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc

import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy

class TestContainerConf {
  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  private val container: PostgreSQLContainer[_] = new PostgreSQLContainer("postgres:13.1")
  container.withInitScript("db/default-init.sql")
  container.withStartupCheckStrategy(new IsRunningStartupCheckStrategy)
  container.withStartupAttempts(5)
  container.start()

  def config: Config =
    ConfigFactory
      .parseString(s"""
     akka.http.server.preview.enable-http2 = on
     akka.projection.grpc {
       consumer.client {
         host = "127.0.0.1"
         port = $grpcPort
         use-tls = false
       }
       producer {
         query-plugin-id = "akka.persistence.r2dbc.query"
       }
     }
     akka.persistence.r2dbc.connection-factory = $${akka.persistence.r2dbc.postgres}
     akka.persistence.r2dbc.connection-factory = {
       host = "${container.getHost}"
       port = ${container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)}
       database = "${container.getDatabaseName}"
       user = "${container.getUsername}"
       password = "${container.getPassword}"
     }
     """)
      .withFallback(ConfigFactory.load("persistence.conf"))
      .resolve()

  def stop(): Unit = container.stop()
}
