/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra

import java.net.InetSocketAddress

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Try

import akka.stream.alpakka.cassandra.CqlSessionProvider
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint
import com.dimafeng.testcontainers.CassandraContainer
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy

/**
 * Use testcontainers to lazily provide a single CqlSession for all Cassandra tests
 */
final class ContainerSessionProvider extends CqlSessionProvider {
  import ContainerSessionProvider._

  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] = started.map { _ =>
    CqlSession.builder
      .addContactEndPoint(
        new DefaultEndPoint(
          InetSocketAddress
            .createUnresolved(
              container.cassandraContainer.getContainerIpAddress,
              container.cassandraContainer.getFirstMappedPort.intValue())))
      .withLocalDatacenter("datacenter1")
      .build()
  }
}

object ContainerSessionProvider {
  private val disabled = java.lang.Boolean.getBoolean("disable-cassandra-testcontainer")
  private lazy val container: CassandraContainer = CassandraContainer()
  lazy val started: Future[Unit] = {
    if (disabled)
      Future.successful(())
    else
      Future.fromTry(Try {
        container.underlyingUnsafeContainer
          .withStartupCheckStrategy(new IsRunningStartupCheckStrategy)
          .withStartupAttempts(5)
        container.start()
      })
  }

  val Config =
    if (disabled)
      ""
    else
      """
      akka.projection.cassandra.session-config.session-provider = "akka.projection.cassandra.ContainerSessionProvider"
      """
}
