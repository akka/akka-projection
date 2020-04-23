/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra

import java.net.InetSocketAddress

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Try

import akka.stream.alpakka.cassandra.CqlSessionProvider
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint
import com.dimafeng.testcontainers.CassandraContainer

/**
 * Use testcontainers to lazily provide a single CqlSession for all Alpakka Cassandra tests
 */
final class ContainerSessionProvider extends CqlSessionProvider {
  import ContainerSessionProvider._

  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] = started.future.map { _ =>
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
  private lazy val container: CassandraContainer = CassandraContainer()
  private lazy val started = Promise[Unit].complete(Try(container.start()))

  val Config =
    """
       akka.projection.cassandra.session-provider = "akka.projection.cassandra.ContainerSessionProvider"
    """
}
