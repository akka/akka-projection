/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra

import java.net.InetSocketAddress

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.stream.alpakka.cassandra.CqlSessionProvider
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint

/**
 * Use testcontainers to lazily provide a single CqlSession for all Cassandra tests
 */
final class ContainerSessionProvider extends CqlSessionProvider {

  override def connect()(implicit ec: ExecutionContext): Future[CqlSession] =
    Future.successful(
      CqlSession.builder
        .addContactEndPoint(new DefaultEndPoint(InetSocketAddress
          .createUnresolved("127.0.0.1", 9042)))
        .withLocalDatacenter("datacenter1")
        .build())
}

object ContainerSessionProvider {
  lazy val started: Future[Unit] = Future.successful(())

  val Config = ""
}
