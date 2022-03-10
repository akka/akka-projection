/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

//#guideClusterSetup
package docs.guide

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.Offset
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.scaladsl.SourceProvider
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import com.typesafe.config.ConfigFactory

object ShoppingCartClusterApp extends App {
  val port = args.headOption match {
    case Some(portString) if portString.matches("""\d+""") => portString.toInt
    case _                                                 => throw new IllegalArgumentException("An akka cluster port argument is required")
  }

  val config = ConfigFactory
    .parseString(s"akka.remote.artery.canonical.port = $port")
    .withFallback(ConfigFactory.load("guide-shopping-cart-cluster-app.conf"))

  ActorSystem(
    Behaviors.setup[String] { context =>
      val system = context.system
      implicit val ec = system.executionContext
      val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
      val repo = new ItemPopularityProjectionRepositoryImpl(session)

      def sourceProvider(tag: String): SourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]] =
        EventSourcedProvider
          .eventsByTag[ShoppingCartEvents.Event](
            system,
            readJournalPluginId = CassandraReadJournal.Identifier,
            tag = tag)

      def projection(tag: String) =
        CassandraProjection.atLeastOnce(
          projectionId = ProjectionId("shopping-carts", tag),
          sourceProvider(tag),
          handler = () => new ItemPopularityProjectionHandler(tag, system, repo))

      ShardedDaemonProcess(system).init[ProjectionBehavior.Command](
        name = "shopping-carts",
        numberOfInstances = ShoppingCartTags.Tags.size,
        behaviorFactory = (i: Int) => ProjectionBehavior(projection(ShoppingCartTags.Tags(i))),
        stopMessage = ProjectionBehavior.Stop)

      Behaviors.empty
    },
    "ShoppingCartClusterApp",
    config)
}
//#guideClusterSetup
