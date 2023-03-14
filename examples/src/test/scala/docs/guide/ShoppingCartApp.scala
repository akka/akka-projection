/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

//#guideSetup
package docs.guide

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.ProjectionBehavior
import akka.projection.eventsourced.EventEnvelope
import com.typesafe.config.ConfigFactory
//#guideSetup

//#guideSourceProviderImports
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.Offset
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.scaladsl.SourceProvider
//#guideSourceProviderImports

//#guideProjectionImports
import akka.projection.ProjectionId
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
//#guideProjectionImports

//#guideSetup

object ShoppingCartApp extends App {
  val config = ConfigFactory.load("guide-shopping-cart-app.conf")

  ActorSystem(
    Behaviors.setup[String] { context =>
      val system = context.system

      // ...

      //#guideSetup
      //#guideSourceProviderSetup
      val sourceProvider: SourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]] =
        EventSourcedProvider
          .eventsByTag[ShoppingCartEvents.Event](
            system,
            readJournalPluginId = CassandraReadJournal.Identifier,
            tag = ShoppingCartTags.Single)
      //#guideSourceProviderSetup

      //#guideProjectionSetup
      implicit val ec = system.executionContext
      val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
      val repo = new ItemPopularityProjectionRepositoryImpl(session)
      val projection = CassandraProjection.atLeastOnce(
        projectionId = ProjectionId("shopping-carts", ShoppingCartTags.Single),
        sourceProvider,
        handler = () => new ItemPopularityProjectionHandler(ShoppingCartTags.Single, system, repo))

      context.spawn(ProjectionBehavior(projection), projection.projectionId.id)
      //#guideProjectionSetup

      //#guideSetup
      Behaviors.empty
    },
    "ShoppingCartApp",
    config)
}
//#guideSetup
