/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

// #guideSetup
package jdocs.guide;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.projection.ProjectionBehavior;
import akka.projection.eventsourced.EventEnvelope;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
// #guideSetup

// #guideSourceProviderImports
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.Offset;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;
// #guideSourceProviderImports

// #guideProjectionImports
import akka.projection.ProjectionId;
import akka.projection.cassandra.javadsl.CassandraProjection;
import akka.projection.javadsl.AtLeastOnceProjection;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry;
// #guideProjectionImports

// #guideSetup

public class ShoppingCartApp {
  public static void main(String[] args) throws Exception {
    Config config = ConfigFactory.load("guide-shopping-cart-app.conf");

    ActorSystem.create(
        Behaviors.setup(
            context -> {
              ActorSystem<Void> system = context.getSystem();

              // ...

              // #guideSetup
              // #guideSourceProviderSetup
              SourceProvider<Offset, EventEnvelope<ShoppingCartEvents.Event>> sourceProvider =
                  EventSourcedProvider.eventsByTag(
                      system, CassandraReadJournal.Identifier(), ShoppingCartTags.SINGLE);
              // #guideSourceProviderSetup

              // #guideProjectionSetup
              CassandraSession session =
                  CassandraSessionRegistry.get(system)
                      .sessionFor("akka.projection.cassandra.session-config");
              ItemPopularityProjectionRepositoryImpl repo =
                  new ItemPopularityProjectionRepositoryImpl(session);
              AtLeastOnceProjection<Offset, EventEnvelope<ShoppingCartEvents.Event>> projection =
                  CassandraProjection.atLeastOnce(
                      ProjectionId.of("shopping-carts", ShoppingCartTags.SINGLE),
                      sourceProvider,
                      () ->
                          new ItemPopularityProjectionHandler(
                              ShoppingCartTags.SINGLE, system, repo));

              context.spawn(ProjectionBehavior.create(projection), projection.projectionId().id());
              // #guideProjectionSetup

              // #guideSetup
              return Behaviors.empty();
            }),
        "ShoppingCartApp",
        config);
  }
}
// #guideSetup
