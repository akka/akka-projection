/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

// #guideClusterSetup
package jdocs.guide;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ShardedDaemonProcess;
import akka.projection.ProjectionBehavior;
import akka.projection.eventsourced.EventEnvelope;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.Offset;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;
import akka.projection.ProjectionId;
import akka.projection.cassandra.javadsl.CassandraProjection;
import akka.projection.javadsl.AtLeastOnceProjection;
import akka.stream.alpakka.cassandra.javadsl.CassandraSession;
import akka.stream.alpakka.cassandra.javadsl.CassandraSessionRegistry;

public class ShoppingCartClusterApp {
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException("An akka cluster port argument is required");
    }

    String portString = args[0];
    int port = Integer.parseInt(portString);

    Config config =
        ConfigFactory.parseString("akka.remote.artery.canonical.port = " + port)
            .withFallback(ConfigFactory.load("guide-shopping-cart-cluster-app.conf"));

    ActorSystem.create(
        Behaviors.setup(
            context -> {
              ActorSystem<Void> system = context.getSystem();

              CassandraSession session =
                  CassandraSessionRegistry.get(system)
                      .sessionFor("akka.projection.cassandra.session-config");
              ItemPopularityProjectionRepositoryImpl repo =
                  new ItemPopularityProjectionRepositoryImpl(session);

              ShardedDaemonProcess.get(system)
                  .init(
                      ProjectionBehavior.Command.class,
                      "shopping-carts",
                      ShoppingCartTags.TAGS.length,
                      n ->
                          ProjectionBehavior.create(
                              projection(system, repo, ShoppingCartTags.TAGS[n])),
                      ProjectionBehavior.stopMessage());

              return Behaviors.empty();
            }),
        "ShoppingCartClusterApp",
        config);
  }

  static SourceProvider<Offset, EventEnvelope<ShoppingCartEvents.Event>> sourceProvider(
      ActorSystem<?> system, String tag) {
    return EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), tag);
  }

  static AtLeastOnceProjection<Offset, EventEnvelope<ShoppingCartEvents.Event>> projection(
      ActorSystem<?> system, ItemPopularityProjectionRepository repo, String tag) {
    return CassandraProjection.atLeastOnce(
        ProjectionId.of("shopping-carts", tag),
        sourceProvider(system, tag),
        () -> new ItemPopularityProjectionHandler(tag, system, repo));
  }
}
// #guideClusterSetup
