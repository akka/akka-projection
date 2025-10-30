/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package jdocs.eventsourced;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;

// #eventsByTagSourceProvider
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.Offset;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;

// #eventsByTagSourceProvider

public interface EventSourcedDocExample {

  public static void illustrateEventsByTagSourceProvider() {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #eventsByTagSourceProvider
    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");
    // #eventsByTagSourceProvider

  }
}
