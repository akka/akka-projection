/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.eventsourced;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
import akka.persistence.cassandra.query.javadsl.CassandraReadJournal;
import akka.persistence.query.Offset;
import akka.projection.eventsourced.EventEnvelope;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;

//#imports

//#imports

public interface EventSourcedDocExample {

  public static void illustrateSourceProvider() {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    //#sourceProvider
    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
      EventSourcedProvider.eventsByTag(system, CassandraReadJournal.Identifier(), "carts-1");
    //#sourceProvider

  }
}
