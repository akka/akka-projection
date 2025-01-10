/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.eventsourced;

import java.util.List;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;
// #eventsBySlicesSourceProvider
import akka.japi.Pair;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;

// #eventsBySlicesSourceProvider

public interface EventSourcedBySlicesDocExample {

  public static class R2dbcReadJournal {
    public static String Identifier() {
      return "akka.persistence.r2dbc.query";
    }
  }

  public static void illustrateEventsSlicesSourceProvider() {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #eventsBySlicesSourceProvider
    // Slit the slices into 4 ranges
    int numberOfSliceRanges = 4;
    List<Pair<Integer, Integer>> sliceRanges =
        EventSourcedProvider.sliceRanges(
            system, R2dbcReadJournal.Identifier(), numberOfSliceRanges);

    // Example of using the first slice range
    int minSlice = sliceRanges.get(0).first();
    int maxSlice = sliceRanges.get(0).second();
    String entityType = "MyEntity";

    SourceProvider<Offset, EventEnvelope<ShoppingCart.Event>> sourceProvider =
        EventSourcedProvider.eventsBySlices(
            system, R2dbcReadJournal.Identifier(), entityType, minSlice, maxSlice);
    // #eventsBySlicesSourceProvider

  }
}
