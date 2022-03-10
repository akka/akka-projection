/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.state;

import java.util.List;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;

// #changesBySlicesSourceProvider
import akka.japi.Pair;
import akka.persistence.query.DurableStateChange;
import akka.persistence.query.Offset;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.javadsl.SourceProvider;
import akka.projection.state.javadsl.DurableStateSourceProvider;

// #changesBySlicesSourceProvider

public interface DurableStateStoreBySlicesDocExample {
  public static class R2dbcDurableStateStore {
    public static String Identifier() {
      return "akka.persistence.r2dbc.query";
    }
  }

  public static void illustrateSourceProvider() {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #changesBySlicesSourceProvider
    // Slit the slices into 4 ranges
    int numberOfSliceRanges = 4;
    List<Pair<Integer, Integer>> sliceRanges =
        EventSourcedProvider.sliceRanges(
            system, R2dbcDurableStateStore.Identifier(), numberOfSliceRanges);

    // Example of using the first slice range
    int minSlice = sliceRanges.get(0).first();
    int maxSlice = sliceRanges.get(0).second();
    String entityType = "MyEntity";

    SourceProvider<Offset, DurableStateChange<AccountEntity.Account>> sourceProvider =
        DurableStateSourceProvider.changesBySlices(
            system, R2dbcDurableStateStore.Identifier(), entityType, minSlice, maxSlice);
    // #changesBySlicesSourceProvider
  }
}
