/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.state;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;

// #imports
import akka.persistence.query.DurableStateChange;
import akka.persistence.query.Offset;
import akka.persistence.state.DurableStateStoreRegistry;
import akka.persistence.state.javadsl.DurableStateSourceProvider;
import akka.projection.javadsl.SourceProvider;
// #imports

public interface DurableStateStoreDocExample {

  public static void illustrateSourceProvider() {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // TODO JdbcDurableStateStore.Identifier does not exist yet
    // #sourceProvider
    SourceProvider<Offset, DurableStateChange<Record>> sourceProvider =
        DurableStateSourceProvider.changesByTag(system, JdbcDurableStateStore.Identifier(), "records-1");
    // #sourceProvider

  }
}
