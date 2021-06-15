/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.state;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.Behaviors;

// #imports
import akka.persistence.jdbc.state.javadsl.JdbcDurableStateStore;
import akka.persistence.query.DurableStateChange;
import akka.persistence.query.Offset;
import akka.projection.state.javadsl.DurableStateSourceProvider;
import akka.projection.javadsl.SourceProvider;
// #imports

public interface DurableStateStoreDocExample {

  public static void illustrateSourceProvider() {

    ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "Example");

    // #sourceProvider
    SourceProvider<Offset, DurableStateChange<AccountEntity.Account>> sourceProvider =
        DurableStateSourceProvider.changesByTag(
            system, JdbcDurableStateStore.Identifier(), "bank-accounts-1");
    // #sourceProvider
  }
}
