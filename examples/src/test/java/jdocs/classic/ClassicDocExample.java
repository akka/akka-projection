/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package jdocs.classic;

import akka.actor.typed.ActorSystem;

import akka.projection.Projection;
import akka.projection.ProjectionBehavior;

// #import-adapter
import akka.actor.typed.javadsl.Adapter;

// #import-adapter

public interface ClassicDocExample {

  public static void illustrateSystem() {

    // #system
    akka.actor.ActorSystem system = akka.actor.ActorSystem.create("Example");
    ActorSystem<Void> typedSystem = Adapter.toTyped(system);
    // #system
  }

  public static void illustrateSpawn() {

    akka.actor.ActorSystem system = akka.actor.ActorSystem.create("Example");
    Projection<?> projection = null;

    // #spawn
    Adapter.spawn(system, ProjectionBehavior.create(projection), "theProjection");
    // #spawn
  }
}
