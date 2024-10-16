/*
 * Copyright (C) 2020-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.classic

import akka.projection.Projection
import akka.projection.ProjectionBehavior

object ClassicDocExample {

  object IllustrateSystem {
    // #system
    import akka.actor.typed.scaladsl.adapter._

    private val system = akka.actor.ActorSystem("Example")
    private val typedSystem: akka.actor.typed.ActorSystem[_] = system.toTyped
    // #system

    typedSystem.terminate() // avoid unused warning
  }

  object IllustrateSpawn {
    private val system = akka.actor.ActorSystem("Example")
    private val projection: Projection[Any] = null

    //#spawn
    import akka.actor.typed.scaladsl.adapter._

    system.spawn(ProjectionBehavior(projection), "theProjection")
    //#spawn
  }
}
