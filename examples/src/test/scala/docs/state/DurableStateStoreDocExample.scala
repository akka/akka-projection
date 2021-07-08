/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.eventsourced

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors

//#imports
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.DurableStateSourceProvider
import akka.projection.scaladsl.SourceProvider
//#imports

object DurableStateStoreDocExample {

  private val system = ActorSystem[Nothing](Behaviors.empty, "Example")

  object IllustrateSourceProvider {
    //#sourceProvider
    // TODO JdbcDurableStateStore.Identifier does not exist yet
    val sourceProvider: SourceProvider[Offset, DurableStateChange[Record]] =
      DurableStateSourceProvider.changesByTag[Record](system, JdbcDurableStateStore.Identifier, tag = "records-1")
    //#sourceProvider
  }
}
