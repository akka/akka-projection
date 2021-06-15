/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.state

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors

//#imports
import akka.persistence.jdbc.state.scaladsl.JdbcDurableStateStore
import akka.persistence.query.DurableStateChange
import akka.persistence.query.Offset
import akka.projection.state.scaladsl.DurableStateSourceProvider
import akka.projection.scaladsl.SourceProvider
//#imports

object DurableStateStoreDocExample {

  private val system = ActorSystem[Nothing](Behaviors.empty, "Example")

  object IllustrateSourceProvider {
    //#sourceProvider
    val sourceProvider: SourceProvider[Offset, DurableStateChange[AccountEntity.Account]] =
      DurableStateSourceProvider
        .changesByTag[AccountEntity.Account](system, JdbcDurableStateStore.Identifier, "bank-accounts-1")
    //#sourceProvider
  }
}
