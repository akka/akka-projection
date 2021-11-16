/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.eventsourced

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors

object EventSourcedDocExample {

  private val system = ActorSystem[Nothing](Behaviors.empty, "Example")

  object IllustrateEventsByTagSourceProvider {
    //#eventsByTagSourceProvider
    import akka.projection.eventsourced.EventEnvelope
    import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
    import akka.persistence.query.Offset
    import akka.projection.eventsourced.scaladsl.EventSourcedProvider
    import akka.projection.scaladsl.SourceProvider

    val sourceProvider: SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]] =
      EventSourcedProvider
        .eventsByTag[ShoppingCart.Event](system, readJournalPluginId = CassandraReadJournal.Identifier, tag = "carts-1")
    //#eventsByTagSourceProvider
  }

  object IllustrateEventsBySlicesSourceProvider {
    object R2dbcReadJournal {
      val Identifier = "akka.persistence.r2dbc.query"
    }

    //#eventsBySlicesSourceProvider
    import akka.persistence.query.typed.EventEnvelope
    import akka.persistence.query.Offset
    import akka.projection.eventsourced.scaladsl.EventSourcedProvider
    import akka.projection.scaladsl.SourceProvider

    // Slit the slices into 4 ranges
    val numberOfSliceRanges: Int = 4
    val sliceRanges = EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, numberOfSliceRanges)

    // Example of using the first slice range
    val minSlice: Int = sliceRanges.head.min
    val maxSlice: Int = sliceRanges.head.max
    val entityType: String = "ShoppingCart"

    val sourceProvider: SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]] =
      EventSourcedProvider
        .eventsBySlices[ShoppingCart.Event](
          system,
          readJournalPluginId = R2dbcReadJournal.Identifier,
          entityType,
          minSlice,
          maxSlice)
    //#eventsBySlicesSourceProvider
  }
}
