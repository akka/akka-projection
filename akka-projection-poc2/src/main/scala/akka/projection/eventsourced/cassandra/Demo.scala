/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.cassandra

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.event.Logging
import akka.persistence.cassandra.ConfigSessionProvider
import akka.persistence.cassandra.session.CassandraSessionSettings
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.EventSourcedProjection
import akka.projection.scaladsl.GroupedEventsHandler
import akka.projection.scaladsl.SingleEventHandler

object Demo {

  object ShoppingCart {
    trait Event
  }

  object ShoppingCartProjection {
    def start(system: ActorSystem[_]): Unit = {

      val eventProcessorId = "ShoppingCartProcessor"
      val tag = "CartSlice-1"

      val eventHandler: EventEnvelope[ShoppingCart.Event] => Future[Done] = { eventEnvelope =>
        // do something
        Future.successful(Done)
      }
      val projectionHandler = new SingleEventHandler[EventEnvelope[ShoppingCart.Event]](eventHandler)

      val offsetStore = new CassandraOffsetStore(session(system), eventProcessorId, tag)

      implicit val ec = system.executionContext
      val projection = EventSourcedProjection.atLeastOnce(
        system,
        eventProcessorId,
        tag,
        projectionHandler,
        offsetStore,
        saveOffsetAfterNumberOfEvents = 100,
        saveOffsetAfterDuration = 250.millis)

      projection.start()
    }

  }

  object ShoppingCartProjectionWithGroupedEvents {
    def start(system: ActorSystem[_]): Unit = {

      val eventProcessorId = "ShoppingCartProcessor"
      val tag = "CartSlice-1"

      val eventHandler: immutable.Seq[EventEnvelope[ShoppingCart.Event]] => Future[Done] = { eventEnvelopes =>
        // do something
        Future.successful(Done)
      }
      val projectionHandler = new GroupedEventsHandler[EventEnvelope[ShoppingCart.Event]](10, 100.millis, eventHandler)

      val offsetStore = new CassandraOffsetStore(session(system), eventProcessorId, tag)

      implicit val ec = system.executionContext
      val projection = EventSourcedProjection.atLeastOnce(
        system,
        eventProcessorId,
        tag,
        projectionHandler,
        offsetStore,
        saveOffsetAfterNumberOfEvents = 100,
        saveOffsetAfterDuration = 250.millis)

      projection.start()
    }

  }

  object Guardian {
    def apply(): Behavior[Nothing] = {
      Behaviors.setup[Nothing] { context =>
        ShoppingCartProjection.start(context.system)
        Behaviors.empty
      }
    }
  }

  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](Guardian(), "Demo")
  }

  private def session(systemProvider: ClassicActorSystemProvider): CassandraSession = {
    // FIXME this will change with APC 1.0 / Alpakka Cassandra
    val system = systemProvider.classicSystem
    val sessionConfig = system.settings.config.getConfig("cassandra-journal")
    new CassandraSession(
      system,
      new ConfigSessionProvider(system, sessionConfig),
      CassandraSessionSettings(sessionConfig),
      system.dispatcher,
      Logging(system, getClass),
      metricsCategory = "sample",
      init = _ => Future.successful(Done))
  }

}
