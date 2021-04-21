/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.cassandra

import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.ProjectionContext
import akka.projection.eventsourced.EventEnvelope
import akka.stream.scaladsl.FlowWithContext
//#daemon-imports
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.projection.ProjectionBehavior

//#daemon-imports

//#source-provider-imports
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import docs.eventsourced.ShoppingCart

//#source-provider-imports

//#projection-imports
import akka.projection.ProjectionId
import akka.projection.cassandra.scaladsl.CassandraProjection

//#projection-imports

//#handler-imports
import scala.concurrent.duration._
import scala.concurrent.Future

import akka.Done
import akka.projection.scaladsl.Handler
import org.slf4j.LoggerFactory

//#handler-imports

object CassandraProjectionDocExample {

  private val system = ActorSystem[Nothing](Behaviors.empty, "Example")

  //#handler
  class ShoppingCartHandler extends Handler[EventEnvelope[ShoppingCart.Event]] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[ShoppingCart.Event]): Future[Done] = {
      envelope.event match {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info2("Shopping cart {} was checked out at {}", cartId, time)
          Future.successful(Done)

        case otherEvent =>
          logger.debug2("Shopping cart {} changed by {}", otherEvent.cartId, otherEvent)
          Future.successful(Done)
      }
    }
  }
  //#handler

  //#grouped-handler
  import scala.collection.immutable

  class GroupedShoppingCartHandler extends Handler[immutable.Seq[EventEnvelope[ShoppingCart.Event]]] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(envelopes: immutable.Seq[EventEnvelope[ShoppingCart.Event]]): Future[Done] = {
      envelopes.map(_.event).foreach {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info2("Shopping cart {} was checked out at {}", cartId, time)

        case otherEvent =>
          logger.debug2("Shopping cart {} changed by {}", otherEvent.cartId, otherEvent)
      }
      Future.successful(Done)
    }
  }
  //#grouped-handler

  //#sourceProvider
  val sourceProvider =
    EventSourcedProvider
      .eventsByTag[ShoppingCart.Event](system, readJournalPluginId = CassandraReadJournal.Identifier, tag = "carts-1")
  //#sourceProvider

  object IllustrateAtLeastOnce {
    //#atLeastOnce
    val projection =
      CassandraProjection
        .atLeastOnce(
          projectionId = ProjectionId("shopping-carts", "carts-1"),
          sourceProvider,
          handler = () => new ShoppingCartHandler)
        .withSaveOffset(afterEnvelopes = 100, afterDuration = 500.millis)
    //#atLeastOnce
  }

  object IllustrateAtMostOnce {
    //#atMostOnce
    val projection =
      CassandraProjection.atMostOnce(
        projectionId = ProjectionId("shopping-carts", "carts-1"),
        sourceProvider,
        handler = () => new ShoppingCartHandler)
    //#atMostOnce
  }

  object IllustrateGrouped {
    //#grouped
    val projection =
      CassandraProjection
        .groupedWithin(
          projectionId = ProjectionId("shopping-carts", "carts-1"),
          sourceProvider,
          handler = () => new GroupedShoppingCartHandler)
        .withGroup(groupAfterEnvelopes = 20, groupAfterDuration = 500.millis)
    //#grouped
  }

  object IllustrateAtLeastOnceFlow {
    //#atLeastOnceFlow
    val logger = LoggerFactory.getLogger(getClass)

    val flow = FlowWithContext[EventEnvelope[ShoppingCart.Event], ProjectionContext]
      .map(envelope => envelope.event)
      .map {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info2("Shopping cart {} was checked out at {}", cartId, time)
          Done

        case otherEvent =>
          logger.debug2("Shopping cart {} changed by {}", otherEvent.cartId, otherEvent)
          Done
      }

    val projection =
      CassandraProjection
        .atLeastOnceFlow(projectionId = ProjectionId("shopping-carts", "carts-1"), sourceProvider, handler = flow)
        .withSaveOffset(afterEnvelopes = 100, afterDuration = 500.millis)
    //#atLeastOnceFlow
  }

  object IllustrateRecoveryStrategy {
    //#withRecoveryStrategy
    import akka.projection.HandlerRecoveryStrategy

    val projection =
      CassandraProjection
        .atLeastOnce(
          projectionId = ProjectionId("shopping-carts", "carts-1"),
          sourceProvider,
          handler = () => new ShoppingCartHandler)
        .withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(retries = 10, delay = 1.second))
    //#withRecoveryStrategy
  }

  object IllustrateRestart {
    //#withRestartBackoff
    val projection =
      CassandraProjection
        .atLeastOnce(
          projectionId = ProjectionId("shopping-carts", "carts-1"),
          sourceProvider,
          handler = () => new ShoppingCartHandler)
        .withRestartBackoff(minBackoff = 200.millis, maxBackoff = 5.seconds, randomFactor = 0.1)
    //#withRestartBackoff
  }

  object IllustrateRunningWithShardedDaemon {

    //#running-source-provider
    def sourceProvider(tag: String) =
      EventSourcedProvider
        .eventsByTag[ShoppingCart.Event](
          system = system,
          readJournalPluginId = CassandraReadJournal.Identifier,
          tag = tag)
    //#running-source-provider

    //#running-projection
    def projection(tag: String) =
      CassandraProjection
        .atLeastOnce(
          projectionId = ProjectionId("shopping-carts", tag),
          sourceProvider(tag),
          handler = () => new ShoppingCartHandler)
        .withSaveOffset(100, 500.millis)
    //#running-projection

    //#running-with-daemon-process
    ShardedDaemonProcess(system).init[ProjectionBehavior.Command](
      name = "shopping-carts",
      numberOfInstances = ShoppingCart.tags.size,
      behaviorFactory = (i: Int) => ProjectionBehavior(projection(ShoppingCart.tags(i))),
      stopMessage = ProjectionBehavior.Stop)
    //#running-with-daemon-process
  }

  object IllustrateRunningWithActor {

    Behaviors.setup[String] { context =>
      //#running-with-actor
      def sourceProvider(tag: String) =
        EventSourcedProvider
          .eventsByTag[ShoppingCart.Event](
            system = system,
            readJournalPluginId = CassandraReadJournal.Identifier,
            tag = tag)

      def projection(tag: String) =
        CassandraProjection
          .atLeastOnce(
            projectionId = ProjectionId("shopping-carts", tag),
            sourceProvider(tag),
            handler = () => new ShoppingCartHandler)

      val projection1 = projection("carts-1")

      context.spawn(ProjectionBehavior(projection1), projection1.projectionId.id)
      //#running-with-actor

      Behaviors.empty
    }
  }

  object IllustrateRunningWithSingleton {

    //#running-with-singleton
    import akka.cluster.typed.ClusterSingleton
    import akka.cluster.typed.SingletonActor

    def sourceProvider(tag: String) =
      EventSourcedProvider
        .eventsByTag[ShoppingCart.Event](
          system = system,
          readJournalPluginId = CassandraReadJournal.Identifier,
          tag = tag)

    def projection(tag: String) =
      CassandraProjection
        .atLeastOnce(
          projectionId = ProjectionId("shopping-carts", tag),
          sourceProvider(tag),
          handler = () => new ShoppingCartHandler)

    val projection1 = projection("carts-1")

    ClusterSingleton(system).init(SingletonActor(ProjectionBehavior(projection1), projection1.projectionId.id))
    //#running-with-singleton

  }

  object IllustrateProjectionSettings {

    //#projection-settings
    val projection =
      CassandraProjection
        .atLeastOnce(
          projectionId = ProjectionId("shopping-carts", "carts-1"),
          sourceProvider,
          handler = () => new ShoppingCartHandler)
        .withRestartBackoff(minBackoff = 10.seconds, maxBackoff = 60.seconds, randomFactor = 0.5)
        .withSaveOffset(100, 500.millis)
    //#projection-settings

  }

  object IllustrateGetOffset {
    //#get-offset
    import akka.projection.scaladsl.ProjectionManagement
    import akka.persistence.query.Offset
    import akka.projection.ProjectionId

    val projectionId = ProjectionId("shopping-carts", "carts-1")
    val currentOffset: Future[Option[Offset]] = ProjectionManagement(system).getOffset[Offset](projectionId)
    //#get-offset
  }

  object IllustrateClearOffset {
    import akka.projection.scaladsl.ProjectionManagement
    //#clear-offset
    val projectionId = ProjectionId("shopping-carts", "carts-1")
    val done: Future[Done] = ProjectionManagement(system).clearOffset(projectionId)
    //#clear-offset
  }

  object IllustrateUpdateOffset {
    import akka.projection.scaladsl.ProjectionManagement
    import system.executionContext
    //#update-offset
    import akka.persistence.query.Sequence
    val projectionId = ProjectionId("shopping-carts", "carts-1")
    val currentOffset: Future[Option[Sequence]] = ProjectionManagement(system).getOffset[Sequence](projectionId)
    currentOffset.foreach {
      case Some(s) => ProjectionManagement(system).updateOffset[Sequence](projectionId, Sequence(s.value + 1))
      case None    => // already removed
    }
    //#update-offset
  }

  object IllustratePauseResume {
    import akka.projection.scaladsl.ProjectionManagement
    import system.executionContext
    def someDataMigration() = Future.successful(Done)
    //#pause-resume
    import akka.projection.scaladsl.ProjectionManagement
    import akka.projection.ProjectionId

    val projectionId = ProjectionId("shopping-carts", "carts-1")
    val mgmt = ProjectionManagement(system)
    val done = for {
      _: Future[Done] <- mgmt.pauseProjection(projectionId)
      _ <- someDataMigration()
      _: Future[Done] <- mgmt.resumeProjection(projectionId)
    } yield Done
    //#pause-resume
  }

  object IllustrateIsPaused {
    import akka.projection.scaladsl.ProjectionManagement
    import akka.projection.ProjectionId
    //#is-paused
    val projectionId = ProjectionId("shopping-carts", "carts-1")
    val paused: Future[Boolean] = ProjectionManagement(system).isProjectionPaused(projectionId)
    //#is-paused
  }

}
