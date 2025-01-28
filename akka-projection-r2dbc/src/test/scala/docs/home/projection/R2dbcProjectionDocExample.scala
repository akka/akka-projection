/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.home.projection

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.persistence.query.Offset
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.serialization.jackson.CborSerializable
import org.slf4j.LoggerFactory
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import io.r2dbc.spi.ConnectionFactory

import akka.cluster.sharding.typed.ShardedDaemonProcessSettings

//#handler
//#grouped-handler
import akka.persistence.query.typed.EventEnvelope
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcSession

//#grouped-handler
//#handler
object R2dbcProjectionDocExample {

  object ShoppingCart {
    val EntityKey: EntityTypeKey[Command] = EntityTypeKey[Command]("ShoppingCart")

    sealed trait Command extends CborSerializable

    sealed trait Event extends CborSerializable {
      def cartId: String
    }

    final case class ItemAdded(cartId: String, itemId: String, quantity: Int) extends Event

    final case class ItemRemoved(cartId: String, itemId: String) extends Event

    final case class ItemQuantityAdjusted(cartId: String, itemId: String, newQuantity: Int) extends Event

    final case class CheckedOut(cartId: String, eventTime: Instant) extends Event
  }

  //#handler
  class ShoppingCartHandler()(implicit ec: ExecutionContext) extends R2dbcHandler[EventEnvelope[ShoppingCart.Event]] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(session: R2dbcSession, envelope: EventEnvelope[ShoppingCart.Event]): Future[Done] = {
      envelope.event match {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info(s"Shopping cart $cartId was checked out at $time")
          val stmt = session
            .createStatement("INSERT into order (id, time) VALUES ($1, $2)")
            .bind(0, cartId)
            .bind(1, time)
          session
            .updateOne(stmt)
            .map(_ => Done)

        case otherEvent =>
          logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
          Future.successful(Done)
      }
    }
  }
  //#handler

  //#grouped-handler

  import scala.collection.immutable

  class GroupedShoppingCartHandler()(implicit ec: ExecutionContext)
      extends R2dbcHandler[immutable.Seq[EventEnvelope[ShoppingCart.Event]]] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(
        session: R2dbcSession,
        envelopes: immutable.Seq[EventEnvelope[ShoppingCart.Event]]): Future[Done] = {

      // save all events in DB
      val stmts = envelopes
        .map(_.event)
        .collect {
          case ShoppingCart.CheckedOut(cartId, time) =>
            logger.info(s"Shopping cart $cartId was checked out at $time")

            session
              .createStatement("INSERT into order (id, time) VALUES ($1, $2)")
              .bind(0, cartId)
              .bind(1, time)

        }
        .toVector

      session.update(stmts).map(_ => Done)
    }
  }
  //#grouped-handler

  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "Example")
  implicit val ec: ExecutionContext = system.executionContext

  object IllustrateInit {
    // #initProjections

    import akka.persistence.query.typed.EventEnvelope
    import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
    import akka.projection.Projection
    import akka.projection.ProjectionBehavior
    import akka.projection.ProjectionId
    import akka.projection.eventsourced.scaladsl.EventSourcedProvider
    import akka.projection.r2dbc.scaladsl.R2dbcProjection
    import akka.projection.scaladsl.SourceProvider

    def initProjections(): Unit = {
      def sourceProvider(sliceRange: Range): SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]] =
        EventSourcedProvider
          .eventsBySlices[ShoppingCart.Event](
            system,
            readJournalPluginId = R2dbcReadJournal.Identifier,
            entityType,
            sliceRange.min,
            sliceRange.max)

      def projection(sliceRange: Range): Projection[EventEnvelope[ShoppingCart.Event]] = {
        val minSlice = sliceRange.min
        val maxSlice = sliceRange.max
        val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

        R2dbcProjection
          .exactlyOnce(
            projectionId,
            settings = None,
            sourceProvider(sliceRange),
            handler = () => new ShoppingCartHandler)
      }

      ShardedDaemonProcess(system).initWithContext(
        name = "ShoppingCartProjection",
        initialNumberOfInstances = 4,
        behaviorFactory = { daemonContext =>
          val sliceRanges =
            EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, daemonContext.totalProcesses)
          val sliceRange = sliceRanges(daemonContext.processNumber)
          ProjectionBehavior(projection(sliceRange))
        },
        ShardedDaemonProcessSettings(system),
        stopMessage = ProjectionBehavior.Stop)
    }
    // #initProjections
  }

  //#sourceProvider

  import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
  import akka.projection.eventsourced.scaladsl.EventSourcedProvider
  import akka.projection.scaladsl.SourceProvider

  // Slit the slices into 4 ranges
  val numberOfSliceRanges: Int = 4
  val sliceRanges = EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, numberOfSliceRanges)

  // Example of using the first slice range
  val minSlice: Int = sliceRanges.head.min
  val maxSlice: Int = sliceRanges.head.max
  val entityType: String = ShoppingCart.EntityKey.name

  val sourceProvider: SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]] =
    EventSourcedProvider
      .eventsBySlices[ShoppingCart.Event](
        system,
        readJournalPluginId = R2dbcReadJournal.Identifier,
        entityType,
        minSlice,
        maxSlice)
  //#sourceProvider

  object IllustrateExactlyOnce {
    //#exactlyOnce

    import akka.projection.ProjectionId
    import akka.projection.r2dbc.scaladsl.R2dbcProjection

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val projection =
      R2dbcProjection
        .exactlyOnce(projectionId, settings = None, sourceProvider, handler = () => new ShoppingCartHandler)
    //#exactlyOnce
  }

  object IllustrateAtLeastOnce {
    //#atLeastOnce

    import akka.projection.ProjectionId
    import akka.projection.r2dbc.scaladsl.R2dbcProjection

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val projection =
      R2dbcProjection
        .atLeastOnce(projectionId, settings = None, sourceProvider, handler = () => new ShoppingCartHandler)
        .withSaveOffset(afterEnvelopes = 100, afterDuration = 500.millis)
    //#atLeastOnce
  }

  object IllustrateGrouped {
    //#grouped

    import akka.projection.ProjectionId
    import akka.projection.r2dbc.scaladsl.R2dbcProjection

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val projection =
      R2dbcProjection
        .groupedWithin(projectionId, settings = None, sourceProvider, handler = () => new GroupedShoppingCartHandler)
        .withGroup(groupAfterEnvelopes = 20, groupAfterDuration = 500.millis)
    //#grouped
  }

  object IllustrateSettings {
    val config =
      """
    // #second-projection-config
    second-projection-r2dbc = ${akka.projection.r2dbc}
    second-projection-r2dbc {
      offset-store {
        # specific projection offset store properties here
      }
      use-connection-factory = "second-r2dbc.connection-factory"
    }
    // #second-projection-config
    
    // #second-projection-config-with-connection-factory
    second-projection-r2dbc = ${akka.projection.r2dbc}
    second-projection-r2dbc {
      connection-factory = ${akka.persistence.r2dbc.connection-factory}
      connection-factory {
        # specific connection properties for offset store and projection handler here 
      }
      
      offset-store {
        # specific projection offset store properties here
      }
      use-connection-factory = "second-projection-r2dbc.connection-factory"
    }
    // #second-projection-config-with-connection-factory
    """

    //#projectionSettings

    import akka.projection.ProjectionId
    import akka.projection.r2dbc.scaladsl.R2dbcProjection

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val settings = Some(R2dbcProjectionSettings(system.settings.config.getConfig("second-projection-r2dbc")))

    val projection =
      R2dbcProjection
        .atLeastOnce(projectionId, settings = None, sourceProvider, handler = () => new ShoppingCartHandler)
    //#projectionSettings
  }

  object CustomConnectionFactory {

    //#customConnectionFactory

    import akka.projection.ProjectionId
    import akka.projection.r2dbc.scaladsl.R2dbcProjection

    val connectionFactory: ConnectionFactory = ???

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val settings = Some(R2dbcProjectionSettings(system).withCustomConnectionFactory(connectionFactory))

    val projection =
      R2dbcProjection
        .atLeastOnce(projectionId, settings = None, sourceProvider, handler = () => new ShoppingCartHandler)

    //#customConnectionFactory
  }

}
