/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package projection.docs.scaladsl

import java.time.Instant

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.projection.scaladsl.SourceProvider
import akka.serialization.jackson.CborSerializable
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.Futures
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

object ProjectionDocExample {
  val config: Config = ConfigFactory.load(ConfigFactory.parseString("""
    //#local-mode
    akka.persistence.dynamodb {
      client.local.enabled = true
    }
    //#local-mode
    """))

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

  object HandlerExample {
    //#handler
    import akka.Done
    import akka.persistence.query.typed.EventEnvelope
    import akka.projection.scaladsl.Handler
    import software.amazon.awssdk.services.dynamodb.model.AttributeValue
    import software.amazon.awssdk.services.dynamodb.model.PutItemRequest

    import scala.concurrent.Future
    import scala.jdk.CollectionConverters._
    import scala.jdk.FutureConverters._

    class ShoppingCartHandler(client: DynamoDbAsyncClient)(implicit ec: ExecutionContext)
        extends Handler[EventEnvelope[ShoppingCart.Event]] {
      private val logger = LoggerFactory.getLogger(getClass)

      override def process(envelope: EventEnvelope[ShoppingCart.Event]): Future[Done] = {
        envelope.event match {
          case ShoppingCart.CheckedOut(cartId, time) =>
            logger.info(s"Shopping cart $cartId was checked out at $time")

            val attributes = Map(
              "id" -> AttributeValue.fromS(cartId),
              "time" -> AttributeValue.fromN(time.toEpochMilli.toString)).asJava

            client
              .putItem(
                PutItemRequest.builder
                  .tableName("orders")
                  .item(attributes)
                  .build())
              .asScala
              .map(_ => Done)

          case otherEvent =>
            logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
            Future.successful(Done)
        }
      }
    }
    //#handler
  }

  object TransactHandlerExample {
    //#transact-handler
    import akka.persistence.query.typed.EventEnvelope
    import akka.projection.dynamodb.scaladsl.DynamoDBTransactHandler
    import software.amazon.awssdk.services.dynamodb.model.AttributeValue
    import software.amazon.awssdk.services.dynamodb.model.Put
    import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem

    import scala.concurrent.Future
    import scala.jdk.CollectionConverters._

    class ShoppingCartTransactHandler extends DynamoDBTransactHandler[EventEnvelope[ShoppingCart.Event]] {
      private val logger = LoggerFactory.getLogger(getClass)

      override def process(envelope: EventEnvelope[ShoppingCart.Event]): Future[Iterable[TransactWriteItem]] = {
        envelope.event match {
          case ShoppingCart.CheckedOut(cartId, time) =>
            logger.info(s"Shopping cart $cartId was checked out at $time")

            val attributes = Map(
              "id" -> AttributeValue.fromS(cartId),
              "time" -> AttributeValue.fromN(time.toEpochMilli.toString)).asJava

            Future.successful(
              Seq(
                TransactWriteItem.builder
                  .put(
                    Put.builder
                      .tableName("orders")
                      .item(attributes)
                      .build())
                  .build()))

          case otherEvent =>
            logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
            Future.successful(Seq.empty)
        }
      }
    }
    //#transact-handler
  }

  object GroupedHandlerExample {
    //#grouped-handler
    import akka.Done
    import akka.persistence.query.typed.EventEnvelope
    import akka.projection.scaladsl.Handler
    import software.amazon.awssdk.services.dynamodb.model.AttributeValue
    import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
    import software.amazon.awssdk.services.dynamodb.model.PutRequest
    import software.amazon.awssdk.services.dynamodb.model.WriteRequest

    import scala.concurrent.Future
    import scala.jdk.CollectionConverters._
    import scala.jdk.FutureConverters._

    class GroupedShoppingCartHandler(client: DynamoDbAsyncClient)(implicit ec: ExecutionContext)
        extends Handler[Seq[EventEnvelope[ShoppingCart.Event]]] {
      private val logger = LoggerFactory.getLogger(getClass)

      override def process(envelopes: Seq[EventEnvelope[ShoppingCart.Event]]): Future[Done] = {
        val items = envelopes.flatMap { envelope =>
          envelope.event match {
            case ShoppingCart.CheckedOut(cartId, time) =>
              logger.info(s"Shopping cart $cartId was checked out at $time")

              val attributes =
                Map("id" -> AttributeValue.fromS(cartId), "time" -> AttributeValue.fromN(time.toEpochMilli.toString)).asJava

              Some(
                WriteRequest.builder
                  .putRequest(
                    PutRequest.builder
                      .item(attributes)
                      .build())
                  .build())

            case otherEvent =>
              logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
              None
          }
        }.asJava

        client
          .batchWriteItem(
            BatchWriteItemRequest.builder
              .requestItems(Map("orders" -> items).asJava)
              .build())
          .asScala
          .map(_ => Done)
      }
    }
    //#grouped-handler
  }

  object GroupedTransactHandlerExample {
    //#grouped-transact-handler
    import akka.persistence.query.typed.EventEnvelope
    import akka.projection.dynamodb.scaladsl.DynamoDBTransactHandler
    import software.amazon.awssdk.services.dynamodb.model.AttributeValue
    import software.amazon.awssdk.services.dynamodb.model.Put
    import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem

    import scala.concurrent.Future
    import scala.jdk.CollectionConverters._

    class GroupedShoppingCartTransactHandler extends DynamoDBTransactHandler[Seq[EventEnvelope[ShoppingCart.Event]]] {
      private val logger = LoggerFactory.getLogger(getClass)

      override def process(envelopes: Seq[EventEnvelope[ShoppingCart.Event]]): Future[Iterable[TransactWriteItem]] = {
        val items = envelopes.flatMap { envelope =>
          envelope.event match {
            case ShoppingCart.CheckedOut(cartId, time) =>
              logger.info(s"Shopping cart $cartId was checked out at $time")

              val attributes = Map(
                "id" -> AttributeValue.fromS(cartId),
                "time" -> AttributeValue.fromN(time.toEpochMilli.toString)).asJava

              Seq(
                TransactWriteItem.builder
                  .put(
                    Put.builder
                      .tableName("orders")
                      .item(attributes)
                      .build())
                  .build())

            case otherEvent =>
              logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
              Seq.empty
          }
        }
        Future.successful(items)
      }
    }
    //#grouped-transact-handler
  }

  def initExample(implicit system: ActorSystem[_]): Unit = {
    import TransactHandlerExample.ShoppingCartTransactHandler

    //#init-projections
    import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
    import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
    import akka.persistence.dynamodb.query.scaladsl.DynamoDBReadJournal
    import akka.persistence.query.typed.EventEnvelope
    import akka.projection.Projection
    import akka.projection.ProjectionBehavior
    import akka.projection.ProjectionId
    import akka.projection.dynamodb.scaladsl.DynamoDBProjection
    import akka.projection.eventsourced.scaladsl.EventSourcedProvider
    import akka.projection.scaladsl.SourceProvider

    def sourceProvider(sliceRange: Range): SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]] =
      EventSourcedProvider.eventsBySlices[ShoppingCart.Event](
        system,
        readJournalPluginId = DynamoDBReadJournal.Identifier,
        ShoppingCart.EntityKey.name,
        sliceRange.min,
        sliceRange.max)

    def projection(sliceRange: Range): Projection[EventEnvelope[ShoppingCart.Event]] = {
      val minSlice = sliceRange.min
      val maxSlice = sliceRange.max
      val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

      DynamoDBProjection.exactlyOnce(
        projectionId,
        settings = None,
        sourceProvider(sliceRange),
        handler = () => new ShoppingCartTransactHandler)
    }

    ShardedDaemonProcess(system).initWithContext(
      name = "ShoppingCartProjection",
      initialNumberOfInstances = 4,
      behaviorFactory = { daemonContext =>
        val sliceRanges =
          EventSourcedProvider.sliceRanges(system, DynamoDBReadJournal.Identifier, daemonContext.totalProcesses)
        val sliceRange = sliceRanges(daemonContext.processNumber)
        ProjectionBehavior(projection(sliceRange))
      },
      ShardedDaemonProcessSettings(system),
      stopMessage = ProjectionBehavior.Stop)
    //#init-projections
  }

  def exactlyOnceExample(
      sourceProvider: SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]],
      minSlice: Int,
      maxSlice: Int)(implicit system: ActorSystem[_]): Unit = {
    import TransactHandlerExample.ShoppingCartTransactHandler

    //#exactly-once
    import akka.projection.ProjectionId
    import akka.projection.dynamodb.scaladsl.DynamoDBProjection

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val projection = DynamoDBProjection.exactlyOnce(
      projectionId,
      settings = None,
      sourceProvider,
      handler = () => new ShoppingCartTransactHandler)
    //#exactly-once

    val _ = projection
  }

  def atLeastOnceExample(
      sourceProvider: akka.projection.scaladsl.SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]],
      minSlice: Int,
      maxSlice: Int,
      client: DynamoDbAsyncClient)(implicit system: ActorSystem[_], ec: ExecutionContext): Unit = {
    import HandlerExample.ShoppingCartHandler

    //#at-least-once
    import akka.projection.ProjectionId
    import akka.projection.dynamodb.scaladsl.DynamoDBProjection

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val projection =
      DynamoDBProjection
        .atLeastOnce(projectionId, settings = None, sourceProvider, handler = () => new ShoppingCartHandler(client))
        .withSaveOffset(afterEnvelopes = 100, afterDuration = 500.millis)
    //#at-least-once

    val _ = projection
  }

  def exactlyOnceGroupedWithinExample(
      sourceProvider: akka.projection.scaladsl.SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]],
      minSlice: Int,
      maxSlice: Int)(implicit system: ActorSystem[_]): Unit = {
    import GroupedTransactHandlerExample.GroupedShoppingCartTransactHandler

    //#exactly-once-grouped-within
    import akka.projection.ProjectionId
    import akka.projection.dynamodb.scaladsl.DynamoDBProjection

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val projection =
      DynamoDBProjection
        .exactlyOnceGroupedWithin(
          projectionId,
          settings = None,
          sourceProvider,
          handler = () => new GroupedShoppingCartTransactHandler)
        .withGroup(groupAfterEnvelopes = 20, groupAfterDuration = 500.millis)
    //#exactly-once-grouped-within

    val _ = projection
  }

  def atLeastOnceGroupedWithinExample(
      sourceProvider: akka.projection.scaladsl.SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]],
      minSlice: Int,
      maxSlice: Int,
      client: DynamoDbAsyncClient)(implicit system: ActorSystem[_], ec: ExecutionContext): Unit = {
    import GroupedHandlerExample.GroupedShoppingCartHandler

    //#at-least-once-grouped-within
    import akka.projection.ProjectionId
    import akka.projection.dynamodb.scaladsl.DynamoDBProjection

    val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

    val projection =
      DynamoDBProjection
        .atLeastOnceGroupedWithin(
          projectionId,
          settings = None,
          sourceProvider,
          handler = () => new GroupedShoppingCartHandler(client))
        .withGroup(groupAfterEnvelopes = 20, groupAfterDuration = 500.millis)
    //#at-least-once-grouped-within

    val _ = projection
  }

  object MultiPluginExample {
    val config =
      s"""
      // #second-projection-config
      second-projection-dynamodb = $${akka.projection.dynamodb}
      second-projection-dynamodb {
        offset-store {
          # specific projection offset store settings here
        }
        use-client = "second-dynamodb.client"
      }
      // #second-projection-config

      // #second-projection-config-with-client
      second-projection-dynamodb = $${akka.projection.dynamodb}
      second-projection-dynamodb {
        offset-store {
          # specific projection offset store settings here
        }
        client = $${akka.persistence.dynamodb.client}
        client {
          # specific client settings for offset store and projection handler here
        }
        use-client = "second-projection-dynamodb.client"
      }
      // #second-projection-config-with-client
      """

    def projectionWithSecondPlugin(
        sourceProvider: akka.projection.scaladsl.SourceProvider[Offset, EventEnvelope[ShoppingCart.Event]],
        minSlice: Int,
        maxSlice: Int,
        client: DynamoDbAsyncClient)(implicit system: ActorSystem[_], ec: ExecutionContext): Unit = {
      import HandlerExample.ShoppingCartHandler

      //#projection-settings
      import akka.projection.ProjectionId
      import akka.projection.dynamodb.DynamoDBProjectionSettings
      import akka.projection.dynamodb.scaladsl.DynamoDBProjection

      val projectionId = ProjectionId("ShoppingCarts", s"carts-$minSlice-$maxSlice")

      val settings = Some(DynamoDBProjectionSettings(system.settings.config.getConfig("second-projection-dynamodb")))

      val projection =
        DynamoDBProjection.atLeastOnce(
          projectionId,
          settings,
          sourceProvider,
          handler = () => new ShoppingCartHandler(client))
      //#projection-settings

      val _ = projection
    }
  }
}

class ProjectionDocExample
    extends ScalaTestWithActorTestKit(ProjectionDocExample.config)
    with AnyWordSpecLike
    with Futures {

  "Projection docs" should {

    "have example of creating tables locally (Scala)" in {
      //#create-tables
      import akka.persistence.dynamodb.util.ClientProvider
      import akka.projection.dynamodb.DynamoDBProjectionSettings
      import akka.projection.dynamodb.scaladsl.CreateTables
      import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

      val dynamoDBConfigPath = "akka.projection.dynamodb"

      val settings: DynamoDBProjectionSettings =
        DynamoDBProjectionSettings(system.settings.config.getConfig(dynamoDBConfigPath))

      val client: DynamoDbAsyncClient = ClientProvider(system).clientFor(settings.useClient)

      // create timestamp offset table, synchronously
      Await.result(
        CreateTables.createTimestampOffsetStoreTable(system, settings, client, deleteIfExists = true),
        10.seconds)
      //#create-tables
    }

    "have example of creating tables locally (Java)" in {
      projection.docs.javadsl.ProjectionDocExample.createTables(system)
    }

  }
}
