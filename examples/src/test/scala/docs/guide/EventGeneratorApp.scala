/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.guide

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.cluster.typed.SelfUp
import akka.cluster.typed.Subscribe
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.typesafe.config.ConfigFactory

object EventGeneratorApp extends App {
  sealed trait Command
  final case object Start extends Command

  val shoppingCartsTag = "shopping-cart"
  val config = ConfigFactory
    .parseString("""
      |akka.actor.provider = "cluster"
      |""".stripMargin)
    .withFallback(ConfigFactory.parseResources("guide-shopping-cart-app.conf"))
  val EntityKey: EntityTypeKey[ShoppingCartEvents.Event] =
    EntityTypeKey[ShoppingCartEvents.Event]("shopping-cart-event")

  ActorSystem(Behaviors.setup[Command] {
    ctx =>
      val system = ctx.system
      val cluster = Cluster(system)
      val upAdapter = ctx.messageAdapter[SelfUp](_ => Start)
      cluster.subscriptions ! Subscribe(upAdapter, classOf[SelfUp])
      cluster.manager ! Join(cluster.selfMember.address)
      val sharding = ClusterSharding(system)
      val _ = sharding.init(Entity(EntityKey)(entityCtx => cartBehaviour(entityCtx.entityId)))

      Behaviors.receiveMessage {
        case Start =>
          val maxQuantity = 3
          val maxItems = 3
          val products = List("cat t-shirt", "akka t-shirt", "skis", "bowling shoes")

          implicit val classic: akka.actor.ActorSystem = system.toClassic

          val _: Future[Done] =
            Source
              .tick(1.second, 1.second, "checkout")
              .mapConcat {
                case "checkout" =>
                  val cartId = java.util.UUID.randomUUID().toString.take(5)
                  val items = Random.nextInt(maxItems)
                  val itemsAdded = for (_ <- 0 to items) yield {
                    val product = products(Random.nextInt(products.size))
                    val quantity = Random.nextInt(maxQuantity)
                    ShoppingCartEvents.ItemAdded(cartId, product, quantity)
                  }
                  itemsAdded :+ ShoppingCartEvents.CheckedOut(cartId, Instant.now())
              }
              .map(event => sharding.entityRefFor(EntityKey, event.cartId).ref ! event)
              .runWith(Sink.ignore)

          Behaviors.empty
      }
  }, "EventGenerator", config)

  def cartBehaviour(persistenceId: String): Behavior[ShoppingCartEvents.Event] =
    Behaviors.setup { ctx =>
      EventSourcedBehavior[ShoppingCartEvents.Event, ShoppingCartEvents.Event, List[Any]](
        persistenceId = PersistenceId.ofUniqueId(persistenceId),
        Nil,
        (_, event) => {
          ctx.log.info("id [{}], persisting event {}", persistenceId.toString, event.toString)
          Effect.persist(event)
        },
        (_, _) => Nil).withTagger(_ => Set(shoppingCartsTag))
    }
}
