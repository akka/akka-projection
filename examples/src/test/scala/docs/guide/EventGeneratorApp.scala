/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.guide

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
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
  val config = ConfigFactory.parseResources("guide-shopping-cart-app.conf")

  ActorSystem(Behaviors.setup[Command] {
    ctx =>
      ctx.self ! Start

      Behaviors.receiveMessage {
        case Start =>
          val persistence: ActorRef[ShoppingCartEvents.Event] = ctx.spawn(eventSink("all-shopping-carts"), "persister")

          val maxQuantity = 3
          val maxItems = 3
          val products = List("cat t-shirt", "akka t-shirt", "skis", "bowling shoes")

          implicit val classic: akka.actor.ActorSystem = ctx.system.toClassic

          val _: Future[Done] =
            Source
              .tick(1.second, 1.second, "checkout")
              .merge(Source.tick(10.seconds, 10.seconds, "next-hour"))
              .statefulMapConcat(() => {
                var time = Instant.now()

                {
                  case "checkout" =>
                    val cartId = java.util.UUID.randomUUID().toString.take(5)
                    val items = Random.nextInt(maxItems)
                    val itemsAdded = for (_ <- 0 to items) yield {
                      val product = products(Random.nextInt(products.size))
                      val quantity = Random.nextInt(maxQuantity)
                      ShoppingCartEvents.ItemAdded(cartId, product, quantity)
                    }
                    itemsAdded :+ ShoppingCartEvents.CheckedOut(cartId, time)
                  case "next-hour" =>
                    time = time.plus(java.time.Duration.ofHours(1))
                    Nil
                }
              })
              .map(event => persistence ! event)
              .runWith(Sink.ignore)

          Behaviors.empty
      }
  }, "EventGenerator", config)

  // FIXME: create actor per cart id so shopping cart events for the same cart get processed in order when query distributed
  def eventSink(persistenceId: String): Behavior[ShoppingCartEvents.Event] =
    Behaviors.setup { ctx =>
      EventSourcedBehavior[ShoppingCartEvents.Event, ShoppingCartEvents.Event, List[Any]](
        persistenceId = PersistenceId.ofUniqueId(persistenceId),
        Nil,
        (_, event) => {
          ctx.log.info("persisting event {}", event)
          Effect.persist(event)
        },
        (_, _) => Nil).withTagger(_ => Set(shoppingCartsTag))
    }
}
