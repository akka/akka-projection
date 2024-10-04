/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

//#guideProjectionHandler
package docs.guide

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Success

import akka.Done
import akka.actor.typed.ActorSystem
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import org.slf4j.LoggerFactory

object ItemPopularityProjectionHandler {
  val LogInterval = 10
}

class ItemPopularityProjectionHandler(tag: String, system: ActorSystem[_], repo: ItemPopularityProjectionRepository)
    extends Handler[EventEnvelope[ShoppingCartEvents.Event]]() {
  import ShoppingCartEvents._

  private var logCounter: Int = 0
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext = system.executionContext

  /**
   * The Envelope handler to process events.
   */
  override def process(envelope: EventEnvelope[Event]): Future[Done] = {
    val processed = envelope.event match {
      case ItemAdded(_, itemId, quantity)                            => repo.update(itemId, quantity)
      case ItemQuantityAdjusted(_, itemId, newQuantity, oldQuantity) => repo.update(itemId, newQuantity - oldQuantity)
      case ItemRemoved(_, itemId, oldQuantity)                       => repo.update(itemId, 0 - oldQuantity)
      case _: CheckedOut                                             => Future.successful(Done) // skip
    }
    processed.onComplete {
      case Success(_) => logItemCount(envelope.event)
      case _          => ()
    }
    processed
  }

  /**
   * Log the popularity of the item in every `ItemEvent` every `LogInterval`.
   */
  private def logItemCount(event: Event): Unit = event match {
    case itemEvent: ItemEvent =>
      logCounter += 1
      if (logCounter == ItemPopularityProjectionHandler.LogInterval) {
        logCounter = 0
        val itemId = itemEvent.itemId
        repo.getItem(itemId).foreach {
          case Some(count) =>
            log.info("ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]", tag, itemId, count)
          case None =>
            log.info("ItemPopularityProjectionHandler({}) item popularity for '{}': [0]", tag, itemId)
        }
      }
    case _ => ()
  }

}
//#guideProjectionHandler
