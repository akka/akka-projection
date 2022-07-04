/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package shopping.order

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import org.slf4j.LoggerFactory
import shopping.cart.proto.ItemAdded

object ShoppingCartConsumer {

  class EventHandler extends Handler[EventEnvelope[ItemAdded]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[ItemAdded]): Future[Done] = {
      log.info("Consumed event: {}", envelope)
      Future.successful(Done)
    }
  }

  def init(system: ActorSystem[_]): Unit = {
    implicit val sys = system
    val numberOfProjectionInstances = 1
    val projectionName = s"cart-events"
    val sliceRanges =
      Persistence(system).sliceRanges(numberOfProjectionInstances)
    val entityType = "ShoppingCart"

    ShardedDaemonProcess(system).init(
      projectionName,
      numberOfProjectionInstances,
      { idx =>
        val sliceRange = sliceRanges(idx)
        val projectionKey = s"$entityType-${sliceRange.min}-${sliceRange.max}"
        val projectionId = ProjectionId(projectionName, projectionKey)

        val sourceProvider =
          EventSourcedProvider.eventsBySlices[ItemAdded](
            system,
            readJournalPluginId = GrpcReadJournal.Identifier,
            entityType,
            sliceRange.min,
            sliceRange.max)

        ProjectionBehavior(
          R2dbcProjection.atLeastOnceAsync(
            projectionId,
            settings = None,
            sourceProvider = sourceProvider,
            handler = () => new EventHandler))

      },
      stopMessage = ProjectionBehavior.Stop)
  }

}
