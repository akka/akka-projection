/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import java.time.Instant

import scala.annotation.nowarn
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import com.example.shoppingcart.ShoppingcartApiProto

@nowarn("msg=never used")
object ConsumerDocSpec {

  implicit val system: ActorSystem[_] = ActorSystem[Nothing](Behaviors.empty, "test")

  val streamId = "ShoppingCart"
  val sliceRanges = Persistence(system).sliceRanges(1)
  val idx = 0

  val eventsBySlicesQuery = GrpcReadJournal(protobufDescriptors = List(ShoppingcartApiProto.javaDescriptor))

  val sliceRange = sliceRanges(idx)

  //#adjustStartOffset
  val sourceProvider =
    EventSourcedProvider
      .eventsBySlices[String](
        system,
        eventsBySlicesQuery,
        eventsBySlicesQuery.streamId,
        sliceRange.min,
        sliceRange.max,
        adjustStartOffset = { (storedOffset: Option[Offset]) =>
          val startOffset = Offset.timestamp(Instant.now().minusSeconds(3600))
          Future.successful(Some(startOffset))
        })
  //#adjustStartOffset

}
