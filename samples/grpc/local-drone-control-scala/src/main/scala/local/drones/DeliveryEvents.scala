package local.drones

import akka.actor.typed.{ ActorRef, ActorSystem, Behavior }
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import akka.projection.{ ProjectionBehavior, ProjectionId }
import akka.util.Timeout

/**
 * Consume delivery events from the cloud and pass to the delivery queue actor
 */
object DeliveryEvents {

  def projectionBehavior(
      queueActor: ActorRef[DeliveriesQueue.Command],
      settings: Settings)(
      implicit system: ActorSystem[_]): Behavior[ProjectionBehavior.Command] = {
    val projectionName: String = "delivery-events"

    implicit val timeout: Timeout = settings.askTimeout

    // initial consumer topic filter for location id
    // FIXME no docs of setting up initial consumer filter, am I missing some API?
    //       Async setup is a race condition but maybe ok? Does not seem to quite work, all or the wrong events are delivered
    ConsumerFilter(system).ref ! ConsumerFilter.UpdateFilter(
      // FIXME stream-id duplicated in config
      "delivery-events",
      // location id already is in the format of a topic filter expression
      Vector(
        ConsumerFilter.ExcludeRegexEntityIds(Set(".*")),
        ConsumerFilter.IncludeTopics(Set(settings.locationId))))

    val eventsBySlicesQuery =
      GrpcReadJournal(
        List(central.deliveries.proto.DeliveryEventsProto.javaDescriptor))

    // single projection handling all slices
    val sliceRanges =
      Persistence(system).sliceRanges(1)
    val sliceRange = sliceRanges(0)
    val projectionKey =
      s"${eventsBySlicesQuery.streamId}-${sliceRange.min}-${sliceRange.max}"
    val projectionId = ProjectionId.of(projectionName, projectionKey)

    val sourceProvider = EventSourcedProvider
      .eventsBySlices[central.deliveries.proto.DeliveryRegistered](
        system,
        eventsBySlicesQuery,
        eventsBySlicesQuery.streamId,
        sliceRange.min,
        sliceRange.max)

    import akka.actor.typed.scaladsl.AskPattern._
    val handler: Handler[
      EventEnvelope[central.deliveries.proto.DeliveryRegistered]] = {
      envelope =>
        queueActor.ask(
          DeliveriesQueue.AddDelivery(
            DeliveriesQueue.WaitingDelivery(
              deliveryId = envelope.event.deliveryId,
              from = Coordinates.fromProto(envelope.event.origin.get),
              to = Coordinates.fromProto(envelope.event.destination.get)),
            _))
    }

    ProjectionBehavior(
      R2dbcProjection
        .atLeastOnceAsync(projectionId, None, sourceProvider, () => handler))

  }

}
