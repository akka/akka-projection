package local.drones

import akka.actor.typed.{ ActorSystem, Behavior }
import akka.grpc.GrpcClientSettings
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducerPush
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import org.slf4j.LoggerFactory

import scala.concurrent.Future

object DroneEvents {

  val logger = LoggerFactory.getLogger("local.drones.DroneEvents")

  val StreamId = "drone-events"

  def eventToCloudPushBehavior(
      implicit system: ActorSystem[_]): Behavior[ProjectionBehavior.Command] = {
    val producerOriginId =
      system.settings.config.getString("local-drone-control.service-id")

    logger.info(
      "Pushing events to central cloud, origin id [{}]",
      producerOriginId)

    // turn events into a public protocol (protobuf) type before publishing
    val eventTransformation =
      EventProducer.Transformation.empty.registerAsyncEnvelopeMapper[
        Drone.CoarseGrainedLocationChanged,
        proto.CoarseDroneLocation] { envelope =>
        val event = envelope.event
        Future.successful(
          Some(
            proto.CoarseDroneLocation(
              envelope.persistenceId,
              event.coordinates.latitude,
              event.coordinates.longitude)))
      }

    val eventProducer = EventProducerPush[Drone.Event](
      originId = producerOriginId,
      eventProducerSource = EventProducerSource[Drone.Event](
        Drone.EntityKey.name,
        StreamId,
        eventTransformation,
        EventProducerSettings(system),
        // only push coarse grained coordinate changes
        producerFilter = envelope =>
          envelope.event.isInstanceOf[Drone.CoarseGrainedLocationChanged]),
      GrpcClientSettings.fromConfig("central-drone-control"))

    // For scaling out the local service this would be split up in slices
    // and run across a cluster with sharded daemon process, now it is instead
    // a single projection actor pushing all event slices
    val maxSlice = Persistence(system).numberOfSlices - 1
    ProjectionBehavior(
      R2dbcProjection.atLeastOnceFlow[Offset, EventEnvelope[Drone.Event]](
        ProjectionId("drone-event-push", s"0-$maxSlice"),
        settings = None,
        sourceProvider = EventSourcedProvider.eventsBySlices[Drone.Event](
          system,
          R2dbcReadJournal.Identifier,
          eventProducer.eventProducerSource.entityType,
          0,
          maxSlice),
        handler = eventProducer.handler()))
  }

}
