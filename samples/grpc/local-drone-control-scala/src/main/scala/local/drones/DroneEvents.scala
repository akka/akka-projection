package local.drones

import akka.actor.typed.{ ActorSystem, Behavior }
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
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

  def eventToCloudPushBehavior(settings: Settings)(
      implicit system: ActorSystem[_]): Behavior[ProjectionBehavior.Command] = {
    logger.info(
      "Pushing events to central cloud, origin id [{}]",
      settings.locationId)

    // turn events into a public protocol (protobuf) type before publishing
    val eventTransformation =
      EventProducer.Transformation.empty.registerAsyncEnvelopeMapper[
        Drone.CoarseGrainedLocationChanged,
        proto.CoarseDroneLocation] { envelope =>
        val event = envelope.event
        Future.successful(
          Some(proto.CoarseDroneLocation(Some(event.coordinates.toProto))))
      }

    val eventProducer = EventProducerPush[Drone.Event](
      // location id is unique and informative, so use it as producer origin id as well
      originId = settings.locationId,
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

  def initEventToCloudDaemonProcess(settings: Settings)(
      implicit system: ActorSystem[_]): Unit = {
    logger.info(
      "Pushing events to central cloud, origin id [{}]",
      settings.locationId)

    val nrOfEventProducers =
      system.settings.config.getInt("local-drone-control.nr-of-event-producers")
    val sliceRanges = Persistence(system).sliceRanges(nrOfEventProducers)

    // turn events into a public protocol (protobuf) type before publishing
    val eventTransformation =
      EventProducer.Transformation.empty.registerAsyncEnvelopeMapper[
        Drone.CoarseGrainedLocationChanged,
        proto.CoarseDroneLocation] { envelope =>
        val event = envelope.event
        Future.successful(
          Some(proto.CoarseDroneLocation(Some(event.coordinates.toProto))))
      }

    val eventProducer = EventProducerPush[Drone.Event](
      // location id is unique and informative, so use it as producer origin id as well
      originId = settings.locationId,
      eventProducerSource = EventProducerSource[Drone.Event](
        Drone.EntityKey.name,
        StreamId,
        eventTransformation,
        EventProducerSettings(system),
        // only push coarse grained coordinate changes
        producerFilter = envelope =>
          envelope.event.isInstanceOf[Drone.CoarseGrainedLocationChanged])
        // #startFromSnapshot
        // start from latest drone snapshot and don't replay history
        .withStartingFromSnapshots[
          Drone.State,
          Drone.CoarseGrainedLocationChanged](state =>
          Drone.CoarseGrainedLocationChanged(
            state.coarseGrainedCoordinates.get)),
      // #startFromSnapshot
      GrpcClientSettings.fromConfig("central-drone-control"))

    def projectionForPartition(
        partition: Int): Behavior[ProjectionBehavior.Command] = {
      val sliceRange = sliceRanges(partition)
      val minSlice = sliceRange.min
      val maxSlice = sliceRange.max

      ProjectionBehavior(
        R2dbcProjection.atLeastOnceFlow[Offset, EventEnvelope[Drone.Event]](
          ProjectionId("drone-event-push", s"$minSlice-$maxSlice"),
          settings = None,
          sourceProvider = EventSourcedProvider.eventsBySlices[Drone.Event](
            system,
            R2dbcReadJournal.Identifier,
            eventProducer.eventProducerSource.entityType,
            minSlice,
            maxSlice),
          handler = eventProducer.handler()))

    }

    ShardedDaemonProcess(system).init(
      "drone-event-push",
      nrOfEventProducers,
      projectionForPartition)

  }

}
