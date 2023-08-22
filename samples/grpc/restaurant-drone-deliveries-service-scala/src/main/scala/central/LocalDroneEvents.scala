/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package central

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.{
  ClusterSharding,
  ShardedDaemonProcess
}
import akka.http.scaladsl.model.{ HttpRequest, HttpResponse }
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.projection.{ Projection, ProjectionBehavior, ProjectionId }
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.{ Handler, SourceProvider }
import akka.util.Timeout
import central.Main.logger
import central.drones.{CoarseGrainedCoordinates, Drone}

import scala.concurrent.Future
import scala.jdk.DurationConverters.JavaDurationOps

/**
 * Handle drone events pushed by the local drone control systems.
 */
object LocalDroneEvents {

  val DroneEventStreamId = "drone-events"

  // FIXME The type key on the producer side. Make sure we have documented it.
  private val ProducerEntityType = "Drone"

  def pushedEventsGrpcHandler(implicit system: ActorSystem[_])
      : PartialFunction[HttpRequest, Future[HttpResponse]] = {
    val destination = EventProducerPushDestination(
      DroneEventStreamId,
      local.drones.proto.DroneEventsProto.javaDescriptor.getFile :: Nil)
      .withTransformationForOrigin((origin, _) =>
        EventProducerPushDestination.Transformation.empty
          // tag all events with the location name of the local control it came from)
          .registerTagMapper[local.drones.proto.CoarseDroneLocation](_ =>
            Set("location:" + origin)))

    // FIXME partial return type still isn't quite right
    EventProducerPushDestination
      .grpcServiceHandler(destination)(system)
      .asInstanceOf[PartialFunction[HttpRequest, Future[HttpResponse]]]
  }

  def initPushedEventsConsumer(implicit system: ActorSystem[_]): Unit = {

    implicit val askTimeout: Timeout = system.settings.config
      .getDuration("restaurant-drone-deliveries-service.drone-ask-timeout")
      .toScala

    val sharding = ClusterSharding(system)

    def sourceProvider(sliceRange: Range): SourceProvider[
      Offset,
      EventEnvelope[local.drones.proto.CoarseDroneLocation]] =
      EventSourcedProvider
        .eventsBySlices[local.drones.proto.CoarseDroneLocation](
          system,
          readJournalPluginId = R2dbcReadJournal.Identifier,
          ProducerEntityType,
          sliceRange.min,
          sliceRange.max)

    def projection(sliceRange: Range)
        : Projection[EventEnvelope[local.drones.proto.CoarseDroneLocation]] = {
      val minSlice = sliceRange.min
      val maxSlice = sliceRange.max
      val projectionId =
        ProjectionId("DroneEvents", s"drone-$minSlice-$maxSlice")

      val handler
          : Handler[EventEnvelope[local.drones.proto.CoarseDroneLocation]] = {
        (envelope: EventEnvelope[local.drones.proto.CoarseDroneLocation]) =>
          logger.info(
            "Saw projected event: {}-{}: {}",
            envelope.persistenceId,
            envelope.sequenceNr,
            envelope.eventOption)

          // Drone id without producer entity key
          val droneId =
            PersistenceId.extractEntityId(envelope.persistenceId)
          val entityRef = sharding.entityRefFor(Drone.EntityKey, droneId)
          // FIXME we are getting
          //  java.lang.ClassCastException: class akka.persistence.FilteredPayload$ cannot be cast to class local.drones.Drone$Event (akka.persistence.FilteredPayload$ and local.drones.Drone$Event are in unnamed module of loader 'app')
          //  here
          envelope.event match {
            case local.drones.proto
                  .CoarseDroneLocation(droneId, latitude, longitude, _) =>
              val originName = envelope.tags
                .find(_.startsWith("location:"))
                .get
                .drop("location:".length)
              entityRef.askWithStatus(
                Drone.UpdateLocation(
                  originName,
                  CoarseGrainedCoordinates(latitude, longitude),
                  _))
            case unknown =>
              throw new RuntimeException(
                s"Unknown event type: ${unknown.getClass}")
          }
      }

      R2dbcProjection.atLeastOnceAsync(
        projectionId,
        settings = None,
        sourceProvider(sliceRange),
        handler = () => handler)
    }

    // Split the slices into N ranges
    val numberOfSliceRanges: Int = system.settings.config.getInt(
      "restaurant-drone-deliveries-service.drones.projections-slice-count")
    val sliceRanges = EventSourcedProvider.sliceRanges(
      system,
      R2dbcReadJournal.Identifier,
      numberOfSliceRanges)

    ShardedDaemonProcess(system).init(
      name = "LocalDronesProjection",
      numberOfInstances = sliceRanges.size,
      behaviorFactory = i => ProjectionBehavior(projection(sliceRanges(i))),
      stopMessage = ProjectionBehavior.Stop)

  }

}
