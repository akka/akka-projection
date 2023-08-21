/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package central

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Props
import akka.actor.typed.SpawnProtocol
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.projection.Projection
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.util.Timeout
import central.Main.logger
import central.drones.Drone

import scala.concurrent.Future
import scala.jdk.DurationConverters.JavaDurationOps

object LocalDroneEvents {

  val DroneEventStreamId = "drone-events"

  def pushedEventsGrpcHandler(implicit system: ActorSystem[_])
      : PartialFunction[HttpRequest, Future[HttpResponse]] = {
    // FIXME we need to be able to pass protobuf descriptors for the gRPC journal via destination or as param to grpcServiceHandler here
    //       right now failing on deserialization, also probably missing some docs on wire format for producer push
    val destination = EventProducerPushDestination(DroneEventStreamId)
      .withTransformationForOrigin((origin, _) =>
        EventProducerPushDestination.Transformation.empty
          .registerTagMapper[local.drones.Drone.CoarseGrainedLocationChanged] {
            _ =>
              // FIXME dodgy scheme, something better? Pass it in the events?
              // tag all events with origin
              Set("location:" + origin)
          })

    // FIXME actual partial push destination for easier combine
    { case req: HttpRequest =>
      EventProducerPushDestination.grpcServiceHandler(destination)(system)(req)
    }
  }

  def initPushedEventsConsumer(implicit system: ActorSystem[_]): Unit = {
    // FIXME The type key on the producer side. Make sure we have documented it.
    val EntityType = "Drone"

    implicit val askTimeout: Timeout = system.settings.config
      .getDuration("restaurant-drone-deliveries-service.drone-ask-timeout")
      .toScala

    val sharding = ClusterSharding(system)

    def sourceProvider(sliceRange: Range)
        : SourceProvider[Offset, EventEnvelope[local.drones.Drone.Event]] =
      EventSourcedProvider.eventsBySlices[local.drones.Drone.Event](
        system,
        readJournalPluginId = R2dbcReadJournal.Identifier,
        EntityType,
        sliceRange.min,
        sliceRange.max)

    def projection(sliceRange: Range)
        : Projection[EventEnvelope[local.drones.Drone.Event]] = {
      val minSlice = sliceRange.min
      val maxSlice = sliceRange.max
      val projectionId =
        ProjectionId("DroneEvents", s"drone-$minSlice-$maxSlice")

      val handler: Handler[EventEnvelope[local.drones.Drone.Event]] = {
        (envelope: EventEnvelope[local.drones.Drone.Event]) =>
          logger.info(
            "Saw projected event: {}-{}: {}",
            envelope.persistenceId,
            envelope.sequenceNr,
            envelope.eventOption.getOrElse("filtered"))

          // Drone id without producer entity key
          val droneId =
            PersistenceId.extractEntityId(envelope.persistenceId)
          val entityRef = sharding.entityRefFor(Drone.EntityKey, droneId)
          // FIXME we are getting
          //  java.lang.ClassCastException: class akka.persistence.FilteredPayload$ cannot be cast to class local.drones.Drone$Event (akka.persistence.FilteredPayload$ and local.drones.Drone$Event are in unnamed module of loader 'app')
          //  here
          envelope.event match {
            case local.drones.Drone.CoarseGrainedLocationChanged(location) =>
              val originName = envelope.tags
                .find(_.startsWith("location:"))
                .get
                .drop("location:".length)
              entityRef.askWithStatus(
                Drone.UpdateLocation(originName, location, _))
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
