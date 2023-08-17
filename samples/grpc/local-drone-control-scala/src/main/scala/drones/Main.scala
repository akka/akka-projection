/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package drones

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.projection.{ProjectionBehavior, ProjectionId}
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.{EventProducer, EventProducerPush}
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import org.slf4j.LoggerFactory
import drone.proto

import scala.concurrent.Future
import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("drones.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem[Nothing](Behaviors.empty, "local-drone-control")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }

  }

  def init(system: ActorSystem[_]): Unit = {
    // FIXME duplicate full cluster management setup like other samples?
    // A single node cluster, to be able to run sharding
    val cluster = Cluster(system)
    cluster.manager ! Join(cluster.selfMember.address)

    Drone.init(system)
    initEventToCloudPush(system)

    val grpcInterface =
      system.settings.config.getString("local-drone-control.grpc.interface")
    val grpcPort =
      system.settings.config.getInt("local-drone-control.grpc.port")
    val grpcService =
      new DroneServiceImpl(system)
    LocalDroneControlServer.start(grpcInterface, grpcPort, system, grpcService)
  }

  def initEventToCloudPush(implicit system: ActorSystem[_]): Unit = {
    val producerOriginId = system.settings.config.getString("local-drone-control.service-id")
    val streamId = "drone-events"

    // turn events into a public protocol (protobuf) type before publishing
    val eventTransformation = EventProducer.Transformation.empty.registerAsyncEnvelopeMapper[Drone.CoarseGrainedLocationChanged, proto.CoarseDroneLocation] { envelope =>
      val event = envelope.event
      Future.successful(Some(proto.CoarseDroneLocation(envelope.persistenceId, event.coordinates.latitude, event.coordinates.longitude)))
    }

    val eventProducer = EventProducerPush[Drone.Event](
      originId = producerOriginId,
      eventProducerSource = EventProducerSource[Drone.Event](
        Drone.EntityKey.name,
        streamId,
        eventTransformation,
        EventProducerSettings(system),
        // only push coarse grained coordinate changes
        producerFilter = envelope => envelope.event.isInstanceOf[Drone.CoarseGrainedLocationChanged]),
      GrpcClientSettings.fromConfig("central-drone-control"))

    // For scaling out the local service this could be split up in slices
    // and run across a cluster with sharded daemon process, now it is instead
    // a single projection actor pushing all events
    val behavior = ProjectionBehavior(
      R2dbcProjection.atLeastOnceFlow[Offset, EventEnvelope[Drone.Event]](
        ProjectionId("drone-event-push", "0-1023"),
        settings = None,
        sourceProvider = EventSourcedProvider.eventsBySlices[Drone.Event](
          system,
          R2dbcReadJournal.Identifier,
          eventProducer.eventProducerSource.entityType,
          0,
          1023),
        handler = eventProducer.handler()))

    system.systemActorOf(behavior, "DroneEventPusher")
  }
}
