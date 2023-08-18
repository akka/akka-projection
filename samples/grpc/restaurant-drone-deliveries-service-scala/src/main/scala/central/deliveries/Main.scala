package central.deliveries

import akka.actor.typed.{ActorSystem, Props, SpawnProtocol}
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.grpc.scaladsl.{ServerReflection, ServiceHandler}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.{ProjectionBehavior, ProjectionId}
import akka.util.Timeout
import central.drones.Drone
import central.drones.proto.{DroneOverviewService, DroneOverviewServiceHandler}
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.jdk.DurationConverters.JavaDurationOps
import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("deliveries.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem(SpawnProtocol(), "deliveries")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }
  }


  def init(implicit system: ActorSystem[SpawnProtocol.Command]): Unit = {
    implicit val ec: ExecutionContext = system.executionContext
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()

    Drone.init(system)

    val interface = system.settings.config.getString("restaurant-drone-deliveries-service.grpc.interface")
    val port = system.settings.config.getInt("restaurant-drone-deliveries-service.grpc.port")

    // consumer runs gRPC server accepting pushed events from producers
    val streamId = "drone-events"
    // FIXME we need to be able to pass protobuf descriptors for the gRPC journal via destination or as param to grpcServiceHandler here
    //       right now failing on deserialization, also probably missing some docs on wire format for producer push
    val destination = EventProducerPushDestination(streamId)

    val composedHandler = ServiceHandler.concatOrNotFound(
      DroneOverviewServiceHandler.partial(new DroneOverviewServiceImpl(system)),
      ServerReflection.partial(List(DroneOverviewService)),
      // FIXME partial push destination for easier combine?
      {
        case req: HttpRequest => EventProducerPushDestination.grpcServiceHandler(destination)(system)(req)
      })


    val bound = Http(system)
      .newServerAt(interface, port)
      .bind(composedHandler)
    bound.foreach(binding =>
      logger.info("Drone event consumer listening at: {}:{}", binding.localAddress.getHostString, binding.localAddress.getPort))


    // FIXME needs to be sharded daemon process, with a few partitions
    // projection picking up pushed drone updates, sending to durable state drone
    val consumerProjectionProvider =
      EventSourcedProvider.eventsBySlices[local.drones.Drone.Event](system, R2dbcReadJournal.Identifier,
        // FIXME The type key on the producer side. Make sure we have documented it.
        "Drone",
        0,
        1023)

    val consumerProjectionId = ProjectionId("drone-consumer", "0-1023")
    implicit val askTimeout: Timeout = system.settings.config.getDuration("restaurant-drone-deliveries-service.drone-ask-timeout").toScala

    val sharding = ClusterSharding(system)
    system ! SpawnProtocol.Spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceAsync(
          consumerProjectionId,
          settings = None,
          sourceProvider = consumerProjectionProvider,
          handler = () => {
            envelope: EventEnvelope[local.drones.Drone.Event] =>
              logger.info(
                "Saw projected event: {}-{}: {}",
                envelope.persistenceId,
                envelope.sequenceNr,
                envelope.eventOption.getOrElse("filtered"))

              // Drone id without producer entity key
              val droneId = PersistenceId.extractEntityId(envelope.persistenceId)
              val entityRef = sharding.entityRefFor(Drone.EntityKey, droneId)
              envelope.event match {
                case local.drones.Drone.CoarseGrainedLocationChanged(location) =>
                  entityRef.askWithStatus(Drone.UpdateLocation("FIXME location name", location, _))
                case unknown =>
                  throw new RuntimeException(s"Unknown event type: ${unknown.getClass}")
              }

          })),
      "consumer-projection",
      Props.empty,
      system.ignoreRef)

  }

}
