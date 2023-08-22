package central.drones

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.grpc.GrpcServiceException
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.persistence.typed.PersistenceId
import akka.serialization.SerializationExtension
import akka.util.Timeout
import central.drones.proto.{DroneOverviewService, GetDroneOverviewRequest, GetDroneOverviewResponse}
import io.grpc.Status
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.DurationConverters.JavaDurationOps

class DroneOverviewServiceImpl(system: ActorSystem[_])
    extends DroneOverviewService {

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val ec: ExecutionContext = system.executionContext
  private implicit val timeout: Timeout = system.settings.config.getDuration("restaurant-drone-deliveries-service.drone-ask-timeout").toScala
  private val serialization = SerializationExtension(system)
  private val sharding = ClusterSharding(system)

  private val findByLocationSql =
    "SELECT persistence_id, state_ser_id, state_ser_manifest, state_payload " +
    "FROM durable_state " +
    "WHERE location = $1"

  override def getCoarseDroneLocations(in: proto.CoarseDroneLocationsRequest)
      : Future[proto.CoarseDroneLocationsResponse] = {
    // query against additional columns for drone
    logger.info("List drones for location {}", in.location)
    R2dbcSession.withSession(system) { session =>
      session
        .select(
          session.createStatement(findByLocationSql).bind(0, in.location)) {
          row =>
            val serializerId = row.get("state_ser_id", classOf[java.lang.Integer])
            val serializerManifest = row.get("state_ser_manifest", classOf[String])
            val payload = row.get("state_payload", classOf[Array[Byte]])
            val state =
              serialization
                .deserialize(payload, serializerId, serializerManifest)
                .get
                .asInstanceOf[Drone.State]
            val droneId = PersistenceId.extractEntityId(row.get("persistence_id", classOf[String]))
            state.currentLocation.map(coordinates => (droneId, coordinates))
        }
        .map { maybeLocations =>
          val locations = maybeLocations.flatten

          if (locations.isEmpty)
            throw new GrpcServiceException(Status.NOT_FOUND)
          else {
            val byLocation = locations.groupMap { case (_, coarse) => coarse } { case (droneId, _) => droneId }

            proto.CoarseDroneLocationsResponse(
              byLocation.map { case (location, entries) =>
                proto
                  .CoarseDroneLocations(location.latitude, location.longitude, entries)
              }.toVector)
          }
        }
    }
  }

  override def getDroneOverview(in: GetDroneOverviewRequest): Future[GetDroneOverviewResponse] = {
    // query against additional columns for drone
    logger.info("Get drone overview for drone {}", in.droneId)

    val entityRef = sharding.entityRefFor(Drone.EntityKey, in.droneId)

    val reply: Future[Drone.State] = entityRef.ask(Drone.GetState(_))

    reply.map(state =>
      GetDroneOverviewResponse(
        locationName = state.locationName,
        coarseLatitude = state.currentLocation.map(_.latitude).getOrElse(0.0),
        coarseLongitude = state.currentLocation.map(_.longitude).getOrElse(0.0),
    ))
  }
}
