package central.drones

import akka.actor.typed.ActorSystem
import akka.grpc.GrpcServiceException
import akka.persistence.r2dbc.session.scaladsl.R2dbcSession
import akka.serialization.SerializationExtension
import central.drones.proto.DroneOverviewService
import io.grpc.Status
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class DroneOverviewServiceImpl(system: ActorSystem[_])
    extends DroneOverviewService {

  private val logger = LoggerFactory.getLogger(getClass)

  private implicit val ec: ExecutionContext = system.executionContext
  private val serialization = SerializationExtension(system)

  // FIXME should it be a separate table because of the extra column?
  private val findByLocationSql =
    "SELECT state_ser_id, state_ser_manifest, state_payload " +
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
            val serializerId =
              row.get("state_ser_id", classOf[java.lang.Integer])
            val serializerManifest =
              row.get("state_ser_manifest", classOf[String])
            val payload = row.get("state_payload", classOf[Array[Byte]])
            val state =
              serialization
                .deserialize(payload, serializerId, serializerManifest)
                .get
                .asInstanceOf[Drone.State]
            state.currentLocation
        }
        .map { maybeLocations =>
          val locations = maybeLocations.flatten

          if (locations.isEmpty)
            throw new GrpcServiceException(Status.NOT_FOUND)
          else
            proto.CoarseDroneLocationsResponse(
              locations.map(location =>
                proto
                  .CoarseDroneLocations(location.latitude, location.longitude)))
        }
    }
  }
}
