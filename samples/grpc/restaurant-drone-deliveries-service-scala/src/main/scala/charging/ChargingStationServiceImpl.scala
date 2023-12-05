package charging

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.util.Timeout
import charging.proto
import com.google.protobuf.timestamp.Timestamp
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class ChargingStationServiceImpl(
    entityRefFactory: String => EntityRef[ChargingStation.Command])(
    implicit val system: ActorSystem[_])
    extends proto.ChargingStationService {
  private final val log = LoggerFactory.getLogger(getClass)
  import system.executionContext
  implicit val askTimeout: Timeout = 3.seconds

  override def createChargingStation(in: proto.CreateChargingStationRequest)
      : Future[proto.CreateChargingStationResponse] = {
    log.info(
      "Creating charging station {} with {} charging slots, in location {}",
      in.chargingStationId,
      in.chargingSlots,
      in.locationId)

    val entityRef = entityRefFactory(in.chargingStationId)
    entityRef
      .ask(ChargingStation.Create(in.locationId, in.chargingSlots, _))
      .map(_ => proto.CreateChargingStationResponse.defaultInstance)

  }

  override def getChargingStationState(in: proto.GetChargingStationStateRequest)
      : Future[proto.GetChargingStationStateResponse] = {
    log.info("Get charging station {} state", in.chargingStationId)
    val entityRef = entityRefFactory(in.chargingStationId)
    entityRef
      .ask(ChargingStation.GetState.apply)
      .map(state =>
        proto.GetChargingStationStateResponse(
          locationId = state.stationLocationId,
          chargingSlots = state.chargingSlots,
          currentlyChargingDrones = state.dronesCharging
            .map(d =>
              proto.ChargingDrone(
                droneId = d.droneId,
                chargingComplete = Some(Timestamp(d.chargingDone))))
            .toSeq))
  }
}
