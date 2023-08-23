package local.drones

import akka.actor.typed.{ ActorSystem, Props, SpawnProtocol }
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("drones.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem[SpawnProtocol.Command](SpawnProtocol(), "local-drone-control")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }

  }

  def init(system: ActorSystem[SpawnProtocol.Command]): Unit = {
    // A single node, but still a cluster, to be able to run sharding for the entities
    val cluster = Cluster(system)
    cluster.manager ! Join(cluster.selfMember.address)

    Drone.init(system)
    system ! SpawnProtocol.Spawn(
      DroneEvents.eventToCloudPushBehavior(system),
      "DroneEventsProjection",
      Props.empty,
      system.ignoreRef)

    val grpcInterface =
      system.settings.config.getString("local-drone-control.grpc.interface")
    val grpcPort =
      system.settings.config.getInt("local-drone-control.grpc.port")
    val grpcService =
      new DroneServiceImpl(system)
    LocalDroneControlServer.start(grpcInterface, grpcPort, system, grpcService)
  }

}
