package iot.registration

import scala.util.control.NonFatal

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import org.slf4j.LoggerFactory

object Main {

  val logger = LoggerFactory.getLogger("iot.registration.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem[Nothing](Behaviors.empty, "iot-service")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }
  }

  def init(system: ActorSystem[_]): Unit = {
    AkkaManagement(system).start()
    ClusterBootstrap(system).start()

    Registration.init(system)

    val eventProducerService = PublishEvents.eventProducerService(system)

    val grpcInterface =
      system.settings.config.getString("iot-service.grpc.interface")
    val grpcPort =
      system.settings.config.getInt("iot-service.grpc.port")
    val grpcService = new RegistrationServiceImpl(system)
    RegistrationServer.start(
      grpcInterface,
      grpcPort,
      system,
      grpcService,
      eventProducerService)
  }

}
