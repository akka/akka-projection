package iot

import scala.util.control.NonFatal

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement
import iot.registration.RegistrationEvents
import iot.registration.Registration
import iot.registration.RegistrationServiceImpl
import iot.temperature.SensorTwin
import iot.temperature.SensorTwinServiceImpl
import iot.temperature.TemperatureEvents
import org.slf4j.LoggerFactory

object Main {

  val logger = LoggerFactory.getLogger("iot.Main")

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
    val registrationEventProducerService =
      RegistrationEvents.eventProducerService(system)

    SensorTwin.init(system)
    TemperatureEvents.initPushedEventsConsumer(system)
    val pushedTemperatureEventsHandler =
      TemperatureEvents.pushedEventsGrpcHandler(system)

    val grpcInterface =
      system.settings.config.getString("iot-service.grpc.interface")
    val grpcPort =
      system.settings.config.getInt("iot-service.grpc.port")
    val registrationService = new RegistrationServiceImpl(system)
    val sensorTwinService = new SensorTwinServiceImpl(system)

    IotServer.start(
      grpcInterface,
      grpcPort,
      system,
      registrationService,
      registrationEventProducerService,
      pushedTemperatureEventsHandler,
      sensorTwinService)
  }

}
