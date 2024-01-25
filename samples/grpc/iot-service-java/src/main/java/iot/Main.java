package iot;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import iot.registration.Registration;
import iot.registration.RegistrationEvents;
import iot.registration.RegistrationServiceImpl;
import iot.temperature.SensorTwin;
import iot.temperature.SensorTwinServiceImpl;
import iot.temperature.TemperatureEvents;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    var system = ActorSystem.create(SpawnProtocol.create(), "iot-service");
    try {
      init(system);
    } catch (Throwable e) {
      logger.error("Terminating due to initialization failure.", e);
      system.terminate();
    }
  }

  private static void init(ActorSystem<SpawnProtocol.Command> system) {
    AkkaManagement.get(system).start();
    ClusterBootstrap.get(system).start();

    Registration.init(system);
    Function<HttpRequest, CompletionStage<HttpResponse>> registrationEventProducerService =
        RegistrationEvents.eventProducerService(system);

    SensorTwin.init(system);
    TemperatureEvents.initPushedEventsConsumer(system);
    Function<HttpRequest, CompletionStage<HttpResponse>> pushedTemperatureEventsHandler =
        TemperatureEvents.pushedEventsGrpcHandler(system);

    var registrationService = new RegistrationServiceImpl(system);
    var sensorTwinService = new SensorTwinServiceImpl(system);

    var grpcInterface = system.settings().config().getString("iot-service.grpc.interface");
    var grpcPort = system.settings().config().getInt("iot-service.grpc.port");

    IotServer.start(
        system,
        grpcInterface,
        grpcPort,
        registrationService,
        registrationEventProducerService,
        sensorTwinService,
        pushedTemperatureEventsHandler);
  }
}
