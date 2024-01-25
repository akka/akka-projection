package iot;

import akka.actor.typed.ActorSystem;
import akka.grpc.javadsl.ServerReflection;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import iot.registration.proto.RegistrationService;
import iot.registration.proto.RegistrationServiceHandlerFactory;
import iot.temperature.proto.SensorTwinService;
import iot.temperature.proto.SensorTwinServiceHandlerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IotServer {

  private static final Logger logger = LoggerFactory.getLogger(IotServer.class);

  public static void start(
      ActorSystem<?> system,
      String host,
      int port,
      RegistrationService registrationService,
      Function<HttpRequest, CompletionStage<HttpResponse>> registrationEventProducerService,
      SensorTwinService sensorTwinService,
      Function<HttpRequest, CompletionStage<HttpResponse>> pushedTemperatureEventsHandler) {

    @SuppressWarnings("unchecked")
    var service =
        ServiceHandler.concatOrNotFound(
            RegistrationServiceHandlerFactory.create(registrationService, system),
            SensorTwinServiceHandlerFactory.create(sensorTwinService, system),
            registrationEventProducerService,
            pushedTemperatureEventsHandler,
            ServerReflection.create(
                Arrays.asList(RegistrationService.description, SensorTwinService.description),
                system));

    var bound = Http.get(system).newServerAt(host, port).bind(service);

    bound.whenComplete(
        (binding, error) -> {
          if (error == null) {
            logger.info(
                "IoT Service online at gRPC server {}:{}",
                binding.localAddress().getHostString(),
                binding.localAddress().getPort());
            binding.addToCoordinatedShutdown(Duration.ofSeconds(3), system);
          } else {
            logger.error("Failed to bind gRPC endpoint, terminating system", error);
            system.terminate();
          }
        });
  }
}
