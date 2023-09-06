package central;

import akka.actor.typed.ActorSystem;
import akka.grpc.javadsl.ServerReflection;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import central.deliveries.proto.RestaurantDeliveriesService;
import central.deliveries.proto.RestaurantDeliveriesServiceHandlerFactory;
import central.drones.proto.DroneOverviewService;
import central.drones.proto.DroneOverviewServiceHandlerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DroneDeliveriesServer {

  private static final Logger logger = LoggerFactory.getLogger(DroneDeliveriesServer.class);

  public static void start(
      ActorSystem<?> system,
      String host,
      int port,
      central.drones.proto.DroneOverviewService droneOverviewService,
      central.deliveries.proto.RestaurantDeliveriesService restaurantDeliveriesService,
      Function<HttpRequest, CompletionStage<HttpResponse>> deliveryEventsProducerService,
      Function<HttpRequest, CompletionStage<HttpResponse>> pushedDroneEventsHandler) {

    // #composeAndBind
    @SuppressWarnings("unchecked")
    var service =
        ServiceHandler.concatOrNotFound(
            DroneOverviewServiceHandlerFactory.create(droneOverviewService, system),
            RestaurantDeliveriesServiceHandlerFactory.create(restaurantDeliveriesService, system),
            ServerReflection.create(
                Arrays.asList(
                    DroneOverviewService.description, RestaurantDeliveriesService.description),
                system),
            deliveryEventsProducerService,
            // FIXME not last once actually partial
            pushedDroneEventsHandler);

    var bound = Http.get(system).newServerAt(host, port).bind(service);
    // #composeAndBind

    bound.whenComplete(
        (binding, error) -> {
          if (error == null) {
            logger.info(
                "Drone event consumer listening at: {}:{}",
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
