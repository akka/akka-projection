package local.drones;

import akka.actor.typed.ActorSystem;
import akka.grpc.javadsl.ServerReflection;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import local.drones.proto.DeliveriesQueueService;
import local.drones.proto.DeliveriesQueueServiceHandlerFactory;
import local.drones.proto.DroneService;
import local.drones.proto.DroneServiceHandlerFactory;

import java.time.Duration;
import java.util.Arrays;

public class LocalDroneControlServer {

  public static void start(    String host,
                            int port,
                          ActorSystem<?> system,
                          DroneService droneService,
                          DeliveriesQueueService deliveriesQueueService) {
    var service =
        ServiceHandler.concatOrNotFound(
            DroneServiceHandlerFactory.create(droneService, system),
            DeliveriesQueueServiceHandlerFactory.create(deliveriesQueueService, system),
            // ServerReflection enabled to support grpcurl without import-path and proto parameters
            ServerReflection.create(
                Arrays.asList(DroneService.description, DeliveriesQueueService.description), system));

    var bound =
        Http.get(system)
            .newServerAt(host, port)
        .bind(service);

    bound.whenComplete((binding, error) -> {
      if (error == null) {
        binding.addToCoordinatedShutdown(Duration.ofSeconds(3), system);
        var address = binding.localAddress();
        system.log().info(
            "Drone control gRPC server started {}:{}",
            address.getHostString(),
            address.getPort());
      } else {
        system.log().error("Failed to bind gRPC endpoint, terminating system", error);
        system.terminate();
      }
    });
  }
}
