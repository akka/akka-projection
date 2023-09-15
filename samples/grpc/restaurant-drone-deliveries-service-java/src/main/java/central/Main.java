package central;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import central.deliveries.DeliveryEvents;
import central.deliveries.RestaurantDeliveries;
import central.deliveries.RestaurantDeliveriesServiceImpl;
import central.drones.Drone;
import central.drones.DroneOverviewServiceImpl;
import central.drones.LocalDroneEvents;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    var system = ActorSystem.create(SpawnProtocol.create(), "deliveries");
    try {
      init(system);
    } catch (Throwable e) {
      logger.error("Terminating due to initialization failure.", e);
      system.terminate();
    }
  }

  private static void init(ActorSystem<SpawnProtocol.Command> system) {
    var settings = DeliveriesSettings.create(system);
    AkkaManagement.get(system).start();
    ClusterBootstrap.get(system).start();

    Drone.init(system);
    LocalDroneEvents.initPushedEventsConsumer(system);
    RestaurantDeliveries.init(system);

    var host =
        system.settings().config().getString("restaurant-drone-deliveries-service.grpc.interface");
    var port = system.settings().config().getInt("restaurant-drone-deliveries-service.grpc.port");

    var pushedDroneEventsHandler = LocalDroneEvents.pushedEventsGrpcHandler(system);
    var deliveryEventsProducerService = DeliveryEvents.eventProducerService(system);
    var droneOverviewService = new DroneOverviewServiceImpl(system, settings);
    var restaurantDeliveriesService = new RestaurantDeliveriesServiceImpl(system, settings);

    DroneDeliveriesServer.start(
        system,
        host,
        port,
        droneOverviewService,
        restaurantDeliveriesService,
        deliveryEventsProducerService,
        pushedDroneEventsHandler);
  }
}
