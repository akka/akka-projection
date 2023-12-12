package central;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.SpawnProtocol;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import akka.projection.grpc.consumer.javadsl.EventProducerPushDestination;
import akka.projection.grpc.producer.javadsl.EventProducer;
import central.deliveries.DeliveryEvents;
import central.deliveries.RestaurantDeliveries;
import central.deliveries.RestaurantDeliveriesServiceImpl;
import central.drones.Drone;
import central.drones.DroneOverviewServiceImpl;
import central.drones.LocalDroneEvents;
import charging.ChargingStation;
import charging.ChargingStationServiceImpl;
import java.util.Set;
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
    var chargingStationReplication = ChargingStation.init(system);

    var host =
        system.settings().config().getString("restaurant-drone-deliveries-service.grpc.interface");
    var port = system.settings().config().getInt("restaurant-drone-deliveries-service.grpc.port");

    var pushedEventsDestination = LocalDroneEvents.pushedEventsDestination(system);
    var deliveryEventsProducerSource = DeliveryEvents.eventProducerSource(system);
    var droneOverviewService = new DroneOverviewServiceImpl(system, settings);
    var restaurantDeliveriesService = new RestaurantDeliveriesServiceImpl(system, settings);

    // #replicationEndpoint
    var chargingStationService =
        new ChargingStationServiceImpl(
            settings,
            // FIXME java function type
            id -> chargingStationReplication.entityRefFactory().apply(id));

    // delivery events and charging station replication both are Akka Projection gRPC event
    // producers (pulled by the local drone control) and needs to be combined into a single gRPC
    // service handling both:
    var eventPullHandler =
        EventProducer.grpcServiceHandler(
            system,
            Set.of(deliveryEventsProducerSource, chargingStationReplication.eventProducerSource()));

    // the drone events from edge and the charging station replicated entity are both Akka
    // Projection gRPC
    // event push destinations (pushed by local drone control) and needs to be combined into a
    // single gRPC service handling both:
    var eventPushHandler =
        EventProducerPushDestination.grpcServiceHandler(
            Set.of(
                pushedEventsDestination,
                chargingStationReplication.eventProducerPushDestination().get()),
            system);
    // #replicationEndpoint

    DroneDeliveriesServer.start(
        system,
        host,
        port,
        droneOverviewService,
        restaurantDeliveriesService,
        chargingStationService,
        eventPullHandler,
        eventPushHandler);
  }
}
