package local.drones;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.typed.ClusterSingleton;
import akka.cluster.typed.SingletonActor;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import charging.ChargingStation;

/**
 * Main for starting the local-drone-control as a cluster rather than a single self-contained node.
 * Requires a separate database, start with config from local{1,2,3}.conf files for running as
 * cluster locally.
 *
 * <p>This should be started with -Dconfig.resource=application-cluster.conf or
 * `-Dconfig.resource=local1.conf`
 */
public class ClusteredMain {

  public static void main(String[] args) {
    ActorSystem.create(rootBehavior(), "local-drone-control");
  }

  private static Behavior<Void> rootBehavior() {
    return Behaviors.setup(
        (ActorContext<Void> context) -> {
          var settings = Settings.create(context.getSystem());

          context.getLog().info("Local Drone Control [{}] starting up", settings.locationId);

          // Bootstrap cluster
          AkkaManagement.get(context.getSystem()).start();
          ClusterBootstrap.get(context.getSystem()).start();

          // keep track of local drones, project aggregate info to the cloud
          Drone.init(context.getSystem());
          DroneEvents.initEventToCloudDaemonProcess(context.getSystem(), settings);

          // start prometheus for custom metrics
          Telemetry.Id.get(context.getSystem()).start();

          // consume delivery events from the cloud service, single queue in cluster singleton
          var deliveriesQueue =
              ClusterSingleton.get(context.getSystem())
                  .init(SingletonActor.of(DeliveriesQueue.create(), "DeliveriesQueue"));

          // single queue, single grpc projection consumer
          ClusterSingleton.get(context.getSystem())
              .init(
                  SingletonActor.of(
                      DeliveryEvents.projectionBehavior(
                          context.getSystem(), deliveriesQueue, settings),
                      "DeliveriesProjection"));

          var deliveriesQueueService =
              new DeliveriesQueueServiceImpl(context.getSystem(), settings, deliveriesQueue);

          // replicated charging station entity
          var chargingStationReplication =
              ChargingStation.initEdge(context.getSystem(), settings.locationId);

          var droneService =
              new DroneServiceImpl(
                  context.getSystem(),
                  deliveriesQueue,
                  // FIXME wrong function type
                  id -> chargingStationReplication.entityRefFactory().apply(id),
                  settings);

          var grpcInterface =
              context
                  .getSystem()
                  .settings()
                  .config()
                  .getString("local-drone-control.grpc.interface");
          var grpcPort =
              context.getSystem().settings().config().getInt("local-drone-control.grpc.port");

          LocalDroneControlServer.start(
              grpcInterface, grpcPort, context.getSystem(), droneService, deliveriesQueueService);

          return Behaviors.empty();
        });
  }
}
