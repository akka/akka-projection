package local.drones;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.Join;

public class Main {

  public static void main(String[] args) {
    ActorSystem.create(rootBehavior(), "local-drone-control");
  }

  private static Behavior<Void> rootBehavior() {
    return Behaviors.setup((ActorContext<Void> context) -> {

      var settings = Settings.create(context.getSystem());

      context.getLog()
          .info("Local Drone Control [{}] starting up", settings.locationId);

      // A single node, but still a cluster, to be able to run sharding for the entities
      var cluster = Cluster.get(context.getSystem());
      cluster.manager().tell(new Join(cluster.selfMember().address()));

      // keep track of local drones, project aggregate info to the cloud
      Drone.init(context.getSystem());
      context.spawn(
          DroneEvents.eventToCloudPushBehavior(context.getSystem(), settings),
          "DroneEventsProjection");

      // consume delivery events from the cloud service
      var deliveriesQueue = context.spawn(DeliveriesQueue.create(), "DeliveriesQueue");
      context.spawn(
          DeliveryEvents.projectionBehavior(context.getSystem(), deliveriesQueue, settings),
          "DeliveriesProjection");
      var deliveriesQueueService = new DeliveriesQueueServiceImpl(context.getSystem(), settings, deliveriesQueue);
      var droneService = new DroneServiceImpl(context.getSystem(), deliveriesQueue, settings);

      var grpcInterface =
          context.getSystem().settings().config()
              .getString("local-drone-control.grpc.interface");
      var grpcPort =
          context.getSystem().settings().config().getInt("local-drone-control.grpc.port");

      LocalDroneControlServer.start(
          grpcInterface,
          grpcPort,
          context.getSystem(),
          droneService,
          deliveriesQueueService);

      return Behaviors.empty();
    });
  }
}
