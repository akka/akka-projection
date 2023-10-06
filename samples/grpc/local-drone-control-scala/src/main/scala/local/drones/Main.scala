package local.drones

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorSystem, Behavior }
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join

object Main {

  // #main
  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](rootBehavior(), "local-drone-control")
  }

  private def rootBehavior(): Behavior[Nothing] = Behaviors.setup[Nothing] {
    context =>
      val settings = Settings(context.system)

      context.log
        .info("Local Drone Control [{}] starting up", settings.locationId)

      // A single node, but still a cluster, to be able to run sharding for the entities
      val cluster = Cluster(context.system)
      cluster.manager ! Join(cluster.selfMember.address)

      // keep track of local drones, project aggregate info to the cloud
      Drone.init(context.system)
      context.spawn(
        DroneEvents.eventToCloudPushBehavior(settings)(context.system),
        "DroneEventsProjection")

      // consume delivery events from the cloud service
      val deliveriesQueue = context.spawn(DeliveriesQueue(), "DeliveriesQueue")
      context.spawn(
        DeliveryEvents.projectionBehavior(deliveriesQueue, settings)(
          context.system),
        "DeliveriesProjection")
      val deliveriesQueueService =
        new DeliveriesQueueServiceImpl(settings, deliveriesQueue)(
          context.system)

      val grpcInterface =
        context.system.settings.config
          .getString("local-drone-control.grpc.interface")
      val grpcPort =
        context.system.settings.config.getInt("local-drone-control.grpc.port")
      val droneService =
        new DroneServiceImpl(deliveriesQueue, settings)(context.system)
      LocalDroneControlServer.start(
        grpcInterface,
        grpcPort,
        context.system,
        droneService,
        deliveriesQueueService)

      Behaviors.empty
  }
  // #main

}
