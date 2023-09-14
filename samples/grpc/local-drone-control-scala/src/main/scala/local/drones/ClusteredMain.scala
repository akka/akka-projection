package local.drones

import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.{ ActorSystem, Behavior }
import akka.cluster.typed.{ ClusterSingleton, SingletonActor }
import akka.management.cluster.bootstrap.ClusterBootstrap
import akka.management.scaladsl.AkkaManagement

/**
 * Main for starting the local-drone-control as a cluster rather than a single self-contained node. Requires
 * a separate database, start with config from local{1,2,3}.conf files for running as cluster locally.
 */
object ClusteredMain {

  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](rootBehavior(), "local-drone-control")
  }

  private def rootBehavior(): Behavior[Nothing] = Behaviors.setup[Nothing] {
    context =>
      val settings = Settings(context.system)

      context.log
        .info("Local Drone Control [{}] starting up", settings.locationId)

      // Bootstrap cluster
      AkkaManagement(context.system).start()
      ClusterBootstrap(context.system).start()

      // keep track of local drones, project aggregate info to the cloud
      Drone.init(context.system)
      DroneEvents.initEventToCloudDaemonProcess(settings)(context.system)

      // consume delivery events from the cloud service, single queue in cluster singleton
      val deliveriesQueue =
        ClusterSingleton(context.system).init(
          SingletonActor[DeliveriesQueue.Command](
            DeliveriesQueue(),
            "DeliveriesQueue"))

      // single queue, single grpc projection consumer
      ClusterSingleton(context.system).init(
        SingletonActor(
          DeliveryEvents.projectionBehavior(deliveriesQueue, settings)(
            context.system),
          "DeliveriesProjection"))
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

}
