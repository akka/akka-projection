/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package drones

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object Main {

  val logger = LoggerFactory.getLogger("drones.Main")

  def main(args: Array[String]): Unit = {
    val system =
      ActorSystem[Nothing](Behaviors.empty, "local-drone-control")
    try {
      init(system)
    } catch {
      case NonFatal(e) =>
        logger.error("Terminating due to initialization failure.", e)
        system.terminate()
    }

  }

  def init(system: ActorSystem[_]): Unit = {
    // FIXME duplicate full management setup like other samples
    val cluster = Cluster(system)
    cluster.manager ! Join(cluster.selfMember.address)

    // AkkaManagement(system).start()
    // ClusterBootstrap(system).start()

    Drone.init(system)

    val grpcInterface =
      system.settings.config.getString("local-drone-control.grpc.interface")
    val grpcPort =
      system.settings.config.getInt("local-drone-control.grpc.port")
    val grpcService =
      new DroneServiceImpl(system)
    LocalDroneControlServer.start(grpcInterface, grpcPort, system, grpcService)
  }
}
