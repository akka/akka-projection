/*
 * Copyright (C) 2009-2024 Lightbend Inc. <https://www.lightbend.com>
 */
package drones

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.GrpcClientSettings
import com.typesafe.config.ConfigFactory
import common.proto.Coordinates
import local.drones.proto.DroneServiceClient
import local.drones.proto.ReportLocationRequest

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

/**
 * Used for Akka Projection CI tests verifying native-image build works, without needing to start up
 * a cloud node for the gRPC replication. This is not something you would typically have in an actual
 * Akka Edge application test suite.
 */
object DroneClientTestApp {

  def main(args: Array[String]): Unit = {
    val host = args(0)
    val port = args(1).toInt

    implicit val system =
      ActorSystem[Nothing](
        Behaviors.empty[Nothing],
        "DroneClientTestApp",
        ConfigFactory.empty())

    val droneServiceClient = DroneServiceClient(
      GrpcClientSettings.connectToServiceAt(host, port).withTls(false))
    Await.result(
      droneServiceClient.reportLocation(
        ReportLocationRequest(
          droneId = "native-image-test",
          coordinates = Some(Coordinates(7.94111d, 14.36312d)),
          altitude = 20)),
      10.seconds)
    // successful response means round trip to database worked

    // FIXME: Optimally we would also start a push destination, or publish Akka Projection gRPC events to the edge
    //        service and verify that it works in a native-image, but this is will have to be enough for automated tests
    //        for now
    system.terminate()
  }

}
