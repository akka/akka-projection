/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc

import scala.concurrent.duration._

import akka.projection.grpc.consumer.GrpcQuerySettings

/**
 * The regular gRPC projection scenarios, with keep alives flowing in both directions, so that they are shown not to
 * interfere with acks, filter updates and replays.
 */
class IntegrationWithKeepAliveSpec extends IntegrationSpec(new TestContainerConf) {

  override protected def grpcQuerySettings(streamId: String): GrpcQuerySettings =
    super.grpcQuerySettings(streamId).withKeepAliveInterval(1.second).withKeepAliveTimeout(3.seconds)
}
