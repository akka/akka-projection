/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.scaladsl

import akka.actor.typed.ActorSystem
import akka.projection.grpc.replication.scaladsl.ReplicationProjectionProvider
import akka.projection.r2dbc.R2dbcProjectionSettings

object R2dbcReplication {

  /**
   * Creates a projection provider for using R2dbc as backend for the Akka Projection gRPC transport for Replicated
   * Event Sourcing.
   */
  def apply()(implicit system: ActorSystem[_]): ReplicationProjectionProvider =
    R2dbcProjection.atLeastOnceFlow(
      _,
      Some(R2dbcProjectionSettings(system).withWarnAboutFilteredEventsInFlow(false)),
      _,
      _)(_)

  /**
   * Creates a projection provider for using R2dbc as backend for the Akka Projection gRPC transport for Replicated
   * Event Sourcing.
   */
  def apply(settings: R2dbcProjectionSettings)(implicit system: ActorSystem[_]): ReplicationProjectionProvider =
    R2dbcProjection.atLeastOnceFlow(_, Some(settings.withWarnAboutFilteredEventsInFlow(false)), _, _)(_)

}
