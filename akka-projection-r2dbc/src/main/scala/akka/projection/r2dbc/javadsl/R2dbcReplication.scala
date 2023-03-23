/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.javadsl

import akka.actor.typed.ActorSystem
import akka.projection.grpc.replication.javadsl.ReplicationProjectionProvider
import akka.projection.r2dbc.R2dbcProjectionSettings

import java.util.Optional

object R2dbcReplication {

  /**
   * Creates a projection provider for using R2dbc as backend for the Akka Projection gRPC transport for Replicated
   * Event Sourcing.
   */
  def create(system: ActorSystem[_]): ReplicationProjectionProvider =
    R2dbcProjection.atLeastOnceFlow(
      _,
      Optional.of(R2dbcProjectionSettings(system).withWarnAboutFilteredEventsInFlow(false)),
      _,
      _,
      _)

  /**
   * Creates a projection provider for using R2dbc as backend for the Akka Projection gRPC transport for Replicated
   * Event Sourcing.
   */
  def create(settings: R2dbcProjectionSettings): ReplicationProjectionProvider =
    R2dbcProjection.atLeastOnceFlow(_, Optional.of(settings.withWarnAboutFilteredEventsInFlow(false)), _, _, _)

}
