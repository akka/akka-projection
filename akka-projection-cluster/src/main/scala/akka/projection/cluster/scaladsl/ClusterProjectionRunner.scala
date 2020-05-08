/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cluster.scaladsl

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.projection.Projection
import akka.projection.cluster.internal.ClusterProjectionRunnerImpl

object ClusterProjectionRunner {

  def init[Envelope](
      system: ActorSystem[_],
      projectionName: String,
      numberOfInstances: Int,
      projectionFactory: Int => Projection[Envelope]): Unit =
    init(system, projectionName, numberOfInstances, projectionFactory, ShardedDaemonProcessSettings(system))

  def init[Envelope](
      system: ActorSystem[_],
      projectionName: String,
      numberOfInstances: Int,
      projectionFactory: Int => Projection[Envelope],
      shardedDaemonSettings: ShardedDaemonProcessSettings): Unit =
    ClusterProjectionRunnerImpl.init(
      system,
      projectionName,
      numberOfInstances,
      projectionFactory,
      shardedDaemonSettings)
}
