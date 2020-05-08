/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cluster.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.japi.function
import akka.projection.Projection
import akka.projection.internal.ProjectionBehavior

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ClusterProjectionRunnerImpl {

  /**
   * Scala API
   */
  def init[Envelope](
      system: ActorSystem[_],
      projectionName: String,
      numberOfInstances: Int,
      projectionFactory: Int => Projection[Envelope],
      shardedDaemonSettings: ShardedDaemonProcessSettings): Unit =
    ShardedDaemonProcess(system)
      .init[ProjectionBehavior.Command](
        s"projection-$projectionName",
        numberOfInstances,
        i => ProjectionBehavior(() => projectionFactory(i)),
        shardedDaemonSettings,
        Some(ProjectionBehavior.Stop))

  /**
   * Java API
   */
  def init[Envelope](
      system: ActorSystem[_],
      projectionName: String,
      numberOfInstances: Int,
      projectionFactory: function.Function[Int, Projection[Envelope]],
      shardedDaemonSettings: ShardedDaemonProcessSettings): Unit =
    init(system, projectionName, numberOfInstances, projectionFactory, shardedDaemonSettings)
}
