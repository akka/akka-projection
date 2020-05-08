/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cluster.internal

import java.util.function.Supplier

import scala.collection.immutable
import scala.jdk.CollectionConverters._

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.cluster.sharding.typed
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
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
      projections: immutable.IndexedSeq[() => Projection[Envelope]],
      shardedDaemonSettings: ShardedDaemonProcessSettings): Unit = {

    typed.scaladsl
      .ShardedDaemonProcess(system)
      .init[ProjectionBehavior.Command](
        s"projection-$projectionName",
        projections.size - 1,
        i => ProjectionBehavior(projections(i)),
        shardedDaemonSettings,
        Some(ProjectionBehavior.Stop))
  }

  /**
   * Java API
   */
  def init[Envelope](
      system: ActorSystem[_],
      projectionName: String,
      projections: java.util.List[Supplier[Projection[Envelope]]],
      shardedDaemonSettings: ShardedDaemonProcessSettings): Unit = {

    val scalaSeq = projections.asScala.toIndexedSeq.map { sup => () => sup.get() }
    init(system, projectionName, scalaSeq, shardedDaemonSettings)
  }
}
