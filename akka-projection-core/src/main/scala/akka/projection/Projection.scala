/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange

import scala.concurrent.{ ExecutionContext, Future }

/**
 * The core abstraction in Akka Projections.
 *
 * A projection instance may share the same name and [[StreamElement]], but must have a unique key. The key is used
 * to achieve processing parallelism for a projection.
 *
 * For example, many projections may share the same name "user-events-projection", but can process elements for
 * different sharded entities within Akka Cluster, where key could be the Akka Cluster shardId.
 * @tparam StreamElement The entity type of the projection.
 */
@ApiMayChange
trait Projection[StreamElement] {

  /**
   * The projection name. The projection name is shared across multiple instances of [[Projection]] with different keys.
   *
   * For example, a "user-view" could be the name of a projection.
   */
  def name: String

  /**
   * The unique key for this Projection instance. The key must be unique for a projection name.
   *
   * For example, a "user-view" could have multiple projections with different keys representing different partitions,
   * shards, etc.
   */
  def key: String

  /**
   * The method wrapping the user EventHandler function.
   *
   * @return A [[scala.concurrent.Future]] that represents the asynchronous completion of the user EventHandler
   *         function.
   */
  def processElement(elt: StreamElement): Future[StreamElement]

  /**
   * Start to run a Projection.
   *
   * @param systemProvider An Akka ActorSystem.
   */
  def start()(implicit systemProvider: ClassicActorSystemProvider): Unit

  /**
   * Stop the projection if it's running.
   */
  def stop(): Future[Done]
}
