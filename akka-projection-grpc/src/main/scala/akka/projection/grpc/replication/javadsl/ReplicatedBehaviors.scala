/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javadsl

import akka.actor.typed.Behavior
import akka.annotation.ApiMayChange
import akka.japi.function.{ Function => JFunction }
import akka.persistence.typed.javadsl.EventSourcedBehavior
import akka.persistence.typed.javadsl.ReplicationContext

/**
 * Dynamically provides factory methods for creating replicated event sourced behaviors.
 *
 * Must be used to create an event sourced behavior to be replicated with [[Replication.grpcReplication]].
 *
 * Can optionally be composed with other Behavior factories, to get access to actor context or timers.
 */
@ApiMayChange
abstract class ReplicatedBehaviors[Command, Event, State] {
  def setup(factory: JFunction[ReplicationContext, EventSourcedBehavior[Command, Event, State]]): Behavior[Command]
}
