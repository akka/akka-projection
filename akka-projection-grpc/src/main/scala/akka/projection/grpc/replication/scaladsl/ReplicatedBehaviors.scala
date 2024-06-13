/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.actor.typed.Behavior
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicationContext

/**
 * Dynamically provides factory methods for creating replicated event sourced behaviors.
 *
 * Must be used to create an event sourced behavior to be replicated with [[Replication.grpcReplication]].
 *
 * Can optionally be composed with other Behavior factories, to get access to actor context or timers.
 */
abstract class ReplicatedBehaviors[Command, Event, State] {
  def setup(factory: ReplicationContext => EventSourcedBehavior[Command, Event, State]): Behavior[Command]
}
