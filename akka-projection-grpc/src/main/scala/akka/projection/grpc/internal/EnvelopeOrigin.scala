/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.annotation.InternalStableApi
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.EventEnvelope

/**
 * INTERNAL API
 *
 * See akka.persistence.r2dbc.internal.EnvelopeOrigin, but we don't have a dependency
 * to akka-persistence-r2dbc here
 */
@InternalStableApi private[akka] object EnvelopeOrigin {
  val SourceQuery = ""
  val SourceBacktracking = "BT"
  val SourcePubSub = "PS"
  val SourceSnapshot = "SN"
  val SourceHeartbeat = "HB"

  def fromQuery(env: EventEnvelope[_]): Boolean =
    env.source == SourceQuery

  def fromBacktracking(env: EventEnvelope[_]): Boolean =
    env.source == SourceBacktracking

  def fromBacktracking(change: UpdatedDurableState[_]): Boolean =
    change.value == null

  def fromPubSub(env: EventEnvelope[_]): Boolean =
    env.source == SourcePubSub

  def fromSnapshot(env: EventEnvelope[_]): Boolean =
    env.source == SourceSnapshot

  def fromHeartbeat(env: EventEnvelope[_]): Boolean =
    env.source == SourceHeartbeat

  def isHeartbeatEvent(env: Any): Boolean =
    env match {
      case e: EventEnvelope[_] => fromHeartbeat(e)
      case _                   => false
    }

  def isFilteredEvent(env: Any): Boolean =
    env match {
      case e: EventEnvelope[_] => e.filtered
      case _                   => false
    }

}
