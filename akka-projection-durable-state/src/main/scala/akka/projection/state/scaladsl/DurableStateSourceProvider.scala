/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.state.scaladsl

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.scaladsl.DurableStateStoreQuery
import akka.persistence.state.DurableStateStoreRegistry
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

/**
 * API may change
 */
@ApiMayChange
object DurableStateSourceProvider {

  def changesByTag[A](
      system: ActorSystem[_],
      pluginId: String,
      tag: String): SourceProvider[Offset, DurableStateChange[A]] = {

    val durableStateStoreQuery =
      DurableStateStoreRegistry(system).durableStateStoreFor[DurableStateStoreQuery[A]](pluginId)

    new DurableStateStoreQuerySourceProvider(durableStateStoreQuery, tag, system)
  }

  private class DurableStateStoreQuerySourceProvider[A](
      durableStateStoreQuery: DurableStateStoreQuery[A],
      tag: String,
      system: ActorSystem[_])
      extends SourceProvider[Offset, DurableStateChange[A]] {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offset: () => Future[Option[Offset]]): Future[Source[DurableStateChange[A], NotUsed]] =
      offset().map { offsetOpt =>
        val offset = offsetOpt.getOrElse(NoOffset)
        durableStateStoreQuery
          .changes(tag, offset)
      }

    override def extractOffset(stateChange: DurableStateChange[A]): Offset = stateChange.offset

    override def extractCreationTime(stateChange: DurableStateChange[A]): Long =
      stateChange match {
        case u: UpdatedDurableState[_] => u.timestamp
        case _                         => 0L // FIXME handle DeletedDurableState when that is added
      }
  }
}
