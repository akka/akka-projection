/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.state.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.javadsl.DurableStateStoreQuery
import akka.persistence.state.DurableStateStoreRegistry
import akka.projection.javadsl
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.Source

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
      DurableStateStoreRegistry(system)
        .getDurableStateStoreFor[DurableStateStoreQuery[A]](classOf[DurableStateStoreQuery[A]], pluginId)

    new DurableStateStoreQuerySourceProvider(durableStateStoreQuery, tag, system)
  }

  @InternalApi
  private class DurableStateStoreQuerySourceProvider[A](
      durableStateStoreQuery: DurableStateStoreQuery[A],
      tag: String,
      system: ActorSystem[_])
      extends javadsl.SourceProvider[Offset, DurableStateChange[A]] {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[DurableStateChange[A], NotUsed]] = {
      val source: Future[Source[DurableStateChange[A], NotUsed]] = offsetAsync.get().toScala.map { offsetOpt =>
        durableStateStoreQuery
          .changes(tag, offsetOpt.orElse(NoOffset))
      }
      source.toJava
    }

    override def extractOffset(stateChange: DurableStateChange[A]): Offset = stateChange.offset

    override def extractCreationTime(stateChange: DurableStateChange[A]): Long =
      stateChange match {
        case u: UpdatedDurableState[_] => u.timestamp
        case _                         => 0L // FIXME handle DeletedDurableState when that is added
      }
  }
}
