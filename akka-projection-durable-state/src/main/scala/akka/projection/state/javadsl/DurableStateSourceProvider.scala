/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
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
import akka.annotation.InternalApi
import akka.japi.Pair
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.javadsl.DurableStateStoreQuery
import akka.persistence.query.typed.javadsl.DurableStateStoreBySliceQuery
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.javadsl.DurableStateStore
import akka.persistence.state.javadsl.GetObjectResult
import akka.projection.BySlicesSourceProvider
import akka.projection.javadsl
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.Source

/**
 * API may change
 */
object DurableStateSourceProvider {

  def changesByTag[A](
      system: ActorSystem[_],
      pluginId: String,
      tag: String): SourceProvider[Offset, DurableStateChange[A]] = {
    val durableStateStoreQuery =
      DurableStateStoreRegistry(system)
        .getDurableStateStoreFor[DurableStateStoreQuery[A]](classOf[DurableStateStoreQuery[A]], pluginId)
    changesByTag(system, durableStateStoreQuery, tag)
  }

  def changesByTag[A](
      system: ActorSystem[_],
      durableStateStoreQuery: DurableStateStoreQuery[A],
      tag: String): SourceProvider[Offset, DurableStateChange[A]] = {
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
        case d: DeletedDurableState[_] => d.timestamp
      }
  }

  def changesBySlices[A](
      system: ActorSystem[_],
      durableStateStoreQueryPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, DurableStateChange[A]] = {

    val durableStateStoreQuery =
      DurableStateStoreRegistry(system)
        .getDurableStateStoreFor(classOf[DurableStateStoreBySliceQuery[A]], durableStateStoreQueryPluginId)
    changesBySlices(system, durableStateStoreQuery, entityType, minSlice, maxSlice)
  }

  def changesBySlices[A](
      system: ActorSystem[_],
      durableStateStoreQuery: DurableStateStoreBySliceQuery[A],
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, DurableStateChange[A]] = {
    new DurableStateBySlicesSourceProvider(durableStateStoreQuery, entityType, minSlice, maxSlice, system)
  }

  def sliceForPersistenceId(
      system: ActorSystem[_],
      durableStateStoreQueryPluginId: String,
      persistenceId: String): Int =
    DurableStateStoreRegistry(system)
      .getDurableStateStoreFor(classOf[DurableStateStoreBySliceQuery[Any]], durableStateStoreQueryPluginId)
      .sliceForPersistenceId(persistenceId)

  def sliceRanges(
      system: ActorSystem[_],
      durableStateStoreQueryPluginId: String,
      numberOfRanges: Int): java.util.List[Pair[Integer, Integer]] =
    DurableStateStoreRegistry(system)
      .getDurableStateStoreFor(classOf[DurableStateStoreBySliceQuery[Any]], durableStateStoreQueryPluginId)
      .sliceRanges(numberOfRanges)

  private class DurableStateBySlicesSourceProvider[A](
      durableStateStoreQuery: DurableStateStoreBySliceQuery[A],
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      system: ActorSystem[_])
      extends SourceProvider[Offset, DurableStateChange[A]]
      with BySlicesSourceProvider
      with DurableStateStore[A] {
    implicit val executionContext: ExecutionContext = system.executionContext

    override def source(offsetAsync: Supplier[CompletionStage[Optional[Offset]]])
        : CompletionStage[Source[DurableStateChange[A], NotUsed]] = {
      val source: Future[Source[DurableStateChange[A], NotUsed]] = offsetAsync.get().toScala.map { offsetOpt =>
        durableStateStoreQuery
          .changesBySlices(entityType, minSlice, maxSlice, offsetOpt.orElse(NoOffset))
      }
      source.toJava
    }

    override def extractOffset(stateChange: DurableStateChange[A]): Offset = stateChange.offset

    override def extractCreationTime(stateChange: DurableStateChange[A]): Long =
      stateChange match {
        case u: UpdatedDurableState[_] => u.timestamp
        case d: DeletedDurableState[_] => d.timestamp
      }

    override def getObject(persistenceId: String): CompletionStage[GetObjectResult[A]] =
      durableStateStoreQuery.getObject(persistenceId)
  }
}
