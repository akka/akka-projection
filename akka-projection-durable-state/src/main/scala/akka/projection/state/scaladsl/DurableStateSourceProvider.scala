/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.state.scaladsl

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.persistence.query.DeletedDurableState
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.scaladsl.DurableStateStoreQuery
import akka.persistence.query.typed.scaladsl.DurableStateStoreBySliceQuery
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.DurableStateStore
import akka.persistence.state.scaladsl.GetObjectResult
import akka.projection.BySlicesSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

/**
 * API may change
 */
object DurableStateSourceProvider {

  def changesByTag[A](
      system: ActorSystem[_],
      pluginId: String,
      tag: String): SourceProvider[Offset, DurableStateChange[A]] = {

    val durableStateStoreQuery =
      DurableStateStoreRegistry(system).durableStateStoreFor[DurableStateStoreQuery[A]](pluginId)
    changesByTag(system, durableStateStoreQuery, tag)
  }

  def changesByTag[A](
      system: ActorSystem[_],
      durableStateStoreQuery: DurableStateStoreQuery[A],
      tag: String): SourceProvider[Offset, DurableStateChange[A]] = {
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
        .durableStateStoreFor[DurableStateStoreBySliceQuery[A]](durableStateStoreQueryPluginId)
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
      .durableStateStoreFor[DurableStateStoreBySliceQuery[Any]](durableStateStoreQueryPluginId)
      .sliceForPersistenceId(persistenceId)

  def sliceRanges(
      system: ActorSystem[_],
      durableStateStoreQueryPluginId: String,
      numberOfRanges: Int): immutable.Seq[Range] =
    DurableStateStoreRegistry(system)
      .durableStateStoreFor[DurableStateStoreBySliceQuery[Any]](durableStateStoreQueryPluginId)
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

    override def source(offset: () => Future[Option[Offset]]): Future[Source[DurableStateChange[A], NotUsed]] =
      offset().map { offsetOpt =>
        val offset = offsetOpt.getOrElse(NoOffset)
        durableStateStoreQuery
          .changesBySlices(entityType, minSlice, maxSlice, offset)
      }

    override def extractOffset(stateChange: DurableStateChange[A]): Offset = stateChange.offset

    override def extractCreationTime(stateChange: DurableStateChange[A]): Long =
      stateChange match {
        case u: UpdatedDurableState[_] => u.timestamp
        case d: DeletedDurableState[_] => d.timestamp
      }

    override def getObject(persistenceId: String): Future[GetObjectResult[A]] =
      durableStateStoreQuery.getObject(persistenceId)
  }
}
