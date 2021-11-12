/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.state.scaladsl

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.persistence.query.DurableStateChange
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.UpdatedDurableState
import akka.persistence.query.typed.scaladsl.DurableStateStoreBySliceQuery
import akka.persistence.state.DurableStateStoreRegistry
import akka.persistence.state.scaladsl.DurableStateStore
import akka.persistence.state.scaladsl.GetObjectResult
import akka.projection.eventsourced.scaladsl.TimestampOffsetBySlicesSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

// FIXME this should be incorporated in Akka Projections

object DurableStateSourceProvider2 {

  def changesBySlices[A](
      system: ActorSystem[_],
      durableStateStoreQueryPluginId: String,
      entityType: String,
      minSlice: Int,
      maxSlice: Int): SourceProvider[Offset, DurableStateChange[A]] = {

    val durableStateStoreQuery =
      DurableStateStoreRegistry(system).durableStateStoreFor[DurableStateStoreBySliceQuery[A]](
        durableStateStoreQueryPluginId)

    new DurableStateStoreQuerySourceProvider2(durableStateStoreQuery, entityType, minSlice, maxSlice, system)
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

  private class DurableStateStoreQuerySourceProvider2[A](
      durableStateStoreQuery: DurableStateStoreBySliceQuery[A],
      entityType: String,
      override val minSlice: Int,
      override val maxSlice: Int,
      system: ActorSystem[_])
      extends SourceProvider[Offset, DurableStateChange[A]]
      with TimestampOffsetBySlicesSourceProvider
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
        case other                     =>
          // FIXME case DeletedDurableState when that is added
          throw new IllegalArgumentException(
            s"DurableStateChange [${other.getClass.getName}] not implemented yet. Please report bug at https://github.com/akka/akka-persistence-r2dbc/issues")
      }

    override def getObject(persistenceId: String): Future[GetObjectResult[A]] =
      durableStateStoreQuery.getObject(persistenceId)
  }
}
