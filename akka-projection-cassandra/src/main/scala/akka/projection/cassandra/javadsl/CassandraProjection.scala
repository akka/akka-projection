/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.projection.HandlerRecoveryStrategy
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.StatusObserver
import akka.projection.StrictRecoveryStrategy
import akka.projection.cassandra.internal.CassandraProjectionImpl
import akka.projection.cassandra.internal.GroupedHandlerAdapter
import akka.projection.cassandra.internal.HandlerAdapter
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SourceProviderAdapter
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider

/**
 * Factories of [[Projection]] where the offset is stored in Cassandra. The envelope handler can
 * integrate with anything, such as publishing to a message broker, or updating a read model in Cassandra.
 */
@ApiMayChange
object CassandraProjection {
  import CassandraProjectionImpl.AtLeastOnce
  import CassandraProjectionImpl.AtMostOnce
  import CassandraProjectionImpl.GroupedHandlerStrategy
  import CassandraProjectionImpl.SingleHandlerStrategy

  /**
   * Create a [[Projection]] with at-least-once processing semantics. It stores the offset in Cassandra
   * after the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset some elements may be processed more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceCassandraProjection.withSaveOffset]] of the returned
   * `AtLeastOnceCassandraProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[Envelope]): AtLeastOnceCassandraProjection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      new SourceProviderAdapter(sourceProvider),
      settingsOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = SingleHandlerStrategy(new HandlerAdapter(handler)),
      statusObserver = NoopStatusObserver)

  /**
   * Create a [[Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes,
   * whatever happens first. This window can be defined with [[GroupedCassandraProjection.withGroup]] of
   * the returned `GroupedCassandraProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.grouped`.
   *
   * It stores the offset in Cassandra immediately after the `handler` has processed the envelopes, but that
   * is still with at-least-once processing semantics. This means that if the projection is restarted
   * from previously stored offset the previous group of envelopes may be processed more than once.
   */
  def groupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[java.util.List[Envelope]]): GroupedCassandraProjection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      new SourceProviderAdapter(sourceProvider),
      settingsOpt = None,
      offsetStrategy = CassandraProjectionImpl
        .AtLeastOnce(afterEnvelopes = Some(1), orAfterDuration = Some(scala.concurrent.duration.Duration.Zero)),
      handlerStrategy = GroupedHandlerStrategy(new GroupedHandlerAdapter(handler)),
      statusObserver = NoopStatusObserver)

  /**
   * Create a [[Projection]] with at-most-once processing semantics. It stores the offset in Cassandra
   * before the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset one envelope may not have been processed.
   */
  def atMostOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[Envelope]): AtMostOnceCassandraProjection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      new SourceProviderAdapter(sourceProvider),
      settingsOpt = None,
      offsetStrategy = AtMostOnce(),
      handlerStrategy = SingleHandlerStrategy(new HandlerAdapter(handler)),
      statusObserver = NoopStatusObserver)
}

@DoNotInherit trait CassandraProjection[Envelope] extends Projection[Envelope] {

  override def withSettings(settings: ProjectionSettings): CassandraProjection[Envelope]

  override def withStatusObserver(observer: StatusObserver[Envelope]): CassandraProjection[Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def initializeOffsetTable(system: ActorSystem[_]): CompletionStage[Done]
}

@DoNotInherit trait AtLeastOnceCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  override def withSettings(settings: ProjectionSettings): AtLeastOnceCassandraProjection[Envelope]

  override def withStatusObserver(observer: StatusObserver[Envelope]): AtLeastOnceCassandraProjection[Envelope]

  def withSaveOffset(afterEnvelopes: Int, afterDuration: java.time.Duration): AtLeastOnceCassandraProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: HandlerRecoveryStrategy): AtLeastOnceCassandraProjection[Envelope]
}

trait GroupedCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  override def withSettings(settings: ProjectionSettings): GroupedCassandraProjection[Envelope]

  override def withStatusObserver(observer: StatusObserver[Envelope]): GroupedCassandraProjection[Envelope]

  def withGroup(groupAfterEnvelopes: Int, groupAfterDuration: java.time.Duration): GroupedCassandraProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: HandlerRecoveryStrategy): GroupedCassandraProjection[Envelope]
}

@DoNotInherit trait AtMostOnceCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  override def withSettings(settings: ProjectionSettings): AtMostOnceCassandraProjection[Envelope]

  override def withStatusObserver(observer: StatusObserver[Envelope]): AtMostOnceCassandraProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: StrictRecoveryStrategy): AtMostOnceCassandraProjection[Envelope]
}
