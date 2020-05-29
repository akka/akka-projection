/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import java.util.concurrent.CompletionStage

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.RunningProjection
import akka.projection.StrictRecoveryStrategy
import akka.projection.cassandra.javadsl
import akka.projection.cassandra.scaladsl
import akka.projection.internal.HandlerRecoveryImpl
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CassandraProjectionImpl {
  sealed trait OffsetStrategy
  final case class AtMostOnce(recoveryStrategy: Option[StrictRecoveryStrategy] = None) extends OffsetStrategy
  final case class AtLeastOnce(
      afterEnvelopes: Option[Int] = None,
      orAfterDuration: Option[FiniteDuration] = None,
      recoveryStrategy: Option[HandlerRecoveryStrategy] = None)
      extends OffsetStrategy

  sealed trait HandlerStrategy[Envelope] {
    def lifecycle: HandlerLifecycle
  }
  final case class SingleHandlerStrategy[Envelope](handler: Handler[Envelope]) extends HandlerStrategy[Envelope] {
    override def lifecycle: HandlerLifecycle = handler
  }
  final case class GroupedHandlerStrategy[Envelope](
      handler: Handler[immutable.Seq[Envelope]],
      afterEnvelopes: Option[Int] = None,
      orAfterDuration: Option[FiniteDuration] = None)
      extends HandlerStrategy[Envelope] {
    override def lifecycle: HandlerLifecycle = handler
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraProjectionImpl[Offset, Envelope](
    override val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    settingsOpt: Option[ProjectionSettings],
    val offsetStrategy: CassandraProjectionImpl.OffsetStrategy,
    handlerStrategy: CassandraProjectionImpl.HandlerStrategy[Envelope])
    extends javadsl.CassandraProjection[Envelope]
    with scaladsl.CassandraProjection[Envelope]
    with javadsl.AtLeastOnceCassandraProjection[Envelope]
    with scaladsl.AtLeastOnceCassandraProjection[Envelope]
    with javadsl.GroupedCassandraProjection[Envelope]
    with scaladsl.GroupedCassandraProjection[Envelope]
    with javadsl.AtMostOnceCassandraProjection[Envelope]
    with scaladsl.AtMostOnceCassandraProjection[Envelope] {

  import CassandraProjectionImpl._
  import HandlerRecoveryImpl.applyUserRecovery

  override def withSettings(settings: ProjectionSettings): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(projectionId, sourceProvider, Option(settings), offsetStrategy, handlerStrategy)

  /**
   * Settings for AtLeastOnceCassandraProjection
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt,
      atLeastOnceStrategy
        .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)),
      handlerStrategy)

  /**
   * Java API
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: java.time.Duration): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt,
      atLeastOnceStrategy
        .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration.toScala)),
      handlerStrategy)

  /**
   * Settings for GroupedCassandraProjection
   */
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt,
      offsetStrategy,
      handlerStrategy
        .asInstanceOf[GroupedHandlerStrategy[Envelope]]
        .copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration)))

  /**
   * Java API
   */
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: java.time.Duration): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt,
      offsetStrategy,
      handlerStrategy
        .asInstanceOf[GroupedHandlerStrategy[Envelope]]
        .copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration.toScala)))

  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt,
      atLeastOnceStrategy.copy(recoveryStrategy = Some(recoveryStrategy)),
      handlerStrategy)

  /**
   * Settings for AtMostOnceCassandraProjection
   */
  override def withRecoveryStrategy(
      recoveryStrategy: StrictRecoveryStrategy): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt,
      atMostOnceStrategy.copy(recoveryStrategy = Some(recoveryStrategy)),
      handlerStrategy)

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection =
    new InternalProjectionState(settingsOrDefaults).newRunningInstance()

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  @InternalApi
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] =
    new InternalProjectionState(settingsOrDefaults).mappedSource()

  /*
   * Build the final ProjectionSettings to use, if currently set to None fallback to values in config file
   */
  private def settingsOrDefaults(implicit system: ActorSystem[_]): ProjectionSettings =
    settingsOpt.getOrElse(ProjectionSettings(system))

  // FIXME make the sessionConfigPath configurable so that it can use same session as akka.persistence.cassandra or alpakka.cassandra
  private val sessionConfigPath = "akka.projection.cassandra"

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  private class InternalProjectionState(settings: ProjectionSettings)(implicit system: ActorSystem[_]) {

    private val killSwitch = KillSwitches.shared(projectionId.id)

    private[projection] def mappedSource(): Source[Done, _] = {
      // FIXME maybe use the session-dispatcher config
      implicit val ec: ExecutionContext = system.executionContext

      val logger = Logging(system.classicSystem, this.getClass)

      // FIXME session lookup could be moved to CassandraOffsetStore if that's better
      val session = CassandraSessionRegistry(system).sessionFor(sessionConfigPath)
      val offsetStore = new CassandraOffsetStore(session)
      val readOffsets = () => offsetStore.readOffset(projectionId)

      val source: Source[(Offset, Envelope), NotUsed] =
        Source
          .futureSource(handlerStrategy.lifecycle.tryStart().flatMap(_ => sourceProvider.source(readOffsets)))
          .via(killSwitch.flow)
          .map(envelope => sourceProvider.extractOffset(envelope) -> envelope)
          .mapMaterializedValue(_ => NotUsed)

      def handlerFlow(recoveryStrategy: HandlerRecoveryStrategy): Flow[(Offset, Envelope), Offset, NotUsed] =
        handlerStrategy match {
          case SingleHandlerStrategy(handler) =>
            Flow[(Offset, Envelope)].mapAsync(parallelism = 1) {
              case (offset, envelope) =>
                applyUserRecovery(recoveryStrategy, offset, offset, logger, () => handler.process(envelope))
                  .map(_ => offset)
            }

          case grouped: GroupedHandlerStrategy[Envelope] =>
            val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
            val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
            Flow[(Offset, Envelope)]
              .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
              .filterNot(_.isEmpty)
              .mapAsync(parallelism = 1) { group =>
                val firstOffset = group.head._1
                val lastOffset = group.last._1
                val envelopes = group.map(_._2)
                applyUserRecovery[Offset](
                  recoveryStrategy,
                  firstOffset,
                  lastOffset,
                  logger,
                  () => grouped.handler.process(envelopes))
                  .map(_ => lastOffset)
              }
        }

      val composedSource: Source[Done, NotUsed] = offsetStrategy match {
        case AtLeastOnce(afterEnvelopesOpt, orAfterDurationOpt, recoveryStrategyOpt) =>
          val afterEnvelopes = afterEnvelopesOpt.getOrElse(settings.saveOffsetAfterEnvelopes)
          val orAfterDuration = orAfterDurationOpt.getOrElse(settings.saveOffsetAfterDuration)
          val recoveryStrategy = recoveryStrategyOpt.getOrElse(settings.recoveryStrategy)

          if (afterEnvelopes == 1)
            // optimization of general AtLeastOnce case
            source.via(handlerFlow(recoveryStrategy)).mapAsync(1) { offset =>
              offsetStore.saveOffset(projectionId, offset)
            }
          else
            source
              .via(handlerFlow(recoveryStrategy))
              .groupedWithin(afterEnvelopes, orAfterDuration)
              .collect { case grouped if grouped.nonEmpty => grouped.last }
              .mapAsync(parallelism = 1) { offset =>
                offsetStore.saveOffset(projectionId, offset)
              }

        case AtMostOnce(recoveryStrategyOpt) =>
          val recoveryStrategy = recoveryStrategyOpt.getOrElse(settings.recoveryStrategy)
          val handler = handlerStrategy match {
            case SingleHandlerStrategy(handler) => handler
            case _                              =>
              // not possible
              throw new IllegalStateException("Unsupported combination of atMostOnce and grouped")
          }

          source
            .mapAsync(parallelism = 1) {
              case (offset, envelope) =>
                offsetStore
                  .saveOffset(projectionId, offset)
                  .flatMap(_ =>
                    applyUserRecovery(recoveryStrategy, offset, offset, logger, () => handler.process(envelope)))
            }
            .map(_ => Done)
      }

      RunningProjection.stopHandlerOnTermination(composedSource, () => handlerStrategy.lifecycle.tryStop())
    }

    private[projection] def newRunningInstance(): RunningProjection = {
      new CassandraRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), killSwitch)
    }
  }

  private class CassandraRunningProjection(source: Source[Done, _], killSwitch: SharedKillSwitch)(
      implicit system: ActorSystem[_])
      extends RunningProjection {

    private val streamDone = source.run()

    /**
     * INTERNAL API
     *
     * Stop the projection if it's running.
     *
     * @return Future[Done] - the returned Future should return the stream materialized value.
     */
    @InternalApi
    override private[projection] def stop()(implicit ec: ExecutionContext): Future[Done] = {
      killSwitch.shutdown()
      streamDone
    }
  }

  override def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): Future[Done] = {
    val session = CassandraSessionRegistry(system).sessionFor(sessionConfigPath)
    val offsetStore = new CassandraOffsetStore(session)(system.executionContext)
    offsetStore.createKeyspaceAndTable()
  }

  override def initializeOffsetTable(system: ActorSystem[_]): CompletionStage[Done] = {
    import scala.compat.java8.FutureConverters._
    createOffsetTableIfNotExists()(system).toJava
  }
}
