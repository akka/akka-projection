/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.event.Logging
import akka.projection.Success
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.RunningProjection
import akka.projection.SkipOffset
import akka.projection.StrictRecoveryStrategy
import akka.projection.cassandra.javadsl
import akka.projection.cassandra.scaladsl
import akka.projection.internal.HandlerRecoveryImpl
import akka.projection.scaladsl.Handler
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
  sealed trait Strategy
  final case class AtMostOnce(recoveryStrategy: Option[StrictRecoveryStrategy] = None) extends Strategy
  final case class AtLeastOnce(
      afterEnvelopes: Option[Int] = None,
      orAfterDuration: Option[FiniteDuration] = None,
      recoveryStrategy: Option[HandlerRecoveryStrategy] = None)
      extends Strategy
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraProjectionImpl[Offset, Envelope](
    override val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    val strategy: CassandraProjectionImpl.Strategy,
    settingsOpt: Option[ProjectionSettings],
    handler: Handler[Envelope])
    extends javadsl.CassandraProjection[Envelope]
    with scaladsl.CassandraProjection[Envelope]
    with javadsl.AtLeastOnceCassandraProjection[Envelope]
    with scaladsl.AtLeastOnceCassandraProjection[Envelope]
    with javadsl.AtMostOnceCassandraProjection[Envelope]
    with scaladsl.AtMostOnceCassandraProjection[Envelope] {

  import CassandraProjectionImpl._
  import HandlerRecoveryImpl.applyUserRecovery

  override def withSettings(settings: ProjectionSettings): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(projectionId, sourceProvider, strategy, Option(settings), handler)

  /**
   * Settings for AtLeastOnceCassandraProjection
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      atLeastOnceStrategy
        .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)),
      settingsOpt,
      handler)

  /**
   * Java API
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: java.time.Duration): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      atLeastOnceStrategy
        .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration.toScala)),
      settingsOpt,
      handler)

  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      atLeastOnceStrategy.copy(recoveryStrategy = Some(recoveryStrategy)),
      settingsOpt,
      handler)

  /**
   * Settings for AtMostOnceCassandraProjection
   */
  override def withRecoveryStrategy(
      recoveryStrategy: StrictRecoveryStrategy): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      atMostOnceStrategy.copy(recoveryStrategy = Some(recoveryStrategy)),
      settingsOpt,
      handler)

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  override private[projection] def run()(implicit systemProvider: ClassicActorSystemProvider): RunningProjection =
    new InternalProjectionState(settingsOrDefaults).newRunningInstance()

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  @InternalApi
  override private[projection] def mappedSource()(
      implicit systemProvider: ClassicActorSystemProvider): Source[Done, _] =
    new InternalProjectionState(settingsOrDefaults).mappedSource()

  /*
   * Build the final ProjectionSettings to use, if currently set to None fallback to values in config file
   */
  private def settingsOrDefaults(implicit systemProvider: ClassicActorSystemProvider): ProjectionSettings =
    settingsOpt.getOrElse(ProjectionSettings(systemProvider))

  // FIXME make the sessionConfigPath configurable so that it can use same session as akka.persistence.cassandra or alpakka.cassandra
  private val sessionConfigPath = "akka.projection.cassandra"

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  private class InternalProjectionState(settings: ProjectionSettings)(
      implicit systemProvider: ClassicActorSystemProvider) {

    private val killSwitch = KillSwitches.shared(projectionId.id)

    private[projection] def mappedSource(): Source[Done, _] = {
      val system: ActorSystem = systemProvider.classicSystem
      // FIXME maybe use the session-dispatcher config
      implicit val ec: ExecutionContext = system.dispatcher

      val logger = Logging(systemProvider.classicSystem, this.getClass)

      // FIXME session lookup could be moved to CassandraOffsetStore if that's better
      val session = CassandraSessionRegistry(system).sessionFor(sessionConfigPath)
      val offsetStore = new CassandraOffsetStore(session)
      val readOffsets = () => offsetStore.readOffset(projectionId)

      val source: Source[(Offset, Envelope), NotUsed] =
        Source
          .futureSource(handler.tryStart().flatMap(_ => sourceProvider.source(readOffsets)))
          .via(killSwitch.flow)
          .mapConcat { env =>
            val offset = sourceProvider.extractOffset(env)
            sourceProvider.verifyOffset(offset) match {
              case Success => List(offset -> env)
              case SkipOffset(reason) =>
                logger.warning("Source provider instructed projection to skip record with reason: {}", reason)
                Nil
            }
          }
          .mapMaterializedValue(_ => NotUsed)

      def handlerFlow(recoveryStrategy: HandlerRecoveryStrategy): Flow[(Offset, Envelope), Offset, NotUsed] =
        Flow[(Offset, Envelope)].mapAsync(parallelism = 1) {
          case (offset, envelope) =>
            applyUserRecovery(recoveryStrategy, offset, logger, () => handler.process(envelope))
              .map(_ => offset)
        }

      val composedSource: Source[Done, NotUsed] = strategy match {
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

          source
            .mapAsync(parallelism = 1) {
              case (offset, envelope) =>
                offsetStore
                  .saveOffset(projectionId, offset)
                  .flatMap(_ => applyUserRecovery(recoveryStrategy, offset, logger, () => handler.process(envelope)))
            }
            .map(_ => Done)
      }

      composedSource.via(RunningProjection.stopHandlerWhenFailed(() => handler.tryStop()))
    }

    private[projection] def newRunningInstance(): RunningProjection = {
      new CassandraRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), killSwitch)
    }
  }

  private class CassandraRunningProjection(source: Source[Done, _], killSwitch: SharedKillSwitch)(
      implicit systemProvider: ClassicActorSystemProvider)
      extends RunningProjection {

    private val streamDone = source.run()
    private val allStopped: Future[Done] =
      RunningProjection.stopHandlerWhenStreamCompletedNormally(streamDone, () => handler.tryStop())(
        systemProvider.classicSystem.dispatcher)

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
      allStopped
    }
  }

  override def createOffsetTableIfNotExists()(implicit systemProvider: ClassicActorSystemProvider): Future[Done] = {
    val system = systemProvider.classicSystem
    val session = CassandraSessionRegistry(system).sessionFor(sessionConfigPath)
    val offsetStore = new CassandraOffsetStore(session)(system.dispatcher)
    offsetStore.createKeyspaceAndTable()
  }

  override def initializeOffsetTable(systemProvider: ClassicActorSystemProvider): CompletionStage[Done] = {
    import scala.compat.java8.FutureConverters._
    createOffsetTableIfNotExists()(systemProvider).toJava
  }
}
