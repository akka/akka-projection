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
import akka.event.LoggingAdapter
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.RunningProjection
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
  case object AtMostOnce extends Strategy
  final case class AtLeastOnce(afterEnvelopes: Option[Int] = None, orAfterDuration: Option[FiniteDuration] = None)
      extends Strategy
  import HandlerRecoveryStrategy.Internal._

  /**
   * Prevent usage of retries for at-most-once.
   */
  private class AtMostOnceHandler[Envelope](delegate: Handler[Envelope], logger: LoggingAdapter)
      extends Handler[Envelope] {
    override def process(envelope: Envelope): Future[Done] =
      delegate.process(envelope)

    override def onFailure(envelope: Envelope, throwable: Throwable): HandlerRecoveryStrategy = {
      super.onFailure(envelope, throwable) match {
        case _: RetryAndFail =>
          logger.warning(
            "RetryAndFail not supported for atMostOnce projection because it would call the handler more than once. " +
            "You should change to `HandlerRecoveryStrategy.fail`.")
          HandlerRecoveryStrategy.fail
        case _: RetryAndSkip =>
          logger.warning(
            "RetryAndSkip not supported for atMostOnce projection because it would call the handler more than once. " +
            "You should change to `HandlerRecoveryStrategy.skip`.")
          HandlerRecoveryStrategy.skip
        case other => other
      }
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraProjectionImpl[Offset, Envelope](
    override val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    strategy: CassandraProjectionImpl.Strategy,
    settingsOpt: Option[ProjectionSettings],
    handler: Handler[Envelope])
    extends javadsl.CassandraProjection[Envelope]
    with scaladsl.CassandraProjection[Envelope]
    with javadsl.AtLeastOnceCassandraProjection[Envelope]
    with scaladsl.AtLeastOnceCassandraProjection[Envelope] {
  import CassandraProjectionImpl._
  import HandlerRecoveryImpl.applyUserRecovery

  override def withSettings(settings: ProjectionSettings): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(projectionId, sourceProvider, strategy, Option(settings), handler)

  // Settings for AtLeastOnceCassandraProjection
  override def withSaveOffsetAfterEnvelopes(afterEnvelopes: Int): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      strategy.asInstanceOf[AtLeastOnce].copy(afterEnvelopes = Some(afterEnvelopes)),
      settingsOpt,
      handler)

  override def withSaveOffsetAfterDuration(afterDuration: FiniteDuration): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      strategy.asInstanceOf[AtLeastOnce].copy(orAfterDuration = Some(afterDuration)),
      settingsOpt,
      handler)

  /**
   * Java API
   */
  override def withSaveOffsetAfterDuration(
      afterDuration: java.time.Duration): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      strategy.asInstanceOf[AtLeastOnce].copy(orAfterDuration = Some(afterDuration.toScala)),
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
          .map(envelope => sourceProvider.extractOffset(envelope) -> envelope)
          .mapMaterializedValue(_ => NotUsed)

      val handlerFlow: Flow[(Offset, Envelope), Offset, NotUsed] =
        Flow[(Offset, Envelope)].mapAsync(parallelism = 1) {
          case (offset, envelope) =>
            applyUserRecovery(handler, envelope, offset, logger, () => handler.process(envelope))
              .map(_ => offset)
        }

      val composedSource: Source[Done, NotUsed] = strategy match {
        case AtLeastOnce(afterEnvelopesOpt, orAfterDurationOpt) =>
          val afterEnvelopes = afterEnvelopesOpt.getOrElse(settings.saveOffsetAfterEnvelopes)
          val orAfterDuration = orAfterDurationOpt.getOrElse(settings.saveOffsetAfterDuration)

          if (afterEnvelopes == 1)
            // optimization of general AtLeastOnce case
            source.via(handlerFlow).mapAsync(1) { offset =>
              offsetStore.saveOffset(projectionId, offset)
            }
          else
            source
              .via(handlerFlow)
              .groupedWithin(afterEnvelopes, orAfterDuration)
              .collect { case grouped if grouped.nonEmpty => grouped.last }
              .mapAsync(parallelism = 1) { offset =>
                offsetStore.saveOffset(projectionId, offset)
              }
        case AtMostOnce =>
          // prevent usage of retries for at-most-once
          val atMostOnceHandler = new AtMostOnceHandler(handler, logger)
          source
            .mapAsync(parallelism = 1) {
              case (offset, envelope) =>
                offsetStore
                  .saveOffset(projectionId, offset)
                  .flatMap(
                    _ =>
                      applyUserRecovery(
                        atMostOnceHandler,
                        envelope,
                        offset,
                        logger,
                        () => atMostOnceHandler.process(envelope)))
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
