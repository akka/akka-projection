/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

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
import akka.projection.internal.HandlerRecoveryImpl
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.AtLeastOnceSlickProjection
import akka.projection.slick.ExactlyOnceSlickProjection
import akka.projection.slick.SlickHandler
import akka.projection.slick.SlickProjection
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

@InternalApi
private[projection] object SlickProjectionImpl {
  sealed trait Strategy
  sealed trait WithRecoveryStrategy extends Strategy {
    def recoveryStrategy: Option[HandlerRecoveryStrategy]
  }
  final case class ExactlyOnce(recoveryStrategy: Option[HandlerRecoveryStrategy] = None) extends WithRecoveryStrategy
  final case class AtLeastOnce(
      afterEnvelopes: Option[Int] = None,
      orAfterDuration: Option[FiniteDuration] = None,
      recoveryStrategy: Option[HandlerRecoveryStrategy] = None)
      extends WithRecoveryStrategy
}

@InternalApi
private[projection] class SlickProjectionImpl[Offset, Envelope, P <: JdbcProfile](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    databaseConfig: DatabaseConfig[P],
    val strategy: SlickProjectionImpl.Strategy,
    settingsOpt: Option[ProjectionSettings],
    handler: SlickHandler[Envelope])
    extends SlickProjection[Envelope]
    with ExactlyOnceSlickProjection[Envelope]
    with AtLeastOnceSlickProjection[Envelope] {

  import SlickProjectionImpl._

  override def withSettings(settings: ProjectionSettings): SlickProjectionImpl[Offset, Envelope, P] =
    new SlickProjectionImpl(projectionId, sourceProvider, databaseConfig, strategy, Option(settings), handler)

  /**
   * Settings for AtLeastOnceSlickProjection
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): SlickProjectionImpl[Offset, Envelope, P] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      strategy
        .asInstanceOf[AtLeastOnce]
        .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)),
      settingsOpt,
      handler)

  /**
   * Java API
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: java.time.Duration): SlickProjectionImpl[Offset, Envelope, P] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      atLeastOnceStrategy
        .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration.toScala)),
      settingsOpt,
      handler)

  /**
   * Settings for AtLeastOnceSlickProjection and ExactlyOnceSlickProjection
   */
  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): SlickProjectionImpl[Offset, Envelope, P] = {
    val newStrategy = strategy match {
      case s: ExactlyOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
      case s: AtLeastOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
    }
    new SlickProjectionImpl(projectionId, sourceProvider, databaseConfig, newStrategy, settingsOpt, handler)
  }

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection =
    new InternalProjectionState(
      offsetStore = new SlickOffsetStore(databaseConfig.db, databaseConfig.profile),
      settings = settingsOrDefaults).newRunningInstance()

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] =
    new InternalProjectionState(
      offsetStore = new SlickOffsetStore(databaseConfig.db, databaseConfig.profile),
      settings = settingsOrDefaults).mappedSource()

  /*
   * Build the final ProjectionSettings to use, if currently set to None fallback to values in config file
   */
  private def settingsOrDefaults(implicit system: ActorSystem[_]): ProjectionSettings =
    settingsOpt.getOrElse(ProjectionSettings(system))

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  private class InternalProjectionState(offsetStore: SlickOffsetStore[P], settings: ProjectionSettings)(
      implicit system: ActorSystem[_]) {

    private val killSwitch = KillSwitches.shared(projectionId.id)

    private[projection] def mappedSource(): Source[Done, _] = {

      import databaseConfig.profile.api._

      // TODO: add a LogSource for projection when we have a name and key
      val logger = Logging(system.classicSystem, this.getClass)

      implicit val dispatcher = system.classicSystem.dispatcher

      def applyUserRecovery(recoveryStrategy: HandlerRecoveryStrategy, offset: Offset)(
          futureCallback: () => Future[Done]): Future[Done] =
        HandlerRecoveryImpl.applyUserRecovery[Offset](recoveryStrategy, offset, logger, futureCallback)

      def processEnvelopeAndStoreOffsetInSameTransaction(
          recoveryStrategy: HandlerRecoveryStrategy,
          env: Envelope): Future[Done] = {
        val offset = sourceProvider.extractOffset(env)
        // run user function and offset storage on the same transaction
        // any side-effect in user function is at-least-once
        val txDBIO =
          offsetStore
            .saveOffset(projectionId, offset)
            .flatMap(_ => handler.process(env))
            .transactionally

        applyUserRecovery(recoveryStrategy, offset) { () =>
          databaseConfig.db.run(txDBIO).map(_ => Done)
        }
      }

      def processEnvelope(recoveryStrategy: HandlerRecoveryStrategy, env: Envelope, offset: Offset): Future[Done] = {
        // user function in one transaction (may be composed of several DBIOAction)
        val dbio = handler.process(env).transactionally
        applyUserRecovery(recoveryStrategy, offset) { () =>
          databaseConfig.db.run(dbio).map(_ => Done)
        }
      }

      def storeOffset(offset: Offset): Future[Done] = {
        // only one DBIOAction, no need for transactionally
        val dbio = offsetStore.saveOffset(projectionId, offset)
        databaseConfig.db.run(dbio).map(_ => Done)
      }

      // -------------------------------------------------------
      // finally build the source with all parts wired
      val readOffsets = () => {
        val offsetsF = offsetStore.readOffset(projectionId)
        offsetsF.foreach { offset => logger.debug("Starting projection [{}] from offset [{}]", projectionId, offset) }
        offsetsF
      }

      val handlerFlow: Flow[Envelope, Done, _] =
        strategy match {
          case ExactlyOnce(recoveryStrategyOpt) =>
            val recoveryStrategy = recoveryStrategyOpt.getOrElse(settings.recoveryStrategy)
            Flow[Envelope]
              .mapAsync(1)(env => processEnvelopeAndStoreOffsetInSameTransaction(recoveryStrategy, env))

          case AtLeastOnce(afterEnvelopesOpt, orAfterDurationOpt, recoveryStrategyOpt) =>
            val afterEnvelopes = afterEnvelopesOpt.getOrElse(settings.saveOffsetAfterEnvelopes)
            val orAfterDuration = orAfterDurationOpt.getOrElse(settings.saveOffsetAfterDuration)
            val recoveryStrategy = recoveryStrategyOpt.getOrElse(settings.recoveryStrategy)

            if (afterEnvelopes == 1)
              // optimization of general AtLeastOnce case, still separate transactions for processEnvelope
              // and storeOffset
              Flow[Envelope].mapAsync(1) { env =>
                val offset = sourceProvider.extractOffset(env)
                processEnvelope(recoveryStrategy, env, offset).flatMap(_ => storeOffset(offset))
              }
            else
              Flow[Envelope]
                .mapAsync(1) { env =>
                  val offset = sourceProvider.extractOffset(env)
                  processEnvelope(recoveryStrategy, env, offset).map(_ => offset)
                }
                .groupedWithin(afterEnvelopes, orAfterDuration)
                .collect { case grouped if grouped.nonEmpty => grouped.last }
                .mapAsync(parallelism = 1)(storeOffset)
        }

      val composedSource: Source[Done, NotUsed] =
        Source
          .futureSource(handler.tryStart().flatMap(_ => sourceProvider.source(readOffsets)))
          .via(killSwitch.flow)
          .via(handlerFlow)
          .mapMaterializedValue(_ => NotUsed)

      RunningProjection.stopHandlerOnTermination(composedSource, () => handler.tryStop())
    }

    private[projection] def newRunningInstance(): RunningProjection =
      new SlickRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), killSwitch)
  }

  private class SlickRunningProjection(source: Source[Done, _], killSwitch: SharedKillSwitch)(
      implicit system: ActorSystem[_])
      extends RunningProjection {

    private val streamDone = source.run()

    /**
     * INTERNAL API
     *
     * Stop the projection if it's running.
     * @return Future[Done] - the returned Future should return the stream materialized value.
     */
    @InternalApi
    override private[projection] def stop()(implicit ec: ExecutionContext): Future[Done] = {
      killSwitch.shutdown()
      streamDone
    }
  }

  override def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): Future[Done] = {
    val offsetStore = new SlickOffsetStore(databaseConfig.db, databaseConfig.profile)
    offsetStore.createIfNotExists
  }
}
