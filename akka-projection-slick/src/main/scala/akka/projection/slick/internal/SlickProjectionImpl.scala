/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.ProjectionOffsetManagement
import akka.projection.ProjectionSettings
import akka.projection.RunningProjection
import akka.projection.internal.HandlerRecoveryImpl
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.AtLeastOnceSlickProjection
import akka.projection.slick.ExactlyOnceSlickProjection
import akka.projection.slick.GroupedSlickProjection
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
  sealed trait OffsetStrategy
  sealed trait WithRecoveryStrategy extends OffsetStrategy {
    def recoveryStrategy: Option[HandlerRecoveryStrategy]
  }
  final case class ExactlyOnce(recoveryStrategy: Option[HandlerRecoveryStrategy] = None) extends WithRecoveryStrategy
  final case class AtLeastOnce(
      afterEnvelopes: Option[Int] = None,
      orAfterDuration: Option[FiniteDuration] = None,
      recoveryStrategy: Option[HandlerRecoveryStrategy] = None)
      extends WithRecoveryStrategy

  sealed trait HandlerStrategy[Envelope] {
    def lifecycle: HandlerLifecycle
  }
  final case class SingleHandlerStrategy[Envelope](handler: SlickHandler[Envelope]) extends HandlerStrategy[Envelope] {
    override def lifecycle: HandlerLifecycle = handler
  }
  final case class GroupedHandlerStrategy[Envelope](
      handler: SlickHandler[immutable.Seq[Envelope]],
      afterEnvelopes: Option[Int] = None,
      orAfterDuration: Option[FiniteDuration] = None)
      extends HandlerStrategy[Envelope] {
    override def lifecycle: HandlerLifecycle = handler
  }
}

@InternalApi
private[projection] class SlickProjectionImpl[Offset, Envelope, P <: JdbcProfile](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    databaseConfig: DatabaseConfig[P],
    settingsOpt: Option[ProjectionSettings],
    val offsetStrategy: SlickProjectionImpl.OffsetStrategy,
    handlerStrategy: SlickProjectionImpl.HandlerStrategy[Envelope])
    extends SlickProjection[Envelope]
    with ExactlyOnceSlickProjection[Envelope]
    with AtLeastOnceSlickProjection[Envelope]
    with GroupedSlickProjection[Envelope] {

  import SlickProjectionImpl._

  override def withSettings(settings: ProjectionSettings): SlickProjectionImpl[Offset, Envelope, P] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      Option(settings),
      offsetStrategy,
      handlerStrategy)

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
      settingsOpt,
      offsetStrategy
        .asInstanceOf[AtLeastOnce]
        .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)),
      handlerStrategy)

  /**
   * Settings for GroupedSlickProjection
   */
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): SlickProjectionImpl[Offset, Envelope, P] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt,
      offsetStrategy,
      handlerStrategy
        .asInstanceOf[GroupedHandlerStrategy[Envelope]]
        .copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration)))

  /**
   * Settings for AtLeastOnceSlickProjection and ExactlyOnceSlickProjection
   */
  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): SlickProjectionImpl[Offset, Envelope, P] = {
    val newStrategy = offsetStrategy match {
      case s: ExactlyOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
      case s: AtLeastOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
    }
    new SlickProjectionImpl(projectionId, sourceProvider, databaseConfig, settingsOpt, newStrategy, handlerStrategy)
  }

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
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] =
    new InternalProjectionState(settingsOrDefaults).mappedSource()

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
  private class InternalProjectionState(settings: ProjectionSettings)(implicit system: ActorSystem[_]) {

    implicit val executionContext: ExecutionContext = system.executionContext

    val offsetStore = createOffsetStore()

    val killSwitch: SharedKillSwitch = KillSwitches.shared(projectionId.id)

    // TODO: add a LogSource for projection when we have a name and key
    private val logger = Logging(system.classicSystem, this.getClass)

    private[projection] def mappedSource(): Source[Done, _] = {

      import databaseConfig.profile.api._

      def applyUserRecovery(recoveryStrategy: HandlerRecoveryStrategy, firstOffset: Offset, lastOffset: Offset)(
          futureCallback: () => Future[Done]): Future[Done] =
        HandlerRecoveryImpl.applyUserRecovery[Offset](recoveryStrategy, firstOffset, lastOffset, logger, futureCallback)

      def processEnvelopeAndStoreOffsetInSameTransaction(
          handler: SlickHandler[Envelope],
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

        applyUserRecovery(recoveryStrategy, offset, offset) { () =>
          databaseConfig.db.run(txDBIO).map(_ => Done)
        }
      }

      def processEnvelopesAndStoreOffsetInSameTransaction(
          handler: SlickHandler[immutable.Seq[Envelope]],
          recoveryStrategy: HandlerRecoveryStrategy,
          envelopes: immutable.Seq[Envelope]): Future[Done] = {
        val firstOffset = sourceProvider.extractOffset(envelopes.head)
        val lastOffset = sourceProvider.extractOffset(envelopes.last)
        // run user function and offset storage on the same transaction
        // any side-effect in user function is at-least-once
        val txDBIO =
          offsetStore
            .saveOffset(projectionId, lastOffset)
            .flatMap(_ => handler.process(envelopes))
            .transactionally

        applyUserRecovery(recoveryStrategy, firstOffset, lastOffset) { () =>
          databaseConfig.db.run(txDBIO).map(_ => Done)
        }
      }

      def processEnvelope(
          handler: SlickHandler[Envelope],
          recoveryStrategy: HandlerRecoveryStrategy,
          env: Envelope,
          offset: Offset): Future[Done] = {
        // user function in one transaction (may be composed of several DBIOAction)
        val dbio = handler.process(env).transactionally
        applyUserRecovery(recoveryStrategy, offset, offset) { () =>
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
        offsetStrategy match {
          case ExactlyOnce(recoveryStrategyOpt) =>
            val recoveryStrategy = recoveryStrategyOpt.getOrElse(settings.recoveryStrategy)
            handlerStrategy match {
              case SingleHandlerStrategy(handler) =>
                Flow[Envelope]
                  .mapAsync(1)(env => processEnvelopeAndStoreOffsetInSameTransaction(handler, recoveryStrategy, env))

              case grouped: GroupedHandlerStrategy[Envelope] =>
                val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
                val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
                Flow[Envelope]
                  .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
                  .filterNot(_.isEmpty)
                  .mapAsync(parallelism = 1) { group =>
                    processEnvelopesAndStoreOffsetInSameTransaction(grouped.handler, recoveryStrategy, group)
                  }
            }

          case AtLeastOnce(afterEnvelopesOpt, orAfterDurationOpt, recoveryStrategyOpt) =>
            val afterEnvelopes = afterEnvelopesOpt.getOrElse(settings.saveOffsetAfterEnvelopes)
            val orAfterDuration = orAfterDurationOpt.getOrElse(settings.saveOffsetAfterDuration)
            val recoveryStrategy = recoveryStrategyOpt.getOrElse(settings.recoveryStrategy)

            val handler = handlerStrategy match {
              case SingleHandlerStrategy(handler) => handler
              case _                              =>
                // not possible
                throw new IllegalStateException("Unsupported combination of atLeastOnce and grouped")
            }

            if (afterEnvelopes == 1)
              // optimization of general AtLeastOnce case, still separate transactions for processEnvelope
              // and storeOffset
              Flow[Envelope].mapAsync(1) { env =>
                val offset = sourceProvider.extractOffset(env)
                processEnvelope(handler, recoveryStrategy, env, offset).flatMap(_ => storeOffset(offset))
              }
            else
              Flow[Envelope]
                .mapAsync(1) { env =>
                  val offset = sourceProvider.extractOffset(env)
                  processEnvelope(handler, recoveryStrategy, env, offset).map(_ => offset)
                }
                .groupedWithin(afterEnvelopes, orAfterDuration)
                .collect { case grouped if grouped.nonEmpty => grouped.last }
                .mapAsync(parallelism = 1)(storeOffset)
        }

      val composedSource: Source[Done, NotUsed] =
        Source
          .futureSource(handlerStrategy.lifecycle.tryStart().flatMap(_ => sourceProvider.source(readOffsets)))
          .via(killSwitch.flow)
          .via(handlerFlow)
          .mapMaterializedValue(_ => NotUsed)

      RunningProjection.stopHandlerOnTermination(composedSource, () => handlerStrategy.lifecycle.tryStop())
    }

    private[projection] def newRunningInstance(): RunningProjection =
      new SlickRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), this)
  }

  private class SlickRunningProjection(source: Source[Done, _], projectionState: InternalProjectionState)(
      implicit system: ActorSystem[_])
      extends RunningProjection
      with ProjectionOffsetManagement[Offset] {

    private implicit val executionContext: ExecutionContext = system.executionContext

    private val streamDone = source.run()

    override def stop(): Future[Done] = {
      projectionState.killSwitch.shutdown()
      streamDone
    }

    // ProjectionOffsetManagement
    override def getOffset(): Future[Option[Offset]] = {
      projectionState.offsetStore.readOffset(projectionId)
    }

    // ProjectionOffsetManagement
    override def setOffset(offset: Option[Offset]): Future[Done] = {
      offset match {
        case Some(o) =>
          val dbio = projectionState.offsetStore.saveOffset(projectionId, o)
          databaseConfig.db.run(dbio).map(_ => Done)
        case None =>
          val dbio = projectionState.offsetStore.clearOffset(projectionId)
          databaseConfig.db.run(dbio).map(_ => Done)
      }
    }
  }

  private def createOffsetStore()(implicit system: ActorSystem[_]) =
    new SlickOffsetStore(databaseConfig.db, databaseConfig.profile, SlickSettings(system))

  override def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): Future[Done] = {
    createOffsetStore().createIfNotExists
  }
}
