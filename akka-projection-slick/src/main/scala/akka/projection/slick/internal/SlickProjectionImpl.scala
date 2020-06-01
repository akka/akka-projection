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
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
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

      implicit val executionContext: ExecutionContext = system.executionContext

      def applyUserRecovery(recoveryStrategy: HandlerRecoveryStrategy, firstOffset: Offset, lastOffset: Offset)(
          futureCallback: () => Future[Done]): Future[Done] =
        HandlerRecoveryImpl.applyUserRecovery[Offset](recoveryStrategy, firstOffset, lastOffset, logger, futureCallback)

      def processEnvelopeAndStoreOffsetInSameTransaction(
          handler: SlickHandler[Envelope],
          recoveryStrategy: HandlerRecoveryStrategy,
          offset: Offset,
          env: Envelope): Future[Done] = {
        applyUserRecovery(recoveryStrategy, offset, offset) { () =>
          val handlerAction = handler.process(env)
          sourceProvider.verifyOffset(offset) match {
            case VerificationSuccess =>
              // run user function and offset storage on the same transaction
              // any side-effect in user function is at-least-once
              val txDBIO = offsetStore.saveOffset(projectionId, offset).flatMap(_ => handlerAction).transactionally
              databaseConfig.db.run(txDBIO).map(_ => Done)
            case VerificationFailure(reason) =>
              logger.warning(
                "The offset failed source provider verification after the envelope was processed. " +
                "The transaction will not be executed. Skipping envelope with reason: {}",
                reason)
              Future.successful(Done)
          }
        }
      }

      def processEnvelopesAndStoreOffsetInSameTransaction(
          handler: SlickHandler[immutable.Seq[Envelope]],
          recoveryStrategy: HandlerRecoveryStrategy,
          envsAndOffsets: immutable.Seq[(Offset, Envelope)]): Future[Done] = {
        val firstOffset = envsAndOffsets.head._1
        val lastOffset = envsAndOffsets.last._1
        val envelopes = envsAndOffsets.map(_._2)
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

      val offsetFlow: Flow[Envelope, (Offset, Envelope), NotUsed] = Flow[Envelope]
        .mapConcat { env =>
          val offset = sourceProvider.extractOffset(env)
          sourceProvider.verifyOffset(offset) match {
            case VerificationSuccess => List(offset -> env)
            case VerificationFailure(reason) =>
              logger.warning("Source provider instructed projection to skip envelope with reason: {}", reason)
              Nil
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
                offsetFlow
                  .mapAsync(1) {
                    case (offset, env) =>
                      processEnvelopeAndStoreOffsetInSameTransaction(handler, recoveryStrategy, offset, env)
                  }

              case grouped: GroupedHandlerStrategy[Envelope] =>
                val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
                val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
                offsetFlow
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
              offsetFlow.mapAsync(1) {
                case (offset, env) =>
                  processEnvelope(handler, recoveryStrategy, env, offset).flatMap(_ => storeOffset(offset))
              }
            else
              offsetFlow
                .mapAsync(1) {
                  case (offset, env) =>
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
