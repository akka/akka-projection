/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import java.util.concurrent.CompletionStage

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._
import scala.util.control.NonFatal

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
import akka.projection.RunningProjection.AbortProjectionException
import akka.projection.StatusObserver
import akka.projection.StrictRecoveryStrategy
import akka.projection.cassandra.javadsl
import akka.projection.cassandra.scaladsl
import akka.projection.internal.HandlerRecoveryImpl
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
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
    handlerStrategy: CassandraProjectionImpl.HandlerStrategy[Envelope],
    override val statusObserver: StatusObserver[Envelope])
    extends javadsl.CassandraProjection[Envelope]
    with scaladsl.CassandraProjection[Envelope]
    with javadsl.AtLeastOnceCassandraProjection[Envelope]
    with scaladsl.AtLeastOnceCassandraProjection[Envelope]
    with javadsl.GroupedCassandraProjection[Envelope]
    with scaladsl.GroupedCassandraProjection[Envelope]
    with javadsl.AtMostOnceCassandraProjection[Envelope]
    with scaladsl.AtMostOnceCassandraProjection[Envelope] {

  import CassandraProjectionImpl._

  private def copy(
      settingsOpt: Option[ProjectionSettings] = this.settingsOpt,
      offsetStrategy: OffsetStrategy = this.offsetStrategy,
      handlerStrategy: CassandraProjectionImpl.HandlerStrategy[Envelope] = this.handlerStrategy,
      statusObserver: StatusObserver[Envelope] = this.statusObserver): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt,
      offsetStrategy,
      handlerStrategy,
      statusObserver)

  override def withSettings(settings: ProjectionSettings): CassandraProjectionImpl[Offset, Envelope] =
    copy(settingsOpt = Option(settings))

  /**
   * Settings for AtLeastOnceCassandraProjection
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): CassandraProjectionImpl[Offset, Envelope] =
    copy(offsetStrategy = atLeastOnceStrategy
      .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)))

  /**
   * Java API
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: java.time.Duration): CassandraProjectionImpl[Offset, Envelope] =
    copy(offsetStrategy = atLeastOnceStrategy
      .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration.toScala)))

  /**
   * Settings for GroupedCassandraProjection
   */
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): CassandraProjectionImpl[Offset, Envelope] =
    copy(handlerStrategy = handlerStrategy
      .asInstanceOf[GroupedHandlerStrategy[Envelope]]
      .copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration)))

  /**
   * Java API
   */
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: java.time.Duration): CassandraProjectionImpl[Offset, Envelope] =
    copy(handlerStrategy = handlerStrategy
      .asInstanceOf[GroupedHandlerStrategy[Envelope]]
      .copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration.toScala)))

  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): CassandraProjectionImpl[Offset, Envelope] =
    copy(offsetStrategy = atLeastOnceStrategy.copy(recoveryStrategy = Some(recoveryStrategy)))

  /**
   * Settings for AtMostOnceCassandraProjection
   */
  override def withRecoveryStrategy(
      recoveryStrategy: StrictRecoveryStrategy): CassandraProjectionImpl[Offset, Envelope] =
    copy(offsetStrategy = atMostOnceStrategy.copy(recoveryStrategy = Some(recoveryStrategy)))

  override def withStatusObserver(observer: StatusObserver[Envelope]): CassandraProjectionImpl[Offset, Envelope] =
    copy(statusObserver = observer)

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection = {
    new InternalProjectionState(settingsOrDefaults).newRunningInstance()
  }

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  @InternalApi
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] = {
    new InternalProjectionState(settingsOrDefaults).mappedSource()
  }

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
    // FIXME maybe use the session-dispatcher config
    implicit val ec: ExecutionContext = system.executionContext

    private val logger = Logging(system.classicSystem, this.getClass)

    val offsetStore = new CassandraOffsetStore(system)

    val killSwitch: SharedKillSwitch = KillSwitches.shared(projectionId.id)
    val abort: Promise[Done] = Promise()

    private[projection] def mappedSource(): Source[Done, _] = {
      val readOffsets = () => offsetStore.readOffset(projectionId)

      statusObserver.started(projectionId)

      val source: Source[(Offset, Envelope), NotUsed] =
        Source
          .futureSource(handlerStrategy.lifecycle.tryStart().flatMap(_ => sourceProvider.source(readOffsets)))
          .via(killSwitch.flow)
          .map(env => (sourceProvider.extractOffset(env), env))
          .filter {
            case (offset, _) =>
              sourceProvider.verifyOffset(offset) match {
                case VerificationSuccess => true
                case VerificationFailure(reason) =>
                  logger.warning(
                    "Source provider instructed projection to skip offset [{}] with reason: {}",
                    offset,
                    reason)
                  false
              }
          }
          .mapMaterializedValue(_ => NotUsed)

      def handlerFlow(
          recoveryStrategy: HandlerRecoveryStrategy): Flow[(Offset, Envelope), (Offset, Envelope), NotUsed] =
        handlerStrategy match {
          case SingleHandlerStrategy(handler) =>
            val handlerRecovery =
              HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

            Flow[(Offset, Envelope)].mapAsync(parallelism = 1) {
              case elem @ (offset, envelope) =>
                handlerRecovery
                  .applyRecovery(envelope, offset, offset, abort.future, () => handler.process(envelope))
                  .map(_ => elem)
            }

          case grouped: GroupedHandlerStrategy[Envelope] =>
            val groupAfterEnvelopes = grouped.afterEnvelopes.getOrElse(settings.groupAfterEnvelopes)
            val groupAfterDuration = grouped.orAfterDuration.getOrElse(settings.groupAfterDuration)
            val handlerRecovery =
              HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)

            Flow[(Offset, Envelope)]
              .groupedWithin(groupAfterEnvelopes, groupAfterDuration)
              .filterNot(_.isEmpty)
              .mapAsync(parallelism = 1) { group =>
                val (firstOffset, firstEnvelope) = group.head
                val (lastOffset, lastEnvelope) = group.last
                val envelopes = group.map { case (_, env) => env }
                handlerRecovery
                  .applyRecovery(
                    firstEnvelope,
                    firstOffset,
                    lastOffset,
                    abort.future,
                    () => grouped.handler.process(envelopes))
                  .map(_ => lastOffset -> lastEnvelope)
              }
        }

      def reportProgress[T](after: Future[T], env: Envelope): Future[T] = {
        after.map { done =>
          try {
            statusObserver.progress(projectionId, env)
          } catch {
            case NonFatal(_) => // ignore
          }
          done
        }
      }

      val composedSource: Source[Done, NotUsed] = offsetStrategy match {
        case AtLeastOnce(afterEnvelopesOpt, orAfterDurationOpt, recoveryStrategyOpt) =>
          val afterEnvelopes = afterEnvelopesOpt.getOrElse(settings.saveOffsetAfterEnvelopes)
          val orAfterDuration = orAfterDurationOpt.getOrElse(settings.saveOffsetAfterDuration)
          val recoveryStrategy = recoveryStrategyOpt.getOrElse(settings.recoveryStrategy)

          if (afterEnvelopes == 1)
            // optimization of general AtLeastOnce case
            source.via(handlerFlow(recoveryStrategy)).mapAsync(1) {
              case (offset, envelope) =>
                reportProgress(offsetStore.saveOffset(projectionId, offset), envelope)
            }
          else
            source
              .via(handlerFlow(recoveryStrategy))
              .groupedWithin(afterEnvelopes, orAfterDuration)
              .collect { case grouped if grouped.nonEmpty => grouped.last }
              .mapAsync(parallelism = 1) {
                case (offset, envelope) =>
                  reportProgress(offsetStore.saveOffset(projectionId, offset), envelope)
              }

        case AtMostOnce(recoveryStrategyOpt) =>
          val recoveryStrategy = recoveryStrategyOpt.getOrElse(settings.recoveryStrategy)
          val handlerRecovery =
            HandlerRecoveryImpl[Offset, Envelope](projectionId, recoveryStrategy, logger, statusObserver)
          val handler = handlerStrategy match {
            case SingleHandlerStrategy(handler) => handler
            case _                              =>
              // not possible
              throw new IllegalStateException("Unsupported combination of atMostOnce and grouped")
          }

          source
            .mapAsync(parallelism = 1) {
              case (offset, envelope) =>
                reportProgress(
                  offsetStore
                    .saveOffset(projectionId, offset)
                    .flatMap(_ =>
                      handlerRecovery
                        .applyRecovery(envelope, offset, offset, abort.future, () => handler.process(envelope))),
                  envelope)
            }
            .map(_ => Done)
      }

      RunningProjection.stopHandlerOnTermination(
        composedSource,
        projectionId,
        handlerStrategy.lifecycle,
        statusObserver)
    }

    private[projection] def newRunningInstance(): RunningProjection = {
      new CassandraRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), this)
    }
  }

  private class CassandraRunningProjection(source: Source[Done, _], projectionState: InternalProjectionState)(
      implicit system: ActorSystem[_])
      extends RunningProjection
      with ProjectionOffsetManagement[Offset] {

    private val streamDone = source.run()

    override def stop(): Future[Done] = {
      projectionState.killSwitch.shutdown()
      // if the handler is retrying it will be aborted by this,
      // otherwise the stream would not be completed by the killSwitch until after all retries
      projectionState.abort.failure(AbortProjectionException)
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
          projectionState.offsetStore.saveOffset(projectionId, o)
        case None =>
          projectionState.offsetStore.clearOffset(projectionId)
      }
    }

  }

  override def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): Future[Done] = {
    val offsetStore = new CassandraOffsetStore(system)
    offsetStore.createKeyspaceAndTable()
  }

  override def initializeOffsetTable(system: ActorSystem[_]): CompletionStage[Done] = {
    import scala.compat.java8.FutureConverters._
    createOffsetTableIfNotExists()(system).toJava
  }

}
