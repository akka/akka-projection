/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.RunningProjection.AbortProjectionException
import akka.projection.RunningProjectionManagement
import akka.projection.StatusObserver
import akka.projection.eventsourced.EventEnvelope
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.AtMostOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.InternalProjection
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.ProjectionContextImpl
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.SettingsImpl
import akka.projection.javadsl
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.projection.r2dbc.internal.R2dbcProjectionImpl.envToString
import akka.projection.r2dbc.internal.R2dbcProjectionImpl.log
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcSession
import akka.projection.scaladsl
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.RestartSettings
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Source
import io.r2dbc.spi.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object R2dbcProjectionImpl {
  val log: Logger = LoggerFactory.getLogger(classOf[R2dbcProjectionImpl[_, _]])

  private val FutureDone: Future[Done] = Future.successful(Done)

  private[projection] def createOffsetStore(
      projectionId: ProjectionId,
      settings: R2dbcProjectionSettings,
      connectionFactory: ConnectionFactory)(implicit system: ActorSystem[_]) = {
    val r2dbcExecutor = new R2dbcExecutor(connectionFactory, log)(system.executionContext, system)
    new R2dbcOffsetStore(projectionId, system, settings, r2dbcExecutor)
  }

  private[projection] def adaptedHandlerForExactlyOnce[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerFactory: () => R2dbcHandler[Envelope],
      offsetStore: R2dbcOffsetStore,
      r2dbcExecutor: R2dbcExecutor)(implicit ec: ExecutionContext, system: ActorSystem[_]): () => Handler[Envelope] = {
    () =>

      new AdaptedR2dbcHandler(handlerFactory()) {
        override def process(envelope: Envelope): Future[Done] = {
          if (offsetStore.isEnvelopeDuplicate(envelope)) {
            // FIXME change to trace
            log.debug("Filtering out duplicate: {}", envToString(envelope))
            FutureDone
          } else if (!offsetStore.isSequenceNumberAccepted(envelope)) {
            log.debug("Filtering out rejected sequence number (might be accepted later): {}", envToString(envelope))
            FutureDone
          } else {
            val offset = sourceProvider.extractOffset(envelope)
            r2dbcExecutor.withConnection("exactly-once handler") { conn =>
              // run users handler
              val session = new R2dbcSession(conn)
              delegate
                .process(session, envelope)
                .flatMap { _ =>
                  offsetStore.saveOffsetInTx(conn, offset)
                }
            }
          }
        }
      }
  }

  private[projection] def adaptedHandlerForGrouped[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerFactory: () => R2dbcHandler[immutable.Seq[Envelope]],
      offsetStore: R2dbcOffsetStore,
      r2dbcExecutor: R2dbcExecutor)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): () => Handler[immutable.Seq[Envelope]] = { () =>

    new AdaptedR2dbcHandler(handlerFactory()) {
      override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {
        val acceptedEnvelopes = envelopes.iterator.filterNot { env =>
          if (offsetStore.isEnvelopeDuplicate(env)) {
            // FIXME change to trace
            log.debug("Filtering out duplicate: {}", envToString(env))
            true
          } else if (!offsetStore.isSequenceNumberAccepted(env)) {
            log.debug("Filtering out rejected sequence number (might be accepted later): {}", envToString(env))
            true
          } else {
            false
          }
        }.toVector

        if (acceptedEnvelopes.isEmpty) {
          FutureDone
        } else {
          val offsets = acceptedEnvelopes.map(sourceProvider.extractOffset)
          r2dbcExecutor.withConnection("grouped handler") { conn =>
            // run users handler
            val session = new R2dbcSession(conn)
            delegate.process(session, acceptedEnvelopes).flatMap { _ =>
              offsetStore.saveOffsetsInTx(conn, offsets)
            }
          }
        }
      }
    }
  }

  private[projection] def adaptedHandlerForAtLeastOnce[Offset, Envelope](
      handlerFactory: () => R2dbcHandler[Envelope],
      offsetStore: R2dbcOffsetStore,
      r2dbcExecutor: R2dbcExecutor)(implicit ec: ExecutionContext, system: ActorSystem[_]): () => Handler[Envelope] = {
    () =>
      new AdaptedR2dbcHandler(handlerFactory()) {
        override def process(envelope: Envelope): Future[Done] = {
          log.debug("processing {}", envToString(envelope))
          if (offsetStore.isEnvelopeDuplicate(envelope)) {
            // FIXME change to trace
            log.debug("Filtering out duplicate: {}", envToString(envelope))
            FutureDone
          } else if (!offsetStore.isSequenceNumberAccepted(envelope)) {
            log.debug("Filtering out rejected sequence number (might be accepted later): {}", envToString(envelope))
            FutureDone
          } else {
            val processFuture =
              r2dbcExecutor.withConnection("at-least-once handler") { conn =>
                // run users handler
                val session = new R2dbcSession(conn)
                try {
                  delegate.process(session, envelope)
                } catch {
                  case NonFatal(e) => Future.failed(e)
                }
              }
            processFuture.recoverWith { case ex =>
              log.debug("Handler failed to process {}. Removing from inflight map", envToString(envelope))
              offsetStore.updateInflightOnError(envelope)
              Future.failed(ex)
            }
          }
        }
      }
  }

  private[projection] def adaptedHandlerForAtLeastOnceAsync[Offset, Envelope](
      handlerFactory: () => Handler[Envelope],
      offsetStore: R2dbcOffsetStore)(implicit ec: ExecutionContext, system: ActorSystem[_]): () => Handler[Envelope] = {
    () =>
      new AdaptedHandler(handlerFactory()) {
        override def process(envelope: Envelope): Future[Done] = {
          if (offsetStore.isEnvelopeDuplicate(envelope)) {
            // FIXME change to trace
            log.debug("Filtering out duplicate: {}", envToString(envelope))
            FutureDone
          } else if (!offsetStore.isSequenceNumberAccepted(envelope)) {
            log.debug("Filtering out rejected sequence number (might be accepted later): {}", envToString(envelope))
            FutureDone
          } else {

            val processFuture =
              try {
                delegate.process(envelope)
              } catch {
                case NonFatal(e) => Future.failed(e)
              }

            processFuture.recoverWith { case ex =>
              log.debug("Handler failed to process {}. Removing from inflight map", envToString(envelope))
              offsetStore.updateInflightOnError(envelope)
              Future.failed(ex)
            }
          }
        }
      }
  }

  private[projection] def adaptedHandlerForGroupedAsync[Offset, Envelope](
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerFactory: () => Handler[immutable.Seq[Envelope]],
      offsetStore: R2dbcOffsetStore)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): () => Handler[immutable.Seq[Envelope]] = { () =>

    new AdaptedHandler(handlerFactory()) {
      override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {
        val acceptedEnvelopes = envelopes.iterator.filterNot { env =>
          if (offsetStore.isEnvelopeDuplicate(env)) {
            // FIXME change to trace
            log.debug("Filtering out duplicate: {}", envToString(env))
            true
          } else if (!offsetStore.isSequenceNumberAccepted(env)) {
            log.debug("Filtering out rejected sequence number (might be accepted later): {}", envToString(env))
            true
          } else {
            false
          }
        }.toVector

        if (acceptedEnvelopes.isEmpty) {
          FutureDone
        } else {
          delegate.process(acceptedEnvelopes)
        }
      }
    }
  }

  private[projection] def adaptedHandlerForFlow[Offset, Envelope](
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _],
      offsetStore: R2dbcOffsetStore)(implicit
      ec: ExecutionContext,
      system: ActorSystem[_]): FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _] = {

    def isAccepted(env: Envelope): Boolean =
      if (offsetStore.isEnvelopeDuplicate(env)) {
        // FIXME change to trace
        log.debug("Filtering out duplicate: {}", envToString(env))
        false
      } else if (!offsetStore.isSequenceNumberAccepted(env)) {
        log.debug("Filtering out rejected sequence number (might be accepted later): {}", envToString(env))
        false
      } else {
        true
      }

    FlowWithContext[Envelope, ProjectionContext]
      .map { env =>
        // using `map` to evaluate the isAccepted once, otherwise
        // with filter it will invoked twice
        isAccepted(env) -> env
      }
      .collect {
        case (accepted, env) if accepted =>
          env
      }
      .via(handler)

  }

  abstract class AdaptedR2dbcHandler[E](val delegate: R2dbcHandler[E])(implicit
      ec: ExecutionContext,
      system: ActorSystem[_])
      extends Handler[E] {

    override def start(): Future[Done] = Future {
      delegate.start()
      Done
    }
    override def stop(): Future[Done] = Future {
      delegate.stop()
      Done
    }
  }

  abstract class AdaptedHandler[E](val delegate: Handler[E])(implicit ec: ExecutionContext, system: ActorSystem[_])
      extends Handler[E] {

    override def start(): Future[Done] = Future {
      delegate.start()
      Done
    }
    override def stop(): Future[Done] = Future {
      delegate.stop()
      Done
    }
  }

  // TODO add toString to EventEnvelope
  def envToString[Envelope](envelope: Envelope): AnyRef = new AnyRef {
    override def toString: String = envelope match {
      case env: EventEnvelope[_] =>
        s"EventEnvelope(${env.offset}, ${env.persistenceId}, ${env.sequenceNr})"
      case env => env.toString
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class R2dbcProjectionImpl[Offset, Envelope](
    val projectionId: ProjectionId,
    r2dbcSettings: R2dbcProjectionSettings,
    settingsOpt: Option[ProjectionSettings],
    sourceProvider: SourceProvider[Offset, Envelope],
    restartBackoffOpt: Option[RestartSettings],
    val offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy,
    override val statusObserver: StatusObserver[Envelope],
    offsetStore: R2dbcOffsetStore)
    extends scaladsl.ExactlyOnceProjection[Offset, Envelope]
    with javadsl.ExactlyOnceProjection[Offset, Envelope]
    with scaladsl.GroupedProjection[Offset, Envelope]
    with javadsl.GroupedProjection[Offset, Envelope]
    with scaladsl.AtLeastOnceProjection[Offset, Envelope]
    with javadsl.AtLeastOnceProjection[Offset, Envelope]
    with scaladsl.AtLeastOnceFlowProjection[Offset, Envelope]
    with javadsl.AtLeastOnceFlowProjection[Offset, Envelope]
    with SettingsImpl[R2dbcProjectionImpl[Offset, Envelope]]
    with InternalProjection {

  private def copy(
      settingsOpt: Option[ProjectionSettings] = this.settingsOpt,
      restartBackoffOpt: Option[RestartSettings] = this.restartBackoffOpt,
      offsetStrategy: OffsetStrategy = this.offsetStrategy,
      handlerStrategy: HandlerStrategy = this.handlerStrategy,
      statusObserver: StatusObserver[Envelope] = this.statusObserver): R2dbcProjectionImpl[Offset, Envelope] =
    new R2dbcProjectionImpl(
      projectionId,
      r2dbcSettings,
      settingsOpt,
      sourceProvider,
      restartBackoffOpt,
      offsetStrategy,
      handlerStrategy,
      statusObserver,
      offsetStore)

  type ReadOffset = () => Future[Option[Offset]]

  /*
   * Build the final ProjectionSettings to use, if currently set to None fallback to values in config file
   */
  private def settingsOrDefaults(implicit system: ActorSystem[_]): ProjectionSettings = {
    val settings = settingsOpt.getOrElse(ProjectionSettings(system))
    restartBackoffOpt match {
      case None    => settings
      case Some(r) => settings.copy(restartBackoff = r)
    }
  }

  override def withRestartBackoffSettings(restartBackoff: RestartSettings): R2dbcProjectionImpl[Offset, Envelope] =
    copy(restartBackoffOpt = Some(restartBackoff))

  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): R2dbcProjectionImpl[Offset, Envelope] =
    copy(offsetStrategy = offsetStrategy
      .asInstanceOf[AtLeastOnce]
      .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)))

  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): R2dbcProjectionImpl[Offset, Envelope] =
    copy(handlerStrategy = handlerStrategy
      .asInstanceOf[GroupedHandlerStrategy[Envelope]]
      .copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration)))

  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): R2dbcProjectionImpl[Offset, Envelope] = {
    val newStrategy = offsetStrategy match {
      case s: ExactlyOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
      case s: AtLeastOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
      //NOTE: AtMostOnce has its own withRecoveryStrategy variant
      // this method is not available for AtMostOnceProjection
      case s: AtMostOnce => s
    }
    copy(offsetStrategy = newStrategy)
  }

  override def withStatusObserver(observer: StatusObserver[Envelope]): R2dbcProjectionImpl[Offset, Envelope] =
    copy(statusObserver = observer)

  private[projection] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] =
    handlerStrategy.actorHandlerInit

  /**
   * INTERNAL API Return a RunningProjection
   */
  override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection =
    new R2dbcInternalProjectionState(settingsOrDefaults).newRunningInstance()

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached. This
   * is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, Future[Done]] =
    new R2dbcInternalProjectionState(settingsOrDefaults).mappedSource()

  private class R2dbcInternalProjectionState(settings: ProjectionSettings)(implicit val system: ActorSystem[_])
      extends InternalProjectionState[Offset, Envelope](
        projectionId,
        sourceProvider,
        offsetStrategy,
        handlerStrategy,
        statusObserver,
        settings) {

    implicit val executionContext: ExecutionContext = system.executionContext
    override val logger: LoggingAdapter = Logging(system.classicSystem, this.getClass)
    private val log = LoggerFactory.getLogger(this.getClass)

    override def readPaused(): Future[Boolean] =
      offsetStore.readManagementState().map(_.exists(_.paused))

    override def readOffsets(): Future[Option[Offset]] =
      offsetStore.readOffset()

    // Called from InternalProjectionState.saveOffsetAndReport
    override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] =
      offsetStore.saveOffset(offset)

    override protected def saveOffsetAndReport(
        projectionId: ProjectionId,
        projectionContext: ProjectionContextImpl[Offset, Envelope],
        batchSize: Int): Future[Done] = {
      import R2dbcProjectionImpl.envToString
      import R2dbcProjectionImpl.FutureDone
      val envelope = projectionContext.envelope

      if (offsetStore.isEnvelopeDuplicate(envelope)) {
        // FIXME change to trace
        log.debug("saveOffset filtering out duplicate: {}", envToString(envelope))
        FutureDone
      } else if (!offsetStore.wasSequenceNumberAccepted(envelope)) {
        log.debug(
          "saveOffset filtering out rejected sequence number (might be accepted later): {}",
          envToString(envelope))
        FutureDone
      } else {
        super.saveOffsetAndReport(projectionId, projectionContext, batchSize)
      }
    }

    override protected def saveOffsetsAndReport(
        projectionId: ProjectionId,
        batch: Seq[ProjectionContextImpl[Offset, Envelope]]): Future[Done] = {
      import R2dbcProjectionImpl.envToString
      import R2dbcProjectionImpl.FutureDone

      val acceptedContexts = batch.iterator.filterNot { ctx =>
        val env = ctx.envelope
        if (offsetStore.isEnvelopeDuplicate(env)) {
          // FIXME change to trace
          log.debug("saveOffsets filtering out duplicate: {}", envToString(env))
          true
        } else if (!offsetStore.wasSequenceNumberAccepted(env)) {
          log.debug(
            "saveOffsets filtering out rejected sequence number (might be accepted later): {}",
            envToString(env))
          true
        } else {
          false
        }
      }.toVector

      if (acceptedContexts.isEmpty) {
        FutureDone
      } else {
        val offsets = acceptedContexts.map(_.offset)
        offsetStore
          .saveOffsets(offsets)
          .map { done =>
            val batchSize = acceptedContexts.map { _.groupSize }.sum
            val last = acceptedContexts.last
            try {
              statusObserver.offsetProgress(projectionId, last.envelope)
            } catch {
              case NonFatal(_) => // ignore
            }
            getTelemetry().onOffsetStored(batchSize)
            done
          }
      }
    }

    private[projection] def newRunningInstance(): RunningProjection =
      new R2dbcRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), this)
  }

  private class R2dbcRunningProjection(source: Source[Done, _], projectionState: R2dbcInternalProjectionState)(implicit
      system: ActorSystem[_])
      extends RunningProjection
      with RunningProjectionManagement[Offset] {

    private val streamDone = source.run()

    override def stop(): Future[Done] = {
      projectionState.killSwitch.shutdown()
      // if the handler is retrying it will be aborted by this,
      // otherwise the stream would not be completed by the killSwitch until after all retries
      projectionState.abort.failure(AbortProjectionException)
      streamDone
    }

    // RunningProjectionManagement
    override def getOffset(): Future[Option[Offset]] = {
      offsetStore.getOffset()
    }

    // RunningProjectionManagement
    override def setOffset(offset: Option[Offset]): Future[Done] = {
      offset match {
        case Some(o) => offsetStore.saveOffset(o)
        case None    => offsetStore.clearOffset()
      }
    }

    // RunningProjectionManagement
    override def getManagementState(): Future[Option[ManagementState]] =
      offsetStore.readManagementState()

    // RunningProjectionManagement
    override def setPaused(paused: Boolean): Future[Done] =
      offsetStore.savePaused(paused)
  }

}
