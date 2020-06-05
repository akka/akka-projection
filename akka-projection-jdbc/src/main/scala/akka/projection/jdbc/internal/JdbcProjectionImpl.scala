/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.RunningProjection.AbortProjectionException
import akka.projection.StatusObserver
import akka.projection.internal.HandlerLifecycleAdapter
import akka.projection.internal.HandlerRecoveryImpl
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.RestartBackoffSettings
import akka.projection.internal.SettingsImpl
import akka.projection.jdbc.javadsl.ExactlyOnceJdbcProjection
import akka.projection.jdbc.javadsl.JdbcHandler
import akka.projection.jdbc.javadsl.JdbcProjection
import akka.projection.jdbc.javadsl.JdbcSession
import akka.projection.jdbc.javadsl.JdbcSession.withSession
import akka.projection.scaladsl.SourceProvider
import akka.stream.KillSwitches
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class JdbcProjectionImpl[Offset, Envelope, S <: JdbcSession](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    sessionFactory: () => S,
    settingsOpt: Option[ProjectionSettings],
    restartBackoffOpt: Option[RestartBackoffSettings],
    handler: JdbcHandler[Envelope, S],
    override val statusObserver: StatusObserver[Envelope])
    extends JdbcProjection[Envelope]
    with ExactlyOnceJdbcProjection[Envelope]
    with SettingsImpl[JdbcProjectionImpl[Offset, Envelope, S]] {

  private def copy(
      settingsOpt: Option[ProjectionSettings] = this.settingsOpt,
      restartBackoffOpt: Option[RestartBackoffSettings] = this.restartBackoffOpt,
      statusObserver: StatusObserver[Envelope] = this.statusObserver): JdbcProjectionImpl[Offset, Envelope, S] =
    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt,
      restartBackoffOpt,
      handler,
      statusObserver)

  type ReadOffset = () => Future[Option[Offset]]

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]) =
    new InternalProjectionState(settingsOrDefaults).mappedSource()

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  override private[projection] def run()(implicit system: ActorSystem[_]) =
    new InternalProjectionState(settingsOrDefaults).newRunningInstance()

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

  private class InternalProjectionState(settings: ProjectionSettings)(implicit system: ActorSystem[_]) {

    val offsetStore = createOffsetStore()

    val killSwitch = KillSwitches.shared(projectionId.id)
    val abort: Promise[Done] = Promise()
    val logger = Logging(system.classicSystem, this.getClass)

    private[projection] def mappedSource(): Source[Done, _] = {

      statusObserver.started(projectionId)
      val adapterHandlerLifecycle = new HandlerLifecycleAdapter(handler)

      val handlerRecovery =
        HandlerRecoveryImpl[Offset, Envelope](
          projectionId,
          HandlerRecoveryStrategy.fail, // TODO: remove this when we fully support recovery strategy
          logger,
          statusObserver)

      def processEnvelopeAndStoreOffsetInSameTransaction(env: Envelope): Future[Done] = {
        val offset = sourceProvider.extractOffset(env)
        handlerRecovery.applyRecovery(env, offset, offset, abort.future, () => {
          // this scope ensures that the blocking DB dispatcher is used solely for DB operations
          implicit val executionContext: ExecutionContext = offsetStore.executionContext
          withSession(sessionFactory) { sess =>
            sess.withConnection[Unit] { conn =>
              offsetStore.saveOffsetBlocking(conn, projectionId, offset)
            }
            handler.process(sess, env)
          }.map(_ => Done)
        })

      }

      // stream ops should use use the actor system dispatcher
      implicit val executionContext: ExecutionContext = system.executionContext

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

      val readOffsets = () => {
        val offsetsF = offsetStore.readOffset(projectionId)
        offsetsF.foreach { offset => logger.debug("Starting projection [{}] from offset [{}]", projectionId, offset) }
        offsetsF
      }

      val handlerFlow =
        Flow[Envelope]
          .mapAsync(1) { env =>
            reportProgress(processEnvelopeAndStoreOffsetInSameTransaction(env), env)
          }

      val composedSource: Source[Done, NotUsed] =
        Source
          .futureSource(adapterHandlerLifecycle.tryStart().flatMap(_ => sourceProvider.source(readOffsets)))
          .via(killSwitch.flow)
          .via(handlerFlow)
          .mapMaterializedValue(_ => NotUsed)

      RunningProjection.stopHandlerOnTermination(composedSource, projectionId, adapterHandlerLifecycle, statusObserver)
    }

    private[projection] def newRunningInstance(): RunningProjection =
      new JdbcRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), this)
  }

  private class JdbcRunningProjection(source: Source[Done, _], projectionState: InternalProjectionState)(
      implicit system: ActorSystem[_])
      extends RunningProjection {

    private val streamDone = source.run()

    override def stop(): Future[Done] = {
      projectionState.killSwitch.shutdown()
      // if the handler is retrying it will be aborted by this,
      // otherwise the stream would not be completed by the killSwitch until after all retries
      projectionState.abort.failure(AbortProjectionException)
      streamDone
    }

  }
  private def createOffsetStore()(implicit system: ActorSystem[_]) = {
    val jdbcSettings = JdbcSettings(system)
    new JdbcOffsetStore[S](jdbcSettings, sessionFactory)
  }

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  override def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): CompletionStage[Done] =
    createOffsetStore().createIfNotExists().toJava

  override def withRestartBackoffSettings(
      restartBackoff: RestartBackoffSettings): JdbcProjectionImpl[Offset, Envelope, S] =
    copy(restartBackoffOpt = Some(restartBackoff))

  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): JdbcProjectionImpl[Offset, Envelope, S] =
    this // not supported yet

  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): JdbcProjectionImpl[Offset, Envelope, S] =
    this // not supported yet

  override def withStatusObserver(observer: StatusObserver[Envelope]): JdbcProjectionImpl[Offset, Envelope, S] =
    copy(statusObserver = observer)

  /**
   * INTERNAL API
   */
  override private[akka] def withSettings(settings: ProjectionSettings) =
    copy(settingsOpt = Option(settings))

}
