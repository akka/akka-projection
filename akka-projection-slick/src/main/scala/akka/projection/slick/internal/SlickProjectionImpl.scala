/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.event.Logging
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.RunningProjection
import akka.projection.internal.HandlerRecoveryImpl
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.SlickHandler
import akka.stream.KillSwitches
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

@InternalApi
private[projection] object SlickProjectionImpl {
  sealed trait Strategy
  case object ExactlyOnce extends Strategy
  final case class AtLeastOnce(afterEnvelopes: Int, orAfterDuration: FiniteDuration) extends Strategy
}

@InternalApi
private[projection] class SlickProjectionImpl[Offset, Envelope, P <: JdbcProfile](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    databaseConfig: DatabaseConfig[P],
    strategy: SlickProjectionImpl.Strategy,
    settingsOpt: Option[ProjectionSettings],
    handler: SlickHandler[Envelope])
    extends Projection[Envelope] {
  import SlickProjectionImpl._

  override def withSettings(settings: ProjectionSettings): Projection[Envelope] = {
    new SlickProjectionImpl(projectionId, sourceProvider, databaseConfig, strategy, Option(settings), handler)
  }

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  override private[projection] def run()(implicit systemProvider: ClassicActorSystemProvider): RunningProjection =
    new InternalProjectionState(
      offsetStore = new SlickOffsetStore(databaseConfig.db, databaseConfig.profile),
      settings = settingsOrDefaults).newRunningInstance()

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  override private[projection] def mappedSource()(
      implicit systemProvider: ClassicActorSystemProvider): Source[Done, _] =
    new InternalProjectionState(
      offsetStore = new SlickOffsetStore(databaseConfig.db, databaseConfig.profile),
      settings = settingsOrDefaults).mappedSource()

  /*
   * Build the final ProjectionSettings to use, if currently set to None fallback to values in config file
   */
  private def settingsOrDefaults(implicit systemProvider: ClassicActorSystemProvider): ProjectionSettings =
    settingsOpt.getOrElse(ProjectionSettings(systemProvider))

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  private class InternalProjectionState(offsetStore: SlickOffsetStore[P], settings: ProjectionSettings)(
      implicit systemProvider: ClassicActorSystemProvider) {

    private val killSwitch = KillSwitches.shared(projectionId.id)

    private[projection] def mappedSource(): Source[Done, _] = {

      import databaseConfig.profile.api._

      // TODO: add a LogSource for projection when we have a name and key
      val logger = Logging(systemProvider.classicSystem, this.getClass)

      implicit val dispatcher = systemProvider.classicSystem.dispatcher

      def applyUserRecovery(envelope: Envelope, offset: Offset)(futureCallback: () => Future[Done]): Future[Done] =
        HandlerRecoveryImpl.applyUserRecovery[Offset, Envelope](handler, envelope, offset, logger, futureCallback)

      def processEnvelopeAndStoreOffsetInSameTransaction(env: Envelope): Future[Done] = {
        val offset = sourceProvider.extractOffset(env)
        // run user function and offset storage on the same transaction
        // any side-effect in user function is at-least-once
        val txDBIO =
          offsetStore
            .saveOffset(projectionId, offset)
            .flatMap(_ => handler.process(env))
            .transactionally

        applyUserRecovery(env, offset) { () =>
          databaseConfig.db.run(txDBIO).map(_ => Done)
        }
      }

      def processEnvelope(env: Envelope, offset: Offset): Future[Done] = {
        // user function in one transaction (may be composed of several DBIOAction)
        val dbio = handler.process(env).transactionally
        applyUserRecovery(env, offset) { () =>
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

      val futSource = sourceProvider.source(readOffsets)

      val handlerFlow: Flow[Envelope, Done, _] =
        strategy match {
          case ExactlyOnce =>
            Flow[Envelope]
              .mapAsync(1)(processEnvelopeAndStoreOffsetInSameTransaction)

          case AtLeastOnce(1, _) =>
            // optimization of general AtLeastOnce case, still separate transactions for processEnvelope
            // and storeOffset
            Flow[Envelope].mapAsync(1) { env =>
              val offset = sourceProvider.extractOffset(env)
              processEnvelope(env, offset).flatMap(_ => storeOffset(offset))
            }

          case AtLeastOnce(afterEnvelopes, orAfterDuration) =>
            Flow[Envelope]
              .mapAsync(1) { env =>
                val offset = sourceProvider.extractOffset(env)
                processEnvelope(env, offset).map(_ => offset)
              }
              .groupedWithin(afterEnvelopes, orAfterDuration)
              .collect { case grouped if grouped.nonEmpty => grouped.last }
              .mapAsync(parallelism = 1)(storeOffset)
        }

      Source
        .futureSource(futSource)
        .via(killSwitch.flow)
        .via(handlerFlow)

    }

    private[projection] def newRunningInstance(): RunningProjection =
      new SlickRunningProjection(RunningProjection.withBackoff(mappedSource(), settings), killSwitch)
  }

  private class SlickRunningProjection(source: Source[Done, _], killSwitch: SharedKillSwitch)(
      implicit systemProvider: ClassicActorSystemProvider)
      extends RunningProjection {

    private val futureDone = source.run()

    /**
     * INTERNAL API
     *
     * Stop the projection if it's running.
     * @return Future[Done] - the returned Future should return the stream materialized value.
     */
    @InternalApi
    override private[projection] def stop()(implicit ec: ExecutionContext): Future[Done] = {
      killSwitch.shutdown()
      futureDone
    }
  }

}
