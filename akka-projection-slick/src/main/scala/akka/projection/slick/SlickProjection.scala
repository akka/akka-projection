/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import akka.Done
import akka.annotation.ApiMayChange
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.internal.SlickProjectionImpl
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

@ApiMayChange
object SlickProjection {

  def exactlyOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      eventHandler: SlickHandler[Envelope]): Projection[Envelope] =
    new SlickProjectionImpl(projectionId, sourceProvider, databaseConfig, SlickProjectionImpl.ExactlyOnce, eventHandler)

  def atLeastOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      saveOffsetAfterEnvelopes: Int,
      saveOffsetAfterDuration: FiniteDuration,
      eventHandler: SlickHandler[Envelope]): Projection[Envelope] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      SlickProjectionImpl.AtLeastOnce(saveOffsetAfterEnvelopes, saveOffsetAfterDuration),
      eventHandler)

}

object SlickHandler {

  /** SlickEventHandler that can be define from a simple function */
  private class SlickHandlerSAM[Envelope](handler: Envelope => DBIO[Done]) extends SlickHandler[Envelope] {
    override def handle(envelope: Envelope): DBIO[Done] = handler(envelope)
  }

  def apply[Envelope](handler: Envelope => DBIO[Done]): SlickHandler[Envelope] = new SlickHandlerSAM(handler)
}

trait SlickHandler[Envelope] {
  def handle(envelope: Envelope): DBIO[Done]
  def onFailure(envelope: Envelope, throwable: Throwable): RecoverStrategy = {
    val _ = envelope // need it otherwise compiler says no
    val _ = throwable
    RecoverStrategy.fail
  }
}

sealed trait RecoverStrategy
case object Fail extends RecoverStrategy
case object Skip extends RecoverStrategy
final case class RetryAndFail(retries: Int, delay: FiniteDuration) extends RecoverStrategy
final case class RetryAndSkip(retries: Int, delay: FiniteDuration) extends RecoverStrategy

object RecoverStrategy {
  def fail: RecoverStrategy = Fail
  def skip: RecoverStrategy = Skip
  def retryAndFail(retries: Int, delay: FiniteDuration): RecoverStrategy = RetryAndFail(retries, delay)
  def retryAndSkip(retries: Int, delay: FiniteDuration): RecoverStrategy = RetryAndSkip(retries, delay)
}
