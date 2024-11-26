/*
 * Copyright (C) 2022-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.scaladsl

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.ConnectionPoolSettings
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.projection.r2dbc.R2dbcProjectionSettings
import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactory
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement
import org.slf4j.LoggerFactory

object R2dbcSession {
  private val log = LoggerFactory.getLogger(classOf[R2dbcSession])

  private val logDbCallsDisabled = -1.millis

  /**
   * Runs the passed function in using a R2dbcSession with a new transaction. The connection is closed and the
   * transaction is committed at the end or rolled back in case of failures.
   */
  def withSession[A](system: ActorSystem[_])(fun: R2dbcSession => Future[A]): Future[A] = {
    withSession(system, connectionFactoryConfigPath(system))(fun)
  }

  def withSession[A](system: ActorSystem[_], connectionFactoryConfigPath: String)(
      fun: R2dbcSession => Future[A]): Future[A] = {
    val connectionFactoryProvider = ConnectionFactoryProvider(system)
    val connectionFactory = connectionFactoryProvider.connectionFactoryFor(connectionFactoryConfigPath)
    val poolSettings = new ConnectionPoolSettings(system.settings.config.getConfig(connectionFactoryConfigPath))
    withSession(system, connectionFactory, poolSettings.closeCallsExceeding)(fun)
  }

  /**
   * Provide a custom connectionFactory. The config closeCallsExceeding is loaded from the default path.
   */
  def withSession[A](system: ActorSystem[_], connectionFactory: ConnectionFactory)(
      fun: R2dbcSession => Future[A]): Future[A] = {
    val poolConfig = system.settings.config.getConfig(connectionFactoryConfigPath(system))
    val poolSettings = new ConnectionPoolSettings(poolConfig)
    withSession(system, connectionFactory, poolSettings.closeCallsExceeding)(fun)
  }

  @InternalApi private[akka] def withSession[A](system: ActorSystem[_], connectionFactory: ConnectionFactory, closeCallsExceeding: Option[FiniteDuration])(
    fun: R2dbcSession => Future[A]): Future[A] = {
    val r2dbcExecutor =
      new R2dbcExecutor(connectionFactory, log, logDbCallsDisabled, closeCallsExceeding)(
        system.executionContext,
        system)
    r2dbcExecutor.withConnection("R2dbcSession") { connection =>
      val session = new R2dbcSession(connection)(system.executionContext, system)
      fun(session)
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def connectionFactoryConfigPath(system: ActorSystem[_]): String =
    system.settings.config.getConfig(R2dbcProjectionSettings.DefaultConfigPath).getString("use-connection-factory")
}

final class R2dbcSession(val connection: Connection)(implicit val ec: ExecutionContext, val system: ActorSystem[_]) {

  def createStatement(sql: String): Statement =
    connection.createStatement(sql)

  def updateOne(statement: Statement): Future[Long] =
    R2dbcExecutor.updateOneInTx(statement)

  def update(statements: immutable.IndexedSeq[Statement]): Future[immutable.IndexedSeq[Long]] =
    R2dbcExecutor.updateInTx(statements)

  def selectOne[A](statement: Statement)(mapRow: Row => A): Future[Option[A]] =
    R2dbcExecutor.selectOneInTx(statement, mapRow)

  def select[A](statement: Statement)(mapRow: Row => A): Future[immutable.IndexedSeq[A]] =
    R2dbcExecutor.selectInTx(statement, mapRow)

}
