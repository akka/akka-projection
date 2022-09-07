/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters._

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.r2dbc.internal.R2dbcExecutor
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

@ApiMayChange
final class R2dbcSession(connection: Connection)(implicit ec: ExecutionContext, system: ActorSystem[_]) {

  def createStatement(sql: String): Statement =
    connection.createStatement(sql)

  def updateOne(statement: Statement): CompletionStage[Integer] =
    R2dbcExecutor.updateOneInTx(statement).map(Integer.valueOf)(ExecutionContext.parasitic).toJava

  def update(statements: java.util.List[Statement]): CompletionStage[java.util.List[Integer]] =
    R2dbcExecutor.updateInTx(statements.asScala.toVector).map(results => results.map(Integer.valueOf).asJava).toJava

  def selectOne[A](statement: Statement)(mapRow: Row => A): CompletionStage[Optional[A]] =
    R2dbcExecutor.selectOneInTx(statement, mapRow).map(_.asJava)(ExecutionContext.parasitic).toJava

  def select[A](statement: Statement)(mapRow: Row => A): CompletionStage[java.util.List[A]] =
    R2dbcExecutor.selectInTx(statement, mapRow).map(_.asJava).toJava

}
