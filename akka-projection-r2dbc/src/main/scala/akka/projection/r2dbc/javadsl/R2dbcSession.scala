/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.persistence.r2dbc.internal.R2dbcExecutor
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

import scala.jdk.CollectionConverters._

@ApiMayChange
final class R2dbcSession(val connection: Connection)(implicit ec: ExecutionContext, system: ActorSystem[_]) {

  def createStatement(sql: String): Statement =
    connection.createStatement(sql)

  def updateOne(statement: Statement): CompletionStage[java.lang.Long] =
    R2dbcExecutor.updateOneInTx(statement).map(java.lang.Long.valueOf)(ExecutionContexts.parasitic).toJava

  def update(statements: java.util.List[Statement]): CompletionStage[java.util.List[java.lang.Long]] =
    R2dbcExecutor
      .updateInTx(statements.asScala.toVector)
      .map(results => results.map(java.lang.Long.valueOf).asJava)
      .toJava

  def selectOne[A](statement: Statement)(mapRow: Row => A): CompletionStage[Optional[A]] =
    R2dbcExecutor.selectOneInTx(statement, mapRow).map(_.asJava)(ExecutionContexts.parasitic).toJava

  def select[A](statement: Statement)(mapRow: Row => A): CompletionStage[java.util.List[A]] =
    R2dbcExecutor.selectInTx(statement, mapRow).map(_.asJava).toJava

}
