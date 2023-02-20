/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc

import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.projection.r2dbc.R2dbcProjectionSettings
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.projection.r2dbc"

  lazy val r2dbcProjectionSettings: R2dbcProjectionSettings =
    R2dbcProjectionSettings(typedSystem.settings.config.getConfig(testConfigPath))

  lazy val r2dbcExecutor: R2dbcExecutor = {
    new R2dbcExecutor(
      ConnectionFactoryProvider(typedSystem).connectionFactoryFor(r2dbcProjectionSettings.useConnectionFactory),
      LoggerFactory.getLogger(getClass),
      r2dbcProjectionSettings.logDbCallsExceeding)(typedSystem.executionContext, typedSystem)
  }

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  override protected def beforeAll(): Unit = {
    lazy val r2dbcSettings: R2dbcSettings =
      new R2dbcSettings(typedSystem.settings.config.getConfig("akka.persistence.r2dbc"))
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(
        _.createStatement(s"delete from ${r2dbcSettings.journalTableWithSchema}")),
      10.seconds)
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(
        _.createStatement(s"delete from ${r2dbcProjectionSettings.timestampOffsetTableWithSchema}")),
      10.seconds)
    super.beforeAll()
  }

}
