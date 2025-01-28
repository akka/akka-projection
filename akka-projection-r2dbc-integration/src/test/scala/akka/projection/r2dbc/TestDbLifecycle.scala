/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.R2dbcExecutor
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.projection.r2dbc"

  lazy val r2dbcProjectionSettings: R2dbcProjectionSettings =
    R2dbcProjectionSettings(typedSystem.settings.config.getConfig(testConfigPath))
  lazy val r2dbcSettings: R2dbcSettings =
    R2dbcSettings(
      typedSystem.settings.config
        .getConfig(r2dbcProjectionSettings.useConnectionFactory.replace(".connection-factory", "")))
  lazy val r2dbcExecutor: R2dbcExecutor = {
    new R2dbcExecutor(
      ConnectionFactoryProvider(typedSystem).connectionFactoryFor(r2dbcProjectionSettings.useConnectionFactory),
      LoggerFactory.getLogger(getClass),
      r2dbcProjectionSettings.logDbCallsExceeding,
      ConnectionFactoryProvider(typedSystem)
        .connectionPoolSettingsFor(r2dbcProjectionSettings.useConnectionFactory)
        .closeCallsExceeding)(typedSystem.executionContext, typedSystem)
  }

  lazy val persistenceExt: Persistence = Persistence(typedSystem)

  override protected def beforeAll(): Unit = {
    try {
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcSettings.journalTableWithSchema(0)}")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from ${r2dbcSettings.snapshotsTable}")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcSettings.durableStateTableWithSchema(0)}")),
        10.seconds)
      if (r2dbcProjectionSettings.isOffsetTableDefined) {
        Await.result(
          r2dbcExecutor.updateOne("beforeAll delete")(
            _.createStatement(s"delete from ${r2dbcProjectionSettings.offsetTableWithSchema}")),
          10.seconds)
      }
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcProjectionSettings.timestampOffsetTableWithSchema}")),
        10.seconds)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcProjectionSettings.managementTableWithSchema}")),
        10.seconds)
    } catch {
      case NonFatal(ex) =>
        throw new RuntimeException("Failed to clean up tables before test", ex)
    }
    super.beforeAll()
  }
}
