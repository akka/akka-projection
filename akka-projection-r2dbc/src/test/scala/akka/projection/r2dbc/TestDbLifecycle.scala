/**
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.r2dbc

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.internal.R2dbcExecutor
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import org.slf4j.LoggerFactory

trait TestDbLifecycle extends BeforeAndAfterAll { this: Suite =>

  def typedSystem: ActorSystem[_]

  def testConfigPath: String = "akka.projection.r2dbc"

  lazy val r2dbcSettings: R2dbcProjectionSettings =
    new R2dbcProjectionSettings(typedSystem.settings.config.getConfig(testConfigPath))

  lazy val r2dbcExecutor: R2dbcExecutor = {
    new R2dbcExecutor(
      ConnectionFactoryProvider(typedSystem).connectionFactoryFor(testConfigPath + ".connection-factory"),
      LoggerFactory.getLogger(getClass))(typedSystem.executionContext, typedSystem)
  }

  override protected def beforeAll(): Unit = {
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from ${r2dbcSettings.table}")),
      10.seconds)
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(_.createStatement(s"delete from ${r2dbcSettings.managementTable}")),
      10.seconds)
    super.beforeAll()
  }

}
