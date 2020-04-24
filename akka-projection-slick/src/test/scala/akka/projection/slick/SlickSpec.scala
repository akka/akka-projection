/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import scala.concurrent.Await
import scala.concurrent.duration._

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.slick.internal.SlickOffsetStore
import akka.projection.testkit.ProjectionTestKit
import com.typesafe.config.Config
import slick.basic.DatabaseConfig
import slick.jdbc.H2Profile

abstract class SlickSpec(config: Config) extends ScalaTestWithActorTestKit(config) {

  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", config)

  def offsetStore[Offset] = new SlickOffsetStore[Offset, H2Profile](dbConfig.db, dbConfig.profile)

  val projectionTestKit = new ProjectionTestKit(testKit)

  override protected def beforeAll(): Unit = {
    // create offset table
    Await.ready(offsetStore.createIfNotExists, 3.seconds)
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    dbConfig.db.close()
  }
}
