/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo

import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.{ ActorTestKit, LogCapturing, ScalaTestWithActorTestKit }
import akka.actor.typed.scaladsl.adapter._
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.testkit.TestKit
import com.dimafeng.testcontainers.{ ForAllTestContainer, GenericContainer }
import com.mongodb.ConnectionString
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.mongodb.scala.MongoClientSettings
import org.scalatest.BeforeAndAfterEach
import org.scalatest.OptionValues
import org.scalatest.Suite
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.testcontainers.containers.MongoDBContainer

abstract class MongoSpecBase(val config: Config)
    extends ScalaTestWithActorTestKit(config)
    with Suite
    with AnyWordSpecLike
    with OptionValues
    with LogCapturing
    with ScalaFutures
    with Matchers
    with PatienceConfiguration
    with Eventually
    with BeforeAndAfterEach
    with TestcontainersMongo {

  protected def this() = this(config = ConfigFactory.load)

  val projectionTestKit = new ProjectionTestKit(testKit)

  implicit val actorSystem = testKit.system
  implicit val dispatcher = testKit.system.executionContext
}

trait TestcontainersMongo extends ForAllTestContainer { self: Suite =>
  private val mongoContainer = new MongoDBContainer()
  val container = new GenericContainer(mongoContainer)

  lazy val clientSettings: MongoClientSettings = MongoClientSettings
    .builder()
    .applyConnectionString(new ConnectionString(mongoContainer.getReplicaSetUrl))
    .build()

}
