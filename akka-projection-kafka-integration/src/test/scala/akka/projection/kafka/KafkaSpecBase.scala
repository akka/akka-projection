/*
 * Copyright (C) 2020-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.typed
import akka.actor.typed.scaladsl.adapter._
import akka.kafka.testkit.KafkaTestkitTestcontainersSettings
import akka.kafka.testkit.internal.TestFrameworkInterface
import akka.kafka.testkit.scaladsl.KafkaSpec
import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
import akka.projection.testkit.scaladsl.ProjectionTestKit
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.OptionValues
import org.scalatest.Suite
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContextExecutor

abstract class KafkaSpecBase(val config: Config, kafkaPort: Int)
    extends KafkaSpec(kafkaPort, kafkaPort + 1, ActorSystem("Spec", config))
    with Suite
    with TestFrameworkInterface.Scalatest
    with AnyWordSpecLike
    with OptionValues
    with LogCapturing
    with ScalaFutures
    with Matchers
    with PatienceConfiguration
    with Eventually
    with BeforeAndAfterEach
    with TestcontainersKafkaPerClassLike {

  protected def this() = this(config = ConfigFactory.load, kafkaPort = -1)
  protected def this(config: Config) = this(config = config, kafkaPort = -1)

  val testKit = ActorTestKit(system.toTyped)
  val projectionTestKit = ProjectionTestKit(system.toTyped)

  implicit val actorSystem: typed.ActorSystem[Nothing] = testKit.system
  implicit val dispatcher: ExecutionContextExecutor = testKit.system.executionContext

  override val testcontainersSettings = KafkaTestkitTestcontainersSettings(system)
}
