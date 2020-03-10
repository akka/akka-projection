/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
import akka.projection.testkit.ProjectionTestRunner
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Matchers, WordSpecLike}


abstract class SpecBase(kafkaPort: Int)
  extends ScalatestKafkaSpec(kafkaPort)
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with ProjectionTestRunner
    with Eventually {

  protected def this() = this(kafkaPort = -1)


}
