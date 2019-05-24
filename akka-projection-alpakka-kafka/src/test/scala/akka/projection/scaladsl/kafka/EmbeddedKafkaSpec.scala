/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike

abstract class EmbeddedKafkaSpec(port: Int) extends SpecBase(port) with EmbeddedKafkaLike
