/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike

abstract class TestcontainersKafkaSpec extends SpecBase with TestcontainersKafkaLike
