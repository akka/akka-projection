/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

///*
// * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
// */
//
//package akka.projection.kafka.internal
//
//import akka.actor.testkit.typed.scaladsl.LogCapturing
//import akka.kafka.testkit.scaladsl.ScalatestKafkaSpec
//import akka.kafka.testkit.scaladsl.TestcontainersKafkaPerClassLike
//import org.scalatest.concurrent.Eventually
//import org.scalatest.concurrent.ScalaFutures
//import org.scalatest.matchers.should.Matchers
//import org.scalatest.wordspec.AnyWordSpecLike
//
//abstract class SpecBase(kafkaPort: Int)
//    extends ScalatestKafkaSpec(kafkaPort)
//    with AnyWordSpecLike
//    with Matchers
//    with ScalaFutures
//    with Eventually
//    with LogCapturing {
//
//  protected def this() = this(kafkaPort = -1)
//}
//
//class KafkaSlackProjectionImplSpec
//    extends SpecBase
//    with TestcontainersKafkaPerClassLike
//    with AnyWordSpecLike
//    with LogCapturing {}
