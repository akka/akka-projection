/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.scaladsl

import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class TransformationSpec extends AnyWordSpec with Matchers with ScalaFutures {

  "The gRPC event Transformation" should {

    "transform simple" in {
      val transformer = Transformation.empty.registerMapper((_: String) => Some("mapped"))
      transformer("whatever", None).futureValue should ===(Some("mapped"))
    }

    "transform filter out simple" in {
      val transformer = Transformation.empty.registerMapper((_: String) => None)
      transformer("whatever", None).futureValue should ===(None)
    }

    "transform simple async" in {
      val transformer = Transformation.empty.registerAsyncMapper((_: String) => Future.successful(Some("mapped")))
      transformer("whatever", None).futureValue should ===(Some("mapped"))
    }

    "transform low level with metadata" in {
      val transformer =
        Transformation.empty.registerLowLevelMapper((_: String, meta) => Future.successful(meta))
      transformer("whatever", Some("meta")).futureValue should ===(Some("meta"))
    }

    "fail by default if no transformer exist for event" in {
      val transformer = Transformation.empty
      transformer("whatever", Some("meta")).failed.futureValue
    }

    "fallback if no transformer exist for event" in {
      val transformer = Transformation.empty.registerOrElseMapper(_ => Some("fallback"))
      transformer("whatever", Some("meta")).futureValue should ===(Some("fallback"))
    }

    "fallback low level with metadata if no transformer exist for event" in {
      val transformer = Transformation.empty.registerLowLevelOrElseMapper((_, meta) => Future.successful(meta))
      transformer("whatever", Some("meta")).futureValue should ===(Some("meta"))
    }
  }

}
