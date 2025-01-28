/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.javadsl

import akka.Done
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.grpc.scaladsl.MetadataBuilder
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.Collections
import java.util.concurrent.CompletableFuture
import scala.annotation.nowarn

class EventProducerPushDestinationSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "The Java DSL EventProducerPushDestination" should {

    "correctly map the transformationForOrigin to accept Scala GRPC metadata" in {
      @nowarn("msg=never used")
      val epsd = EventProducerPushDestination
        .create("origin-id", Collections.emptyList(), system)
        .withTransformationForOrigin((originId, metadata) => Transformation.identity)
      val scalaEpsd = epsd.asScala

      val transformation = scalaEpsd.transformationForOrigin("someOrigin", MetadataBuilder.empty)

      transformation should be(Transformation.identity.delegate)
    }

    "correctly map the interceptor to accept Scala GRPC metadata" in {
      @nowarn("msg=never used")
      val epsd = EventProducerPushDestination
        .create("origin-id", Collections.emptyList(), system)
        .withInterceptor((originId, metadata) => CompletableFuture.completedFuture(Done))
      val scalaEpsd = epsd.asScala

      val interceptResult = scalaEpsd.interceptor.get.intercept("someOrigin", MetadataBuilder.empty)

      interceptResult.futureValue should be(Done)
    }

  }

}
