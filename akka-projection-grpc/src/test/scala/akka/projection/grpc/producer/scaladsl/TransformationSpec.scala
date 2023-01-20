/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.scaladsl

import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class TransformationSpec extends AnyWordSpec with Matchers with ScalaFutures {

  def envelope(payload: String, meta: Option[Any] = None) =
    new EventEnvelope[String](
      offset = null,
      persistenceId = "pid1",
      sequenceNr = 1,
      Some(payload),
      timestamp = System.currentTimeMillis(),
      entityType = "banana",
      slice = 5,
      eventMetadata = meta,
      filtered = false,
      source = "").asInstanceOf[EventEnvelope[Any]]

  "The gRPC event Transformation" should {

    "transform simple" in {
      val transformer = Transformation.empty.registerMapper((_: String) => Some("mapped"))
      transformer(envelope("whatever")).futureValue should ===(Some("mapped"))
    }

    "transform filter out simple" in {
      val transformer = Transformation.empty.registerMapper((_: String) => None)
      transformer(envelope("whatever")).futureValue should ===(None)
    }

    "transform simple async" in {
      val transformer = Transformation.empty.registerAsyncMapper((_: String) => Future.successful(Some("mapped")))
      transformer(envelope("whatever")).futureValue should ===(Some("mapped"))
    }

    "transform low level with metadata" in {
      val transformer =
        Transformation.empty.registerAsyncEnvelopeMapper((env: EventEnvelope[String]) =>
          Future.successful(env.eventMetadata))
      transformer(envelope("whatever", Some("meta"))).futureValue should ===(Some("meta"))
    }

    "fail by default if no transformer exist for event" in {
      val transformer = Transformation.empty
      transformer(envelope("whatever", Some("meta"))).failed.futureValue
    }

    "fallback if no transformer exist for event" in {
      val transformer = Transformation.empty.registerOrElseMapper(_ => Some("fallback"))
      transformer(envelope("whatever", Some("meta"))).futureValue should ===(Some("fallback"))
    }

    "fallback low level with metadata if no transformer exist for event" in {
      val transformer = Transformation.empty.registerAsyncEnvelopeOrElseMapper((env: EventEnvelope[Any]) =>
        Future.successful(env.eventMetadata))
      transformer(envelope("whatever", Some("meta"))).futureValue should ===(Some("meta"))
    }
  }

}
