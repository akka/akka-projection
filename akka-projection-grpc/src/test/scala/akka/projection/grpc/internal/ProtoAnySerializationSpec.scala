/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import java.nio.charset.StandardCharsets
import java.time.Instant

import akka.actor.Address
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.grpc.internal.proto.TestEvent
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpecLike
import com.google.protobuf.any.{ Any => ScalaPbAny }
import com.google.protobuf.{ Any => PbAny }

class ProtoAnySerializationSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with LogCapturing {

  private val protoAnySerialization =
    new ProtoAnySerialization(
      system,
      protoClassMapping = Map(
        "google.protobuf.Timestamp" -> "com.google.protobuf.Timestamp",
        "akka.projection.grpc.internal.TestEvent" -> "akka.projection.grpc.internal.proto.TestEvent"))
  private val akkaSerialization = SerializationExtension(system.classicSystem)

  "ProtoAnySerialization" must {
    "encode and decode Java proto message" in {
      val instant = Instant.now()
      val event =
        com.google.protobuf.Timestamp
          .newBuilder()
          .setSeconds(instant.getEpochSecond)
          .setNanos(17)
          .build()
      val pbAny = protoAnySerialization.encode(event)
      pbAny.typeUrl shouldBe "type.googleapis.com/google.protobuf.Timestamp"
      val deserializedEvent = protoAnySerialization.decode(pbAny)
      deserializedEvent shouldBe event
    }

    "encode and decode ScalaPb proto message" in {
      val event = TestEvent("cart1", "item1", 17)
      val pbAny = protoAnySerialization.encode(event)
      pbAny.typeUrl shouldBe "type.googleapis.com/akka.projection.grpc.internal.TestEvent"
      val deserializedEvent = protoAnySerialization.decode(pbAny)
      deserializedEvent shouldBe event
    }

    "pass through Java proto Any" in {
      val value = "hello"
      val typeUrl = "type.my.io/custom"
      val event = PbAny
        .newBuilder()
        .setTypeUrl(typeUrl)
        .setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8))
        .build()
      val pbAny = protoAnySerialization.encode(event)
      pbAny.typeUrl shouldBe typeUrl
      val deserializedEvent =
        protoAnySerialization.decode(pbAny).asInstanceOf[PbAny]
      deserializedEvent.getTypeUrl shouldBe typeUrl
      deserializedEvent.getValue.toString(StandardCharsets.UTF_8) shouldBe value
    }

    "pass through ScalaPb Any and decode it as Java proto Any" in {
      val value = "hello"
      val typeUrl = "type.my.io/custom"
      val event =
        ScalaPbAny(typeUrl, ByteString.copyFrom(value, StandardCharsets.UTF_8))
      val pbAny = protoAnySerialization.encode(event)
      pbAny.typeUrl shouldBe typeUrl
      val deserializedEvent =
        protoAnySerialization.decode(pbAny).asInstanceOf[PbAny]
      deserializedEvent.getTypeUrl shouldBe typeUrl
      deserializedEvent.getValue.toString(StandardCharsets.UTF_8) shouldBe value
    }

    "encode and decode with Akka serialization with string manifest" in {
      val event = Address("akka", system.name, "localhost", 2552)
      val pbAny = protoAnySerialization.encode(event)
      val serializer = akkaSerialization.findSerializerFor(event)
      // no manifest for String serializer
      pbAny.typeUrl shouldBe s"ser.akka.io/${serializer.identifier}:${Serializers
        .manifestFor(serializer, event)}"

      val deserializedEvent = protoAnySerialization.decode(pbAny)
      deserializedEvent shouldBe event
    }

    "encode and decode with Akka serialization without string manifest" in {
      val event = "e1"
      val pbAny = protoAnySerialization.encode(event)
      val serializer = akkaSerialization.findSerializerFor(event)
      // no manifest for String serializer
      pbAny.typeUrl shouldBe s"ser.akka.io/${serializer.identifier}"

      val deserializedEvent = protoAnySerialization.decode(pbAny)
      deserializedEvent shouldBe event
    }
  }
}
