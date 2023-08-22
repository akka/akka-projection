/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.nio.charset.StandardCharsets
import java.time.Instant

import akka.actor.Address
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.grpc.internal.proto.TestEvent
import akka.projection.grpc.internal.proto.TestProto
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import com.example.shoppingcart.AddLineItem
import com.example.shoppingcart.ShoppingcartApiProto
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpecLike
import com.google.protobuf.any.{ Any => ScalaPbAny }
import com.google.protobuf.{ Any => PbAny }

class ProtoAnySerializationSpec
    extends ScalaTestWithActorTestKit("""
    akka.actor.serialization-bindings {
      # // FIXME important to document this for the SerializedEvent optimization to work
      "scalapb.GeneratedMessage" = proto
    }
    """)
    with AnyWordSpecLike
    with LogCapturing {

  private val serializationJava =
    new ProtoAnySerialization(
      system,
      List(
        TestProto.javaDescriptor,
        ShoppingcartApiProto.javaDescriptor,
        com.google.protobuf.TimestampProto.getDescriptor),
      ProtoAnySerialization.Prefer.Java)

  private val serializationScala =
    new ProtoAnySerialization(
      system,
      List(
        TestProto.javaDescriptor,
        ShoppingcartApiProto.javaDescriptor,
        com.google.protobuf.timestamp.TimestampProto.javaDescriptor,
        com.google.protobuf.any.AnyProto.javaDescriptor),
      ProtoAnySerialization.Prefer.Scala)

  private val akkaSerialization = SerializationExtension(system.classicSystem)
  private val akkaProtobufSerializer = akkaSerialization.serializerFor(classOf[com.google.protobuf.GeneratedMessageV3])

  private val addLineItem = AddLineItem(name = "item", productId = "id", quantity = 10)

  "ProtoAnySerialization" must {
    "serialize and deserialize Java proto message" in {
      val instant = Instant.now()
      val event =
        com.google.protobuf.Timestamp
          .newBuilder()
          .setSeconds(instant.getEpochSecond)
          .setNanos(17)
          .build()
      val pbAny = serializationJava.serialize(event)
      pbAny.typeUrl shouldBe "type.googleapis.com/google.protobuf.Timestamp"
      val deserializedEvent = serializationJava.deserialize(pbAny)
      deserializedEvent.getClass shouldBe classOf[com.google.protobuf.Timestamp]
      deserializedEvent shouldBe event

      val serializedEvent = serializationJava.toSerializedEvent(pbAny).get
      serializedEvent.serializerId shouldBe akkaProtobufSerializer.identifier
      serializedEvent.serializerManifest shouldBe event.getClass.getName
      val deserializedEvent2 = akkaSerialization
        .deserialize(serializedEvent.bytes, serializedEvent.serializerId, serializedEvent.serializerManifest)
        .get
      deserializedEvent2.getClass shouldBe classOf[com.google.protobuf.Timestamp]
      deserializedEvent2 shouldBe event
    }

    "encode and decode ScalaPb proto message" in {
      val event = TestEvent("cart1", "item1", 17)
      val pbAny = serializationScala.serialize(event)
      pbAny.typeUrl shouldBe "type.googleapis.com/akka.projection.grpc.internal.TestEvent"
      val deserializedEvent = serializationScala.deserialize(pbAny)
      deserializedEvent shouldBe event

      val serializedEvent = serializationScala.toSerializedEvent(pbAny).get
      serializedEvent.serializerId shouldBe akkaProtobufSerializer.identifier
      serializedEvent.serializerManifest shouldBe event.getClass.getName
      val deserializedEvent2 = akkaSerialization
        .deserialize(serializedEvent.bytes, serializedEvent.serializerId, serializedEvent.serializerManifest)
        .get
      deserializedEvent2 shouldBe event
    }

    "pass through Java proto Any" in {
      val value = "hello"
      val typeUrl = "type.my.io/custom"
      val event = PbAny
        .newBuilder()
        .setTypeUrl(typeUrl)
        .setValue(ByteString.copyFrom(value, StandardCharsets.UTF_8))
        .build()
      val pbAny = serializationJava.serialize(event)
      pbAny.typeUrl shouldBe typeUrl
      val deserializedEvent =
        serializationJava.deserialize(pbAny).asInstanceOf[PbAny]
      deserializedEvent.getTypeUrl shouldBe typeUrl
      deserializedEvent.getValue.toString(StandardCharsets.UTF_8) shouldBe value

      val serializedEvent = serializationJava.toSerializedEvent(pbAny).get
      serializedEvent.serializerId shouldBe akkaProtobufSerializer.identifier
      serializedEvent.serializerManifest shouldBe event.getClass.getName
      val deserializedEvent2 = akkaSerialization
        .deserialize(serializedEvent.bytes, serializedEvent.serializerId, serializedEvent.serializerManifest)
        .get
        .asInstanceOf[PbAny]
      deserializedEvent2.getTypeUrl shouldBe typeUrl
      deserializedEvent2.getValue.toStringUtf8 shouldBe value
    }

    "pass through ScalaPb Any and decode it as preferred Any" in {
      val value = "hello"
      val typeUrl = "type.my.io/custom"
      val event =
        ScalaPbAny(typeUrl, ByteString.copyFrom(value, StandardCharsets.UTF_8))
      val pbAny = serializationScala.serialize(event)
      pbAny.typeUrl shouldBe typeUrl

      val deserializedEventScala =
        serializationScala.deserialize(pbAny).asInstanceOf[ScalaPbAny]
      deserializedEventScala.typeUrl shouldBe typeUrl
      deserializedEventScala.value.toString(StandardCharsets.UTF_8) shouldBe value

      val deserializedEventJava =
        serializationJava.deserialize(pbAny).asInstanceOf[PbAny]
      deserializedEventJava.getTypeUrl shouldBe typeUrl
      deserializedEventJava.getValue.toString(StandardCharsets.UTF_8) shouldBe value

      val serializedEventScala = serializationScala.toSerializedEvent(pbAny).get
      serializedEventScala.serializerId shouldBe akkaProtobufSerializer.identifier
      serializedEventScala.serializerManifest shouldBe classOf[ScalaPbAny].getName
      val deserializedEventScala2 = akkaSerialization
        .deserialize(
          serializedEventScala.bytes,
          serializedEventScala.serializerId,
          serializedEventScala.serializerManifest)
        .get
        .asInstanceOf[ScalaPbAny]
      deserializedEventScala2.typeUrl shouldBe typeUrl
      deserializedEventScala2.value.toStringUtf8 shouldBe value

      val serializedEventJava = serializationJava.toSerializedEvent(pbAny).get
      serializedEventJava.serializerId shouldBe akkaProtobufSerializer.identifier
      serializedEventJava.serializerManifest shouldBe classOf[PbAny].getName
      val deserializedEventJava2 = akkaSerialization
        .deserialize(
          serializedEventJava.bytes,
          serializedEventJava.serializerId,
          serializedEventJava.serializerManifest)
        .get
        .asInstanceOf[PbAny]
      deserializedEventJava2.getTypeUrl shouldBe typeUrl
      deserializedEventJava2.getValue.toStringUtf8 shouldBe value
    }

    "pass through Java proto Any with Google typeUrl" in {
      val instant = Instant.now()
      val value =
        com.google.protobuf.Timestamp
          .newBuilder()
          .setSeconds(instant.getEpochSecond)
          .setNanos(17)
          .build()
      val typeUrl = "type.googleapis.com/google.protobuf.Timestamp"
      val event = PbAny
        .newBuilder()
        .setTypeUrl(typeUrl)
        .setValue(value.toByteString)
        .build()
      val pbAny = serializationJava.serialize(event)
      pbAny.typeUrl shouldBe "type.googleapis.com/google.protobuf.Any" // wrapped
      val deserializedEvent = serializationJava.deserialize(pbAny).asInstanceOf[PbAny]
      deserializedEvent.getTypeUrl shouldBe typeUrl
      com.google.protobuf.Timestamp.parseFrom(deserializedEvent.getValue) shouldBe value

      val serializedEvent = serializationJava.toSerializedEvent(pbAny).get
      serializedEvent.serializerId shouldBe akkaProtobufSerializer.identifier
      serializedEvent.serializerManifest shouldBe event.getClass.getName
      val pbAny2 = akkaSerialization
        .deserialize(serializedEvent.bytes, serializedEvent.serializerId, serializedEvent.serializerManifest)
        .get
        .asInstanceOf[PbAny]
      pbAny2.getTypeUrl shouldBe "type.googleapis.com/google.protobuf.Any" // wrapped
      val deserializedEvent2 = serializationJava.deserialize(pbAny).asInstanceOf[PbAny]
      deserializedEvent2.getTypeUrl shouldBe typeUrl
      com.google.protobuf.Timestamp.parseFrom(deserializedEvent2.getValue) shouldBe value
    }

    "pass through ScalaPb Any with Google typeUrl" in {
      val value = TestEvent("cart1", "item1", 17)
      val typeUrl = "type.googleapis.com/TestEvent"
      val event =
        ScalaPbAny(typeUrl, value.toByteString)
      val pbAny = serializationScala.serialize(event)
      pbAny.typeUrl shouldBe "type.googleapis.com/google.protobuf.Any" // wrapped

      val deserializedEvent =
        serializationScala.deserialize(pbAny).asInstanceOf[ScalaPbAny]
      deserializedEvent.typeUrl shouldBe typeUrl
      TestEvent.parseFrom(deserializedEvent.value.toByteArray) shouldBe value

      val serializedEvent = serializationScala.toSerializedEvent(pbAny).get
      serializedEvent.serializerId shouldBe akkaProtobufSerializer.identifier
      serializedEvent.serializerManifest shouldBe event.getClass.getName
      val pbAny2 = akkaSerialization
        .deserialize(serializedEvent.bytes, serializedEvent.serializerId, serializedEvent.serializerManifest)
        .get
        .asInstanceOf[ScalaPbAny]
      pbAny2.typeUrl shouldBe "type.googleapis.com/google.protobuf.Any" // wrapped
      val deserializedEvent2 = serializationScala.deserialize(pbAny).asInstanceOf[ScalaPbAny]
      deserializedEvent2.typeUrl shouldBe typeUrl
      TestEvent.parseFrom(deserializedEvent2.value.toByteArray) shouldBe value
    }

    "encode and decode with Akka serialization with string manifest" in {
      val event = Address("akka", system.name, "localhost", 2552)
      val pbAny = serializationJava.serialize(event)
      val serializer = akkaSerialization.findSerializerFor(event)
      // no manifest for String serializer
      pbAny.typeUrl shouldBe s"ser.akka.io/${serializer.identifier}:${Serializers
        .manifestFor(serializer, event)}"

      val deserializedEvent = serializationJava.deserialize(pbAny)
      deserializedEvent shouldBe event

      val serializedEvent = serializationJava.toSerializedEvent(pbAny).get
      val deserializedEvent2 = akkaSerialization
        .deserialize(serializedEvent.bytes, serializedEvent.serializerId, serializedEvent.serializerManifest)
        .get
      deserializedEvent2 shouldBe event
    }

    "encode and decode with Akka serialization without string manifest" in {
      val event = "e1"
      val pbAny = serializationJava.serialize(event)
      val serializer = akkaSerialization.findSerializerFor(event)
      // no manifest for String serializer
      pbAny.typeUrl shouldBe s"ser.akka.io/${serializer.identifier}"

      val deserializedEvent = serializationJava.deserialize(pbAny)
      deserializedEvent shouldBe event

      val serializedEvent = serializationJava.toSerializedEvent(pbAny).get
      val deserializedEvent2 = akkaSerialization
        .deserialize(serializedEvent.bytes, serializedEvent.serializerId, serializedEvent.serializerManifest)
        .get
      deserializedEvent2 shouldBe event
    }

    "support se/deserializing java protobufs" in {
      val any = serializationJava.encode(addLineItem)
      any.typeUrl should ===("type.googleapis.com/" + AddLineItem.scalaDescriptor.fullName)
      serializationJava.decodeMessage(any) should ===(addLineItem)
    }

  }

}
