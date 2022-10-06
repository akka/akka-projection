/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
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

class ProtoAnySerializationSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {

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
        com.google.protobuf.timestamp.TimestampProto.javaDescriptor),
      ProtoAnySerialization.Prefer.Scala)

  private val akkaSerialization = SerializationExtension(system.classicSystem)

  private val addLineItem = AddLineItem(name = "item", productId = "id", quantity = 10)

  "ProtoAnySerialization with Prefer.Java" must {
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
    }

    "encode and decode ScalaPb proto message" in {
      val event = TestEvent("cart1", "item1", 17)
      val pbAny = serializationJava.serialize(event)
      pbAny.typeUrl shouldBe "type.googleapis.com/akka.projection.grpc.internal.TestEvent"
      val deserializedEvent = serializationJava.deserialize(pbAny)
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
      val pbAny = serializationJava.serialize(event)
      pbAny.typeUrl shouldBe typeUrl
      val deserializedEvent =
        serializationJava.deserialize(pbAny).asInstanceOf[PbAny]
      deserializedEvent.getTypeUrl shouldBe typeUrl
      deserializedEvent.getValue.toString(StandardCharsets.UTF_8) shouldBe value
    }

    "pass through ScalaPb Any and decode it as Java proto Any" in {
      val value = "hello"
      val typeUrl = "type.my.io/custom"
      val event =
        ScalaPbAny(typeUrl, ByteString.copyFrom(value, StandardCharsets.UTF_8))
      val pbAny = serializationJava.serialize(event)
      pbAny.typeUrl shouldBe typeUrl
      val deserializedEvent =
        serializationJava.deserialize(pbAny).asInstanceOf[PbAny]
      deserializedEvent.getTypeUrl shouldBe typeUrl
      deserializedEvent.getValue.toString(StandardCharsets.UTF_8) shouldBe value
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
    }

    "encode and decode with Akka serialization without string manifest" in {
      val event = "e1"
      val pbAny = serializationJava.serialize(event)
      val serializer = akkaSerialization.findSerializerFor(event)
      // no manifest for String serializer
      pbAny.typeUrl shouldBe s"ser.akka.io/${serializer.identifier}"

      val deserializedEvent = serializationJava.deserialize(pbAny)
      deserializedEvent shouldBe event
    }

    "support se/deserializing java protobufs" in {
      val any = serializationJava.encode(addLineItem)
      any.typeUrl should ===("type.googleapis.com/" + AddLineItem.scalaDescriptor.fullName)
      serializationJava.decodePossiblyPrimitive(any) should ===(addLineItem)
    }

    def testPrimitive[T](name: String, value: T, defaultValue: T) = {
      val any = serializationJava.encode(value)
      any.typeUrl should ===(ProtoAnySerialization.PrimitivePrefix + name)
      serializationJava.decodePossiblyPrimitive(any) should ===(value)

      val defaultAny = serializationJava.encode(defaultValue)
      defaultAny.typeUrl should ===(ProtoAnySerialization.PrimitivePrefix + name)
      defaultAny.value.size() shouldBe 0
      serializationJava.decodePossiblyPrimitive(defaultAny) should ===(defaultValue)
    }

    "support se/deserializing strings" in testPrimitive("string", "foo", "")
    "support se/deserializing ints" in testPrimitive("int32", 10, 0)
    "support se/deserializing longs" in testPrimitive("int64", 10L, 0L)
    "support se/deserializing floats" in testPrimitive("float", 0.5f, 0f)
    "support se/deserializing doubles" in testPrimitive("double", 0.5d, 0d)
    "support se/deserializing bytes" in testPrimitive("bytes", ByteString.copyFromUtf8("foo"), ByteString.EMPTY)
    "support se/deserializing booleans" in testPrimitive("bool", true, false)

    "deserialize text into StringValue" in {
      val plainText = "some text"
      val any =
        ScalaPbAny(
          "type.akka.io/string",
          ProtoAnySerialization.encodePrimitiveBytes(ByteString.copyFromUtf8(plainText)))
      // both as top level message
      val decoded = serializationJava.decodeMessage(any)
      decoded shouldBe a[com.google.protobuf.StringValue]
      decoded.asInstanceOf[com.google.protobuf.StringValue].getValue should ===(plainText)
      val decoded2 = serializationJava.decodePossiblyPrimitive(any)
      decoded2 shouldBe a[String]
    }

    "deserialize bytes into BytesValue" in {
      val bytes = "some texty bytes"
      val any =
        ScalaPbAny("type.akka.io/bytes", ProtoAnySerialization.encodePrimitiveBytes(ByteString.copyFromUtf8(bytes)))
      // both as top level message
      val decoded = serializationJava.decodeMessage(any)
      decoded shouldBe a[com.google.protobuf.BytesValue]
      decoded.asInstanceOf[com.google.protobuf.BytesValue].getValue.toStringUtf8 should ===(bytes)
      val decoded2 = serializationJava.decodePossiblyPrimitive(any)
      decoded2 shouldBe a[ByteString]
    }

    "serialize BytesValue like a regular message" in {
      val bytes = ByteString.copyFromUtf8("woho!")
      val encoded = serializationJava.encode(com.google.protobuf.BytesValue.newBuilder().setValue(bytes).build())
      encoded.typeUrl should ===("type.googleapis.com/google.protobuf.BytesValue")
      com.google.protobuf.BytesValue.parseFrom(encoded.value).getValue should ===(bytes)
    }

    "serialize StringValue like a regular message" in {
      val text = "waha!"
      val encoded = serializationJava.encode(com.google.protobuf.StringValue.newBuilder().setValue(text).build())
      encoded.typeUrl should ===("type.googleapis.com/google.protobuf.StringValue")
      com.google.protobuf.StringValue.parseFrom(encoded.value).getValue should ===(text)
    }
  }

  "ProtoAnySerialization with Prefer.Scala" must {
    "deserialize text into StringValue" in {
      val plainText = "some text"
      val any =
        ScalaPbAny(
          "type.akka.io/string",
          ProtoAnySerialization.encodePrimitiveBytes(ByteString.copyFromUtf8(plainText)))
      // both as top level message
      val decoded = serializationScala.decodeMessage(any)
      decoded shouldBe a[com.google.protobuf.wrappers.StringValue]
      decoded.asInstanceOf[com.google.protobuf.wrappers.StringValue].value should ===(plainText)
      val decoded2 = serializationScala.decodePossiblyPrimitive(any)
      decoded2 shouldBe a[String]
    }

    "deserialize bytes into BytesValue" in {
      val bytes = "some texty bytes"
      val any =
        ScalaPbAny("type.akka.io/bytes", ProtoAnySerialization.encodePrimitiveBytes(ByteString.copyFromUtf8(bytes)))
      // both as top level message
      val decoded = serializationScala.decodeMessage(any)
      decoded shouldBe a[com.google.protobuf.wrappers.BytesValue]
      decoded.asInstanceOf[com.google.protobuf.wrappers.BytesValue].value.toStringUtf8 should ===(bytes)
      val decoded2 = serializationScala.decodePossiblyPrimitive(any)
      decoded2 shouldBe a[ByteString]
    }

    "serialize BytesValue like a regular message" in {
      val bytes = ByteString.copyFromUtf8("woho!")
      val encoded = serializationScala.encode(com.google.protobuf.wrappers.BytesValue(bytes))
      encoded.typeUrl should ===("type.googleapis.com/google.protobuf.BytesValue")
      com.google.protobuf.wrappers.BytesValue.parseFrom(encoded.value.newCodedInput()).value should ===(bytes)
    }

    "serialize StringValue like a regular message" in {
      val text = "waha!"
      val encoded = serializationScala.encode(com.google.protobuf.wrappers.StringValue(text))
      encoded.typeUrl should ===("type.googleapis.com/google.protobuf.StringValue")
      com.google.protobuf.wrappers.StringValue.parseFrom(encoded.value.newCodedInput()).value should ===(text)
    }
  }
}
