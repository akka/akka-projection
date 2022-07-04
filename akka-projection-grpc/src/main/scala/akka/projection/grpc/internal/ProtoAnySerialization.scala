/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.lang.reflect.Method

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import com.google.protobuf.ByteString
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.Message
import com.google.protobuf.any.{ Any => ScalaPbAny }
import com.google.protobuf.{ Any => PbAny }
import scalapb.GeneratedMessageCompanion

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ProtoAnySerialization {
  final val GoogleTypeUrlPrefix = "type.googleapis.com/"
  final val AkkaSerializationTypeUrlPrefix = "ser.akka.io/"
  final val AkkaTypeUrlManifestSeparator = ':'

  private val ArrayOfByteArray = Array[Class[_]](classOf[Array[Byte]])
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ProtoAnySerialization(
    system: ActorSystem[_],
    protoClassMapping: Map[String, String]) {
  import ProtoAnySerialization._

  private val serialization = SerializationExtension(system.classicSystem)

  private val scalaPbCompanions
      : Map[String, GeneratedMessageCompanion[scalapb.GeneratedMessage]] = {
    protoClassMapping.flatMap { case (protoName, className) =>
      system.dynamicAccess
        .getObjectFor[GeneratedMessageCompanion[scalapb.GeneratedMessage]](
          className)
        .toOption
        .map(companion => protoName -> companion)
    }
  }

  private val javaParsingMethods: Map[String, Method] = {
    protoClassMapping.collect {
      case (protoName, className) if !scalaPbCompanions.contains(protoName) =>
        val clazz = system.dynamicAccess.getClassFor[Message](className).get
        val parsingMethod =
          clazz.getDeclaredMethod("parseFrom", ArrayOfByteArray: _*)
        protoName -> parsingMethod
    }
  }

  def encode(event: Any): ScalaPbAny = {
    event match {
      case scalaPbAny: ScalaPbAny => scalaPbAny
      case pbAny: PbAny           => ScalaPbAny.fromJavaProto(pbAny)
      case msg: scalapb.GeneratedMessage =>
        ScalaPbAny(
          GoogleTypeUrlPrefix + msg.companion.scalaDescriptor.fullName,
          msg.toByteString)
      case msg: GeneratedMessageV3 =>
        ScalaPbAny(
          GoogleTypeUrlPrefix + msg.getDescriptorForType.getFullName,
          msg.toByteString)
      case other =>
        val otherAnyRef = other.asInstanceOf[AnyRef]
        val bytes = serialization.serialize(otherAnyRef).get
        val serializer = serialization.findSerializerFor(otherAnyRef)
        val manifest = Serializers.manifestFor(serializer, otherAnyRef)
        val id = serializer.identifier
        val typeUrl =
          if (manifest.isEmpty) s"$AkkaSerializationTypeUrlPrefix$id"
          else
            s"$AkkaSerializationTypeUrlPrefix$id$AkkaTypeUrlManifestSeparator$manifest"

        ScalaPbAny(typeUrl, ByteString.copyFrom(bytes))
    }
  }

  def decode(scalaPbAny: ScalaPbAny): Any = {
    val typeUrl = scalaPbAny.typeUrl
    if (typeUrl.startsWith(GoogleTypeUrlPrefix)) {
      val manifest = typeUrl.substring(GoogleTypeUrlPrefix.length)
      scalaPbCompanions.get(manifest) match {
        case Some(companion) =>
          companion.parseFrom(scalaPbAny.value.newCodedInput())
        case None =>
          javaParsingMethods.get(manifest) match {
            case Some(parsingMethod) =>
              parsingMethod.invoke(null, scalaPbAny.value.toByteArray)
            case None =>
              throw new IllegalArgumentException(
                s"Need a configured protobuf message class to be able to deserialize [$manifest].")
          }
      }
    } else if (typeUrl.startsWith(AkkaSerializationTypeUrlPrefix)) {
      val idAndManifest =
        typeUrl.substring(AkkaSerializationTypeUrlPrefix.length)
      val i = idAndManifest.indexOf(AkkaTypeUrlManifestSeparator)
      val (id, manifest) =
        if (i == -1)
          idAndManifest.toInt -> ""
        else
          idAndManifest.substring(0, i).toInt -> idAndManifest.substring(i + 1)

      serialization.deserialize(scalaPbAny.value.toByteArray, id, manifest).get
    } else {
      ScalaPbAny.toJavaProto(scalaPbAny)
    }
  }

}
