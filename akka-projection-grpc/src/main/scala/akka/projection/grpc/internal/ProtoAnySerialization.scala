/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.util.Try

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope.SerializedEvent
import akka.serialization.SerializationExtension
import akka.serialization.Serializer
import akka.serialization.SerializerWithStringManifest
import akka.serialization.Serializers
import akka.util.ccompat.JavaConverters._
import com.google.common.base.CaseFormat
import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.any.{ Any => ScalaPbAny }
import com.google.protobuf.{ Any => JavaPbAny }
import com.google.protobuf.{ Any => PbAny }
import org.slf4j.LoggerFactory
import scalapb.GeneratedMessage
import scalapb.GeneratedMessageCompanion
import scalapb.options.Scalapb

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ProtoAnySerialization {
  final val GoogleTypeUrlPrefix = "type.googleapis.com/"
  final val AkkaSerializationTypeUrlPrefix = "ser.akka.io/"
  final val AkkaTypeUrlManifestSeparator = ':'
  private final val ProtoAnyTypeUrl = GoogleTypeUrlPrefix + "google.protobuf.Any"

  private val log = LoggerFactory.getLogger(classOf[ProtoAnySerialization])

  final case class SerializationException(msg: String, cause: Throwable = null) extends RuntimeException(msg, cause)

  /**
   * A resolved type
   */
  private sealed trait ResolvedType[T] {

    /**
     * Parse the given bytes into this type.
     */
    def parseFrom(bytes: ByteString): T

    def messageClass: Class[_]

  }

  private final class JavaPbResolvedType[T <: Message](parser: Parser[T], override val messageClass: Class[_])
      extends ResolvedType[T] {
    override def parseFrom(bytes: ByteString): T = parser.parseFrom(bytes)
  }

  private final class ScalaPbResolvedType[T <: scalapb.GeneratedMessage](
      companion: scalapb.GeneratedMessageCompanion[_],
      override val messageClass: Class[_])
      extends ResolvedType[T] {
    override def parseFrom(bytes: ByteString): T = companion.parseFrom(bytes.newCodedInput()).asInstanceOf[T]
  }

  /**
   * When locating protobufs, if both a Java and a ScalaPB generated class is found on the classpath, this says which
   * should be preferred.
   */
  sealed trait Prefer
  object Prefer {
    case object Java extends Prefer
    case object Scala extends Prefer
  }

  private def flattenDescriptors(
      descriptors: Seq[Descriptors.FileDescriptor]): Map[String, Descriptors.FileDescriptor] =
    flattenDescriptors(Map.empty, descriptors)

  private def flattenDescriptors(
      seenSoFar: Map[String, Descriptors.FileDescriptor],
      descriptors: Seq[Descriptors.FileDescriptor]): Map[String, Descriptors.FileDescriptor] =
    descriptors.foldLeft(seenSoFar) {
      case (results, descriptor) =>
        val descriptorName = descriptor.getName
        if (results.contains(descriptorName)) results
        else {
          val withDesc = results.updated(descriptorName, descriptor)
          flattenDescriptors(
            withDesc,
            descriptor.getDependencies.asScala.toSeq ++ descriptor.getPublicDependencies.asScala)
        }
    }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ProtoAnySerialization(
    system: ActorSystem[_],
    descriptors: immutable.Seq[Descriptors.FileDescriptor],
    prefer: ProtoAnySerialization.Prefer) {
  import ProtoAnySerialization._

  private val serialization = SerializationExtension(system.classicSystem)
  private lazy val akkaProtobufSerializer: Serializer =
    serialization.serializerFor(classOf[com.google.protobuf.GeneratedMessageV3])
  private lazy val isDefaultAkkaProtobufSerializer = {
    val yes = akkaProtobufSerializer.getClass == classOf[akka.remote.serialization.ProtobufSerializer]
    if (yes && akkaProtobufSerializer.isInstanceOf[SerializerWithStringManifest]) {
      // just in case it is changed in the future
      throw new IllegalStateException("ProtobufSerializer was expected to not be a SerializerWithStringManifest")
    }
    yes
  }

  private val allDescriptors = flattenDescriptors(descriptors)

  // proto type name -> Descriptor
  private val allTypes: Map[String, Descriptors.Descriptor] = (for {
    descriptor <- allDescriptors.values
    messageType <- descriptor.getMessageTypes.asScala
  } yield messageType.getFullName -> messageType).toMap

  private val reflectionCache = TrieMap.empty[String, Try[ResolvedType[Any]]]

  // Class name -> typeUrl
  private lazy val allReverseTypeUrls: Map[String, String] = (for {
    descriptor <- allDescriptors.values
    messageType <- descriptor.getMessageTypes.asScala
  } yield {
    val resolvedType = resolveTypeDescriptor(messageType)
    resolvedType.messageClass.getName -> (GoogleTypeUrlPrefix + messageType.getFullName)
  }).toMap

  /**
   * If only used for encoding messages and not decoding messages.
   */
  def this(system: ActorSystem[_]) =
    this(system, descriptors = Nil, ProtoAnySerialization.Prefer.Scala)

  def serialize(event: Any): ScalaPbAny = {
    event match {
      case scalaPbAny: ScalaPbAny if scalaPbAny.typeUrl.startsWith(GoogleTypeUrlPrefix) =>
        ScalaPbAny(ProtoAnyTypeUrl, scalaPbAny.toByteString)
      case pbAny: PbAny if pbAny.getTypeUrl.startsWith(GoogleTypeUrlPrefix) =>
        ScalaPbAny(ProtoAnyTypeUrl, pbAny.toByteString)
      case scalaPbAny: ScalaPbAny => scalaPbAny
      case pbAny: PbAny           => ScalaPbAny.fromJavaProto(pbAny)
      case msg: scalapb.GeneratedMessage =>
        encode(msg)
      case msg: GeneratedMessageV3 =>
        encode(msg)
      case SerializedEvent(bytes, id, manifest) =>
        if (id == akkaProtobufSerializer.identifier && isDefaultAkkaProtobufSerializer) {
          // see corresponding typeUrl cases in `toSerializedEvent`
          allReverseTypeUrls.get(manifest) match {
            case Some(typeUrl) =>
              ScalaPbAny(typeUrl, ByteString.copyFrom(bytes))
            case None =>
              val errMsg =
                if (allDescriptors.isEmpty)
                  s"Unable to find Protobuf descriptor for message class: [$manifest]. " +
                  "No descriptors defined when creating GrpcReadJournal. Note that GrpcReadJournal should be created " +
                  "with the GrpcReadJournal apply/create factory method and not from configuration via GrpcReadJournalProvider " +
                  "when using Protobuf serialization."
                else
                  s"Unable to find Protobuf descriptor for class: [$manifest]."
              throw new IllegalArgumentException(errMsg)
          }
        } else {
          // Akka serialization
          val typeUrl = akkaSerializationTypeUrl(id, manifest)
          ScalaPbAny(typeUrl, ByteString.copyFrom(bytes))
        }
      case other =>
        // fallback to Akka serialization
        val otherAnyRef = other.asInstanceOf[AnyRef]
        val bytes = serialization.serialize(otherAnyRef).get
        val serializer = serialization.findSerializerFor(otherAnyRef)
        val manifest = Serializers.manifestFor(serializer, otherAnyRef)
        val id = serializer.identifier
        ScalaPbAny(akkaSerializationTypeUrl(id, manifest), ByteString.copyFrom(bytes))
    }
  }

  def deserialize(scalaPbAny: ScalaPbAny): Any = {
    val typeUrl = scalaPbAny.typeUrl
    if (typeUrl == ProtoAnyTypeUrl) {
      if (prefer == Prefer.Scala)
        ScalaPbAny.parseFrom(scalaPbAny.value.newCodedInput())
      else
        PbAny.parseFrom(scalaPbAny.value)
    } else if (typeUrl.startsWith(GoogleTypeUrlPrefix)) {
      decodeMessage(scalaPbAny)
    } else if (typeUrl.startsWith(AkkaSerializationTypeUrlPrefix)) {
      val (id, manifest) = akkaSerializerIdAndManifestFromTypeUrl(typeUrl)
      serialization.deserialize(scalaPbAny.value.toByteArray, id, manifest).get
    } else if (prefer == Prefer.Scala) {
      // when custom typeUrl
      scalaPbAny
    } else {
      // when custom typeUrl
      ScalaPbAny.toJavaProto(scalaPbAny)
    }
  }

  def toSerializedEvent(scalaPbAny: ScalaPbAny): Option[SerializedEvent] = {
    // see corresponding typeUrl cases in `deserialize`

    val typeUrl = scalaPbAny.typeUrl
    if (typeUrl == ProtoAnyTypeUrl) {
      // We don't try to optimize this case. One level of indirection too much, and probably not a common case.
      None
    } else if (typeUrl.startsWith(GoogleTypeUrlPrefix)) {
      val messageClass = resolveTypeUrl(typeUrl).messageClass
      val serializer = serialization.serializerFor(messageClass)
      if (serializer.identifier == akkaProtobufSerializer.identifier && isDefaultAkkaProtobufSerializer) {
        // this is the case for akka.remote.serialization.ProtobufSerializer, which is configured by default
        // for protobuf messages
        val manifest = if (serializer.includeManifest) messageClass.getName else ""
        Some(SerializedEvent(scalaPbAny.value.toByteArray, serializer.identifier, manifest))
      } else {
        // we don't know how what a custom serializer would do
        None
      }
    } else if (typeUrl.startsWith(AkkaSerializationTypeUrlPrefix)) {
      val (id, manifest) = akkaSerializerIdAndManifestFromTypeUrl(typeUrl)
      Some(SerializedEvent(scalaPbAny.value.toByteArray, id, manifest))
    } else {
      // We don't try to optimize this case. One level of indirection too much, and probably not a common case.
      None
    }
  }

  private def akkaSerializationTypeUrl(serializerId: Int, manifest: String): String = {
    if (manifest.isEmpty) s"$AkkaSerializationTypeUrlPrefix$serializerId"
    else
      s"$AkkaSerializationTypeUrlPrefix$serializerId$AkkaTypeUrlManifestSeparator$manifest"
  }

  private def akkaSerializerIdAndManifestFromTypeUrl(typeUrl: String): (Int, String) = {
    val idAndManifest =
      typeUrl.substring(AkkaSerializationTypeUrlPrefix.length)
    val i = idAndManifest.indexOf(AkkaTypeUrlManifestSeparator)
    if (i == -1)
      idAndManifest.toInt -> ""
    else
      idAndManifest.substring(0, i).toInt -> idAndManifest.substring(i + 1)
  }

  private def strippedFileName(fileName: String) =
    fileName.split(Array('/', '\\')).last.stripSuffix(".proto")

  private def tryResolveJavaPbType(typeDescriptor: Descriptors.Descriptor): Option[JavaPbResolvedType[Message]] = {
    val fileDescriptor = typeDescriptor.getFile
    val options = fileDescriptor.getOptions
    // Firstly, determine the java package
    val packageName =
      if (options.hasJavaPackage) options.getJavaPackage + "."
      else if (fileDescriptor.getPackage.nonEmpty)
        fileDescriptor.getPackage + "."
      else ""

    val outerClassName =
      if (options.hasJavaMultipleFiles && options.getJavaMultipleFiles) ""
      else if (options.hasJavaOuterClassname)
        options.getJavaOuterClassname + "$"
      else if (fileDescriptor.getName.nonEmpty) {
        val name = strippedFileName(fileDescriptor.getName)
        if (name.contains('_') || name.contains('-') || !name(0).isUpper) {
          // transform snake and kebab case into camel case
          CaseFormat.LOWER_UNDERSCORE
            .to(CaseFormat.UPPER_CAMEL, name.replace('-', '_')) + "$"
        } else {
          // keep name as is to keep already camel cased file name
          strippedFileName(fileDescriptor.getName) + "$"
        }
      } else ""

    val className = packageName + outerClassName + typeDescriptor.getName
    try {
      log.debug("tryResolveJavaPbType attempting to load class {}", className)

      val clazz = system.dynamicAccess.getClassFor[Any](className).get
      val parser = clazz
        .getMethod("parser")
        .invoke(null)
        .asInstanceOf[Parser[com.google.protobuf.Message]]
      Some(new JavaPbResolvedType(parser, clazz))

    } catch {
      case cnfe: ClassNotFoundException =>
        log.debug2("Failed to load class [{}] because: {}", className, cnfe.getMessage)
        None
      case nsme: NoSuchElementException =>
        // Not sure this is exception is thrown. NoSuchMethodException is thrown from getMethod("parser").
        // It was like this in the original Kalix JVM SDK.
        throw SerializationException(
          s"Found com.google.protobuf.Message class $className to deserialize protobuf ${typeDescriptor.getFullName} but it didn't have a static parser() method on it.",
          nsme)
      case _: NoSuchMethodException =>
        // NoSuchMethodException may be thrown from getMethod("parser") if the ScalaPB class can be loaded,
        // but the ScalaPB class doesn't have the parser method.
        None
      case iae @ (_: IllegalAccessException | _: IllegalArgumentException) =>
        throw SerializationException(s"Could not invoke $className.parser()", iae)
      case cce: ClassCastException =>
        throw SerializationException(s"$className.parser() did not return a ${classOf[Parser[_]]}", cce)
    }
  }

  private def tryResolveScalaPbType(typeDescriptor: Descriptors.Descriptor): Option[ScalaPbResolvedType[Nothing]] = {
    // todo - attempt to load the package.proto file for this package to get default options from there
    val fileDescriptor = typeDescriptor.getFile
    val options = fileDescriptor.getOptions
    val scalaOptions: Scalapb.ScalaPbOptions =
      if (options.hasExtension(Scalapb.options)) {
        options.getExtension(Scalapb.options)
      } else Scalapb.ScalaPbOptions.getDefaultInstance

    // Firstly, determine the java package
    val packageName =
      if (scalaOptions.hasPackageName) scalaOptions.getPackageName + "."
      else if (options.hasJavaPackage) options.getJavaPackage + "."
      else if (fileDescriptor.getPackage.nonEmpty)
        fileDescriptor.getPackage + "."
      else ""

    // flat package could be overridden on the command line, so attempt to load both possibilities if it's not
    // explicitly setclassLoader.loadClass(className)
    val possibleBaseNames =
      if (scalaOptions.hasFlatPackage) {
        if (scalaOptions.getFlatPackage) Seq("")
        else Seq(fileDescriptor.getName.stripSuffix(".proto") + ".")
      } else if (fileDescriptor.getName.nonEmpty)
        Seq("", strippedFileName(fileDescriptor.getName) + ".")
      else Seq("")

    possibleBaseNames.collectFirst(Function.unlift { baseName =>
      val className = packageName + baseName + typeDescriptor.getName
      try {
        log.debug("Attempting to load scalapb.GeneratedMessageCompanion object {}", className)
        val companionObject =
          system.dynamicAccess.getObjectFor[GeneratedMessageCompanion[GeneratedMessage]](className).get
        val clazz = system.dynamicAccess.getClassFor[Any](className).get
        Some(new ScalaPbResolvedType(companionObject, clazz))
      } catch {
        case cnfe: ClassNotFoundException =>
          log.debug2("Failed to load class [{}] because: {}", className, cnfe.getMessage)
          None
        case cce: ClassCastException =>
          log.debug2("Failed to load class [{}] because: {}", className, cce.getMessage)
          None
      }
    })
  }

  private def resolveTypeDescriptor(typeDescriptor: Descriptors.Descriptor): ResolvedType[Any] =
    reflectionCache
      .getOrElseUpdate(
        typeDescriptor.getFullName,
        Try {
          val maybeResolvedType =
            if (prefer == Prefer.Java) {
              tryResolveJavaPbType(typeDescriptor).orElse(tryResolveScalaPbType(typeDescriptor))
            } else {
              tryResolveScalaPbType(typeDescriptor).orElse(tryResolveJavaPbType(typeDescriptor))
            }

          maybeResolvedType match {
            case Some(resolvedType) =>
              resolvedType.asInstanceOf[ResolvedType[Any]]
            case None =>
              throw SerializationException("Could not determine serializer for type " + typeDescriptor.getFullName)
          }
        })
      .get

  private def tryResolveTypeUrl(typeUrl: String): Option[ResolvedType[_]] = {
    val typeName = typeNameFromTypeUrl(typeUrl)
    allTypes.get(typeName).map(resolveTypeDescriptor)
  }

  private def resolveTypeUrl(typeUrl: String): ResolvedType[_] =
    tryResolveTypeUrl(typeUrl) match {
      case Some(parser) =>
        parser
      case None =>
        val errMsg =
          if (allDescriptors.isEmpty)
            s"Unable to find Protobuf descriptor for type: [$typeUrl]. " +
            "No descriptors defined when creating GrpcReadJournal. Note that GrpcReadJournal should be created " +
            "with the GrpcReadJournal apply/create factory method and not from configuration via GrpcReadJournalProvider " +
            "when using Protobuf serialization."
          else
            s"Unable to find Protobuf descriptor for type: [$typeUrl]."
        throw SerializationException(errMsg)
    }

  def encode(value: Any): ScalaPbAny =
    value match {
      case javaPbAny: JavaPbAny   => ScalaPbAny.fromJavaProto(javaPbAny)
      case scalaPbAny: ScalaPbAny => scalaPbAny

      case javaProtoMessage: com.google.protobuf.Message =>
        ScalaPbAny(
          GoogleTypeUrlPrefix + javaProtoMessage.getDescriptorForType.getFullName,
          javaProtoMessage.toByteString)

      case scalaPbMessage: GeneratedMessage =>
        ScalaPbAny(GoogleTypeUrlPrefix + scalaPbMessage.companion.scalaDescriptor.fullName, scalaPbMessage.toByteString)

      case null =>
        throw SerializationException(s"Don't know how to serialize object of type null.")

      case other =>
        throw SerializationException(
          s"Don't know how to serialize object of type ${other.getClass.getName}. " +
          "Try passing a protobuf message type.")
    }

  /**
   * Decodes a Protobuf Any wrapped message into the concrete user message type.
   */
  def decodeMessage(any: ScalaPbAny): Any = {
    val typeUrl = any.typeUrl
    // wrapped concrete protobuf message, parse into the right type
    if (!typeUrl.startsWith(GoogleTypeUrlPrefix)) {
      log.warn2("Message type [{}] does not match type url prefix [{}]", typeUrl, GoogleTypeUrlPrefix)
    }

    tryResolveTypeUrl(typeUrl) match {
      case Some(parser) =>
        parser.parseFrom(any.value)
      case None =>
        val errMsg =
          if (allDescriptors.isEmpty)
            s"Unable to find Protobuf descriptor for type: [$typeUrl]. " +
            "No descriptors defined when creating GrpcReadJournal. Note that GrpcReadJournal should be created " +
            "with the GrpcReadJournal apply/create factory method and not from configuration via GrpcReadJournalProvider " +
            "when using Protobuf serialization."
          else
            s"Unable to find Protobuf descriptor for type: [$typeUrl]."
        throw SerializationException(errMsg)
    }
  }

  private def typeNameFromTypeUrl(typeUrl: String): String = {
    typeUrl.split("/", 2) match {
      case Array(_, typeName) =>
        typeName
      case _ =>
        log.warn2(
          "Message type [{}] does not have a url prefix, it should have one that matchers the type url prefix [{}]",
          typeUrl,
          GoogleTypeUrlPrefix)
        typeUrl
    }
  }

}
