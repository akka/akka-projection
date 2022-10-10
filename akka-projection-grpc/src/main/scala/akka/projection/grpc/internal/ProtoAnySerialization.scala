/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.util.Try

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.serialization.SerializationExtension
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

  }

  private final class JavaPbResolvedType[T <: Message](parser: Parser[T]) extends ResolvedType[T] {
    override def parseFrom(bytes: ByteString): T = parser.parseFrom(bytes)
  }

  private final class ScalaPbResolvedType[T <: scalapb.GeneratedMessage](
      companion: scalapb.GeneratedMessageCompanion[_])
      extends ResolvedType[T] {
    override def parseFrom(bytes: ByteString): T = companion.parseFrom(bytes.newCodedInput()).asInstanceOf[T]
  }

  /**
   * When locating protobufs, if both a Java and a ScalaPB generated class is found on the classpath, this says which
   * should be preferred.
   */
  sealed trait Prefer
  final object Prefer {
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
  val classLoader = system.dynamicAccess.classLoader // FIXME use DynamicAccess

  private val allDescriptors = flattenDescriptors(descriptors)

  private val allTypes = (for {
    descriptor <- allDescriptors.values
    messageType <- descriptor.getMessageTypes.asScala
  } yield messageType.getFullName -> messageType).toMap

  private val reflectionCache = TrieMap.empty[String, Try[ResolvedType[Any]]]

  /**
   * If only used for encoding messages and not decoding messages.
   */
  def this(system: ActorSystem[_]) =
    this(system, descriptors = Nil, ProtoAnySerialization.Prefer.Scala)

  def serialize(event: Any): ScalaPbAny = {
    event match {
      case scalaPbAny: ScalaPbAny => scalaPbAny
      case pbAny: PbAny           => ScalaPbAny.fromJavaProto(pbAny)
      case msg: scalapb.GeneratedMessage =>
        encode(msg)
      case msg: GeneratedMessageV3 =>
        encode(msg)
      case other =>
        // fallback to Akka serialization
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

  def deserialize(scalaPbAny: ScalaPbAny): Any = {
    val typeUrl = scalaPbAny.typeUrl
    if (typeUrl.startsWith(GoogleTypeUrlPrefix)) {
      decodeMessage(scalaPbAny)
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
      Some(new JavaPbResolvedType(parser))

    } catch {
      case cnfe: ClassNotFoundException =>
        log.debug("Failed to load class [{}] because: {}", className, cnfe.getMessage)
        None
      case nsme: NoSuchElementException =>
        // FIXME wrong exception? NoSuchMethodException is thrown from getMethod("parser")
        throw SerializationException(
          s"Found com.google.protobuf.Message class $className to deserialize protobuf ${typeDescriptor.getFullName} but it didn't have a static parser() method on it.",
          nsme)
      case _: NoSuchMethodException =>
        //        throw SerializationException(
        //          s"Found com.google.protobuf.Message class $className to deserialize protobuf ${typeDescriptor.getFullName} but it didn't have a static parser() method on it.",
        //          nsme)
        // FIXME there seems to be a case where the ScalaPB class can be loaded, but it ofc doesn't have the parser method
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
        Some(new ScalaPbResolvedType(companionObject))
      } catch {
        case cnfe: ClassNotFoundException =>
          log.debug("Failed to load class [{}] because: {}", className, cnfe.getMessage)
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

  private def resolveTypeUrl(typeName: String): Option[ResolvedType[_]] =
    allTypes.get(typeName).map(resolveTypeDescriptor)

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
      log.warn("Message type [{}] does not match type url prefix [{}]", typeUrl: Any, GoogleTypeUrlPrefix)
    }
    val typeName = typeUrl.split("/", 2) match {
      case Array(_, typeName) =>
        typeName
      case _ =>
        log.warn(
          "Message type [{}] does not have a url prefix, it should have one that matchers the type url prefix [{}]",
          typeUrl: Any,
          GoogleTypeUrlPrefix)
        typeUrl
    }

    resolveTypeUrl(typeName) match {
      case Some(parser) =>
        parser.parseFrom(any.value)
      case None =>
        throw SerializationException("Unable to find descriptor for type: " + typeUrl)
    }
  }

}
