/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.io.ByteArrayOutputStream
import java.lang
import java.util.Locale

import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.immutable
import scala.reflect.ClassTag
import scala.util.Try

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.serialization.SerializationExtension
import akka.serialization.Serializers
import akka.util.ccompat.JavaConverters._
import com.google.common.base.CaseFormat
import com.google.protobuf.ByteString
import com.google.protobuf.CodedInputStream
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.Descriptors
import com.google.protobuf.GeneratedMessageV3
import com.google.protobuf.Message
import com.google.protobuf.Parser
import com.google.protobuf.UnsafeByteOperations
import com.google.protobuf.WireFormat
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

  private final val PrimitiveFieldNumber = 1
  final val PrimitivePrefix = "type.akka.io/"

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

  private sealed abstract class Primitive[T: ClassTag] {
    val name: String = fieldType.name().toLowerCase(Locale.ROOT)
    val fullName: String = PrimitivePrefix + name
    final val clazz = implicitly[ClassTag[T]].runtimeClass
    def write(stream: CodedOutputStream, t: T): Unit
    def read(stream: CodedInputStream): T
    def fieldType: WireFormat.FieldType
    def defaultValue: T
    val tag: Int = (PrimitiveFieldNumber << 3) | fieldType.getWireType
  }

  private final object StringPrimitive extends Primitive[String] {
    override def fieldType = WireFormat.FieldType.STRING
    override def defaultValue = ""
    override def write(stream: CodedOutputStream, t: String): Unit =
      stream.writeString(PrimitiveFieldNumber, t)
    override def read(stream: CodedInputStream): String = stream.readString()
  }
  private final object BytesPrimitive extends Primitive[ByteString] {
    override def fieldType = WireFormat.FieldType.BYTES
    override def defaultValue: ByteString = ByteString.EMPTY
    override def write(stream: CodedOutputStream, t: ByteString): Unit =
      stream.writeBytes(PrimitiveFieldNumber, t)
    override def read(stream: CodedInputStream): ByteString = stream.readBytes()
  }

  private final val Primitives = Seq(
    StringPrimitive,
    BytesPrimitive,
    new Primitive[Integer] {
      override def fieldType = WireFormat.FieldType.INT32
      override def defaultValue = 0
      override def write(stream: CodedOutputStream, t: Integer): Unit =
        stream.writeInt32(PrimitiveFieldNumber, t)
      override def read(stream: CodedInputStream): Integer = stream.readInt32()
    },
    new Primitive[java.lang.Long] {
      override def fieldType = WireFormat.FieldType.INT64
      override def defaultValue = 0L
      override def write(stream: CodedOutputStream, t: java.lang.Long): Unit =
        stream.writeInt64(PrimitiveFieldNumber, t)
      override def read(stream: CodedInputStream): lang.Long = stream.readInt64()
    },
    new Primitive[java.lang.Float] {
      override def fieldType = WireFormat.FieldType.FLOAT
      override def defaultValue = 0f
      override def write(stream: CodedOutputStream, t: java.lang.Float): Unit =
        stream.writeFloat(PrimitiveFieldNumber, t)
      override def read(stream: CodedInputStream): lang.Float = stream.readFloat()
    },
    new Primitive[java.lang.Double] {
      override def fieldType = WireFormat.FieldType.DOUBLE
      override def defaultValue = 0d
      override def write(stream: CodedOutputStream, t: java.lang.Double): Unit =
        stream.writeDouble(PrimitiveFieldNumber, t)
      override def read(stream: CodedInputStream): lang.Double = stream.readDouble()
    },
    new Primitive[java.lang.Boolean] {
      override def fieldType = WireFormat.FieldType.BOOL
      override def defaultValue = false
      override def write(stream: CodedOutputStream, t: java.lang.Boolean): Unit =
        stream.writeBool(PrimitiveFieldNumber, t)
      override def read(stream: CodedInputStream): lang.Boolean = stream.readBool()
    })

  private final val ClassToPrimitives = Primitives
    .map(p => p.clazz -> p)
    .asInstanceOf[Seq[(Any, Primitive[Any])]]
    .toMap
  private final val NameToPrimitives = Primitives
    .map(p => p.fullName -> p)
    .asInstanceOf[Seq[(String, Primitive[Any])]]
    .toMap

  private[akka] def encodePrimitiveBytes(bytes: ByteString): ByteString =
    primitiveToBytes(BytesPrimitive, bytes)

  private[akka] def decodePrimitiveBytes(bytes: ByteString): ByteString =
    bytesToPrimitive(BytesPrimitive, bytes)

  private def primitiveToBytes[T](primitive: Primitive[T], value: T): ByteString =
    if (value != primitive.defaultValue) {
      val baos = new ByteArrayOutputStream()
      val stream = CodedOutputStream.newInstance(baos)
      primitive.write(stream, value)
      stream.flush()
      UnsafeByteOperations.unsafeWrap(baos.toByteArray)
    } else ByteString.EMPTY

  @nowarn("msg=deprecated") // Stream.continually
  private def bytesToPrimitive[T](primitive: Primitive[T], bytes: ByteString) = {
    val stream = bytes.newCodedInput()
    if (Stream.continually(stream.readTag()).takeWhile(_ != 0).exists { tag =>
          if (primitive.tag != tag) {
            stream.skipField(tag)
            false
          } else true
        }) {
      primitive.read(stream)
    } else primitive.defaultValue
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

  def extractBytes(bytes: ByteString): ByteString =
    bytesToPrimitive(BytesPrimitive, bytes)

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

      // these are all generated message so needs to go before GeneratedMessage,
      // but we encode them inside Any just like regular message, we just need to get the type_url right
      case javaBytes: com.google.protobuf.BytesValue =>
        ScalaPbAny.fromJavaProto(JavaPbAny.pack(javaBytes))

      case scalaBytes: com.google.protobuf.wrappers.BytesValue =>
        ScalaPbAny.pack(scalaBytes)

      case javaText: com.google.protobuf.StringValue =>
        ScalaPbAny.fromJavaProto(JavaPbAny.pack(javaText))

      case scalaText: com.google.protobuf.wrappers.StringValue =>
        ScalaPbAny.pack(scalaText)

      case javaProtoMessage: com.google.protobuf.Message =>
        ScalaPbAny(
          GoogleTypeUrlPrefix + javaProtoMessage.getDescriptorForType.getFullName,
          javaProtoMessage.toByteString)

      case scalaPbMessage: GeneratedMessage =>
        ScalaPbAny(GoogleTypeUrlPrefix + scalaPbMessage.companion.scalaDescriptor.fullName, scalaPbMessage.toByteString)

      case null =>
        throw SerializationException(s"Don't know how to serialize object of type null.")

      case _ if ClassToPrimitives.contains(value.getClass) =>
        val primitive = ClassToPrimitives(value.getClass)
        ScalaPbAny(primitive.fullName, primitiveToBytes(primitive, value))

      case byteString: ByteString =>
        ScalaPbAny(BytesPrimitive.fullName, primitiveToBytes(BytesPrimitive, byteString))

      case other =>
        throw SerializationException(
          s"Don't know how to serialize object of type ${other.getClass.getName}. " +
          "Try passing a protobuf or use a primitive type.")
    }

  /**
   * Decodes a Protobuf Any wrapped message into the concrete user message type or a wrapped
   * primitive into the Java primitive type value. Must only be used where primitive values are expected.
   */
  def decodePossiblyPrimitive(any: ScalaPbAny): Any = {
    val typeUrl = any.typeUrl
    if (typeUrl.startsWith(PrimitivePrefix)) {
      NameToPrimitives.get(typeUrl) match {
        case Some(primitive) =>
          bytesToPrimitive(primitive, any.value)
        case None =>
          throw SerializationException("Unknown primitive type url: " + typeUrl)
      }
    } else {
      decodeMessage(any)
    }
  }

  /**
   * Decodes a Protobuf Any wrapped message into the concrete user message type.
   *
   * Other wrapped primitives are not expected, but the wrapped value is passed through as it is.
   */
  def decodeMessage(any: ScalaPbAny): Any = {
    val typeUrl = any.typeUrl
    if (typeUrl.equals(BytesPrimitive.fullName)) {
      // raw byte strings we turn into BytesValue and expect service method to accept
      val bytes = bytesToPrimitive(BytesPrimitive, any.value)
      if (prefer == Prefer.Java)
        com.google.protobuf.BytesValue.of(bytes)
      else
        com.google.protobuf.wrappers.BytesValue.of(bytes)

    } else if (typeUrl.equals(StringPrimitive.fullName)) {
      // strings as StringValue
      val string = bytesToPrimitive(StringPrimitive, any.value)
      if (prefer == Prefer.Java)
        com.google.protobuf.StringValue.of(string)
      else
        com.google.protobuf.wrappers.StringValue.of(string)

    } else if (typeUrl.startsWith(PrimitivePrefix)) {
      // pass on as is, the generated types will not match the primitive type if we unwrap/decode
      any
    } else {
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

}
