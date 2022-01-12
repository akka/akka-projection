/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.io.NotSerializableException

import akka.Done
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization.MultipleOffsets
import akka.projection.internal.protobuf.ProjectionMessages
import akka.serialization.BaseSerializer
import akka.serialization.SerializerWithStringManifest

/**
 * INTERNAL API
 */
@InternalApi private[projection] class ProjectionSerializer(val system: akka.actor.ExtendedActorSystem)
    extends SerializerWithStringManifest
    with BaseSerializer {

  import ProjectionBehavior.Internal._

  // lazy because Serializers are initialized early on. `toTyped` might then try to
  // initialize the classic ActorSystemAdapter extension.
  private lazy val resolver = ActorRefResolver(system.toTyped)
  private lazy val offsetSerialization = {
    import akka.actor.typed.scaladsl.adapter._
    new OffsetSerialization(system.toTyped)
  }

  private val GetOffsetManifest = "a"
  private val CurrentOffsetManifest = "b"
  private val SetOffsetManifest = "c"
  private val IsPausedManifest = "d"
  private val SetPausedManifest = "e"

  override def manifest(o: AnyRef): String = o match {
    case _: GetOffset[_]     => GetOffsetManifest
    case _: CurrentOffset[_] => CurrentOffsetManifest
    case _: SetOffset[_]     => SetOffsetManifest
    case _: IsPaused         => IsPausedManifest
    case _: SetPaused        => SetPausedManifest
    case _ =>
      throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
  }

  override def toBinary(o: AnyRef): Array[Byte] = o match {
    case m: GetOffset[_]     => getOffsetToBinary(m)
    case m: CurrentOffset[_] => currentOffsetToBinary(m)
    case m: SetOffset[_]     => setOffsetToBinary(m)
    case m: IsPaused         => isPausedToBinary(m)
    case m: SetPaused        => setPausedToBinary(m)
    case _ =>
      throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
  }

  private def getOffsetToBinary(m: GetOffset[_]): Array[Byte] = {
    val b = ProjectionMessages.GetOffset.newBuilder()
    b.setProjectionId(projectionIdToProto(m.projectionId))
    b.setReplyTo(resolver.toSerializationFormat(m.replyTo))
    b.build().toByteArray()
  }

  private def currentOffsetToBinary(m: CurrentOffset[_]): Array[Byte] = {
    val b = ProjectionMessages.CurrentOffset.newBuilder()
    b.setProjectionId(projectionIdToProto(m.projectionId))
    m.offset.foreach { o =>
      b.setOffset(offsetToProto(m.projectionId, o))
    }
    b.build().toByteArray()
  }

  private def setOffsetToBinary(m: SetOffset[_]): Array[Byte] = {
    val b = ProjectionMessages.SetOffset.newBuilder()
    b.setProjectionId(projectionIdToProto(m.projectionId))
    b.setReplyTo(resolver.toSerializationFormat(m.replyTo))
    m.offset.foreach { o =>
      b.setOffset(offsetToProto(m.projectionId, o))
    }
    b.build().toByteArray()
  }

  private def offsetToProto(projectionId: ProjectionId, offset: Any): ProjectionMessages.Offset = {
    val storageRepresentation = offsetSerialization.toStorageRepresentation(projectionId, offset) match {
      case s: OffsetSerialization.SingleOffset => s
      case _: MultipleOffsets                  => throw new IllegalArgumentException("MultipleOffsets not supported yet.") // TODO
    }
    ProjectionMessages.Offset
      .newBuilder()
      .setManifest(storageRepresentation.manifest)
      .setValue(storageRepresentation.offsetStr)
      .build()
  }

  private def isPausedToBinary(m: IsPaused): Array[Byte] = {
    val b = ProjectionMessages.IsPaused.newBuilder()
    b.setProjectionId(projectionIdToProto(m.projectionId))
    b.setReplyTo(resolver.toSerializationFormat(m.replyTo))
    b.build().toByteArray()
  }

  private def setPausedToBinary(m: SetPaused): Array[Byte] = {
    val b = ProjectionMessages.SetPaused.newBuilder()
    b.setProjectionId(projectionIdToProto(m.projectionId))
    b.setReplyTo(resolver.toSerializationFormat(m.replyTo))
    b.setPaused(m.paused)
    b.build().toByteArray()
  }

  private def projectionIdToProto(projectionId: ProjectionId): ProjectionMessages.ProjectionId = {
    ProjectionMessages.ProjectionId.newBuilder().setName(projectionId.name).setKey(projectionId.key).build()
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = manifest match {
    case GetOffsetManifest     => getOffsetFromBinary(bytes)
    case CurrentOffsetManifest => currentOffsetFromBinary(bytes)
    case SetOffsetManifest     => setOffsetFromBinary(bytes)
    case IsPausedManifest      => isPausedFromBinary(bytes)
    case SetPausedManifest     => setPausedFromBinary(bytes)
    case _ =>
      throw new NotSerializableException(
        s"Unimplemented deserialization of message with manifest [$manifest] in [${getClass.getName}]")
  }

  private def getOffsetFromBinary(bytes: Array[Byte]): AnyRef = {
    val getOffset = ProjectionMessages.GetOffset.parseFrom(bytes)
    GetOffset(
      projectionId = projectionIdFromProto(getOffset.getProjectionId),
      replyTo = resolver.resolveActorRef[CurrentOffset[Any]](getOffset.getReplyTo))
  }

  private def currentOffsetFromBinary(bytes: Array[Byte]): AnyRef = {
    val currentOffset = ProjectionMessages.CurrentOffset.parseFrom(bytes)
    CurrentOffset(
      projectionId = projectionIdFromProto(currentOffset.getProjectionId),
      offset = if (currentOffset.hasOffset) Some(offsetFromProto(currentOffset.getOffset)) else None)
  }

  private def setOffsetFromBinary(bytes: Array[Byte]): AnyRef = {
    val setOffset = ProjectionMessages.SetOffset.parseFrom(bytes)
    SetOffset(
      projectionId = projectionIdFromProto(setOffset.getProjectionId),
      replyTo = resolver.resolveActorRef[Done](setOffset.getReplyTo),
      offset = if (setOffset.hasOffset) Some(offsetFromProto(setOffset.getOffset)) else None)
  }

  private def isPausedFromBinary(bytes: Array[Byte]): AnyRef = {
    val isPaused = ProjectionMessages.IsPaused.parseFrom(bytes)
    IsPaused(
      projectionId = projectionIdFromProto(isPaused.getProjectionId),
      replyTo = resolver.resolveActorRef[Boolean](isPaused.getReplyTo))
  }

  private def setPausedFromBinary(bytes: Array[Byte]): AnyRef = {
    val setPaused = ProjectionMessages.SetPaused.parseFrom(bytes)
    SetPaused(
      projectionId = projectionIdFromProto(setPaused.getProjectionId),
      replyTo = resolver.resolveActorRef[Done](setPaused.getReplyTo),
      paused = setPaused.getPaused)
  }

  private def projectionIdFromProto(p: ProjectionMessages.ProjectionId): ProjectionId =
    ProjectionId(p.getName, p.getKey)

  private def offsetFromProto(o: ProjectionMessages.Offset): Any =
    offsetSerialization.fromStorageRepresentation(o.getValue, o.getManifest)

}
