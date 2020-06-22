/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

import akka.annotation.InternalApi
import akka.projection.MergeableKey
import akka.projection.MergeableOffset
import akka.projection.OffsetVerification

abstract class SourceProvider[Offset, Envelope] {

  def source(
      offset: Supplier[CompletionStage[Optional[Offset]]]): CompletionStage[akka.stream.javadsl.Source[Envelope, _]]

  def extractOffset(envelope: Envelope): Offset

  @InternalApi private[akka] def toScalaSource(
      offset: () => Future[Option[Offset]]): Future[akka.stream.scaladsl.Source[Envelope, _]] = {
    // the parasitic context is used to convert the Optional to Option and a java streams Source to a scala Source,
    // it _should_ not be used for the blocking operation of getting offsets themselves
    val ec = akka.dispatch.ExecutionContexts.parasitic
    val offsetAdapter = new Supplier[CompletionStage[Optional[Offset]]] {
      override def get(): CompletionStage[Optional[Offset]] = offset().map(_.asJava)(ec).toJava
    }
    source(offsetAdapter).toScala.map(_.asScala)(ec)
  }

}

trait VerifiableSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  def verifyOffset(offset: Offset): OffsetVerification

}

trait MergeableOffsetSourceProvider[MKey <: MergeableKey, Offset <: MergeableOffset[MKey, _], Envelope]
    extends SourceProvider[Offset, Envelope]
