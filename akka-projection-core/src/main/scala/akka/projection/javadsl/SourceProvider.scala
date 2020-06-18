/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.jdk.CollectionConverters._

import akka.projection.MergeableOffset
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.internal.ProjectionContextImpl
import akka.projection.ProjectionContext
import akka.projection.scaladsl.MergeableOffsetSourceProvider
import akka.stream.javadsl.Source

trait SourceProvider[Offset, Envelope] {

  def source(offset: Supplier[CompletionStage[Optional[Offset]]]): CompletionStage[Source[Envelope, _]]

  def extractOffset(envelope: Envelope): Offset

}

trait VerifiableSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  def verifyOffset(offset: Offset): OffsetVerification

}

trait MergeableOffsetSourceProvider[Offset <: MergeableOffset[_, _], Envelope] {

  private[projection] def groupByKey(envs: java.util.List[ProjectionContextImpl[_, Envelope]])
      : java.util.Map[String, java.util.List[ProjectionContext]]

}
