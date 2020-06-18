/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.collection.immutable
import scala.concurrent.Future

import akka.projection.MergeableKey
import akka.projection.MergeableOffset
import akka.projection.OffsetVerification
import akka.projection.ProjectionContext
import akka.projection.internal.ProjectionContextImpl
import akka.stream.scaladsl.Source

trait SourceProvider[Offset, Envelope] {

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, _]]

  def extractOffset(envelope: Envelope): Offset
}

trait VerifiableSourceProvider[Offset, Envelope] extends SourceProvider[Offset, Envelope] {

  def verifyOffset(offset: Offset): OffsetVerification

}

object MergeableOffsetSourceProvider {

  private[projection] def groupByKey[Envelope](
      envs: immutable.Seq[ProjectionContextImpl[_, Envelope]]): Map[String, immutable.Seq[ProjectionContext]] = {
    val groups: Map[String, immutable.Seq[ProjectionContext]] = envs
      .asInstanceOf[immutable.Seq[ProjectionContextImpl[MergeableOffset[MergeableKey, _], Envelope]]]
      .flatMap { context => context.offset.entries.toSeq.map { case (key, _) => (key, context) } }
      .groupBy { case (key, _) => key }
      .map {
        case (key, keyAndContexts) =>
          val envs = keyAndContexts.map { case (_, context) => context }
          key.surrogateKey -> envs
      }
    groups
  }

}

trait MergeableOffsetSourceProvider[Offset <: MergeableOffset[_, _], Envelope]
    extends SourceProvider[Offset, Envelope] {

  private[projection] def groupByKey(
      envs: immutable.Seq[ProjectionContextImpl[_, Envelope]]): Map[String, immutable.Seq[ProjectionContext]]

}
