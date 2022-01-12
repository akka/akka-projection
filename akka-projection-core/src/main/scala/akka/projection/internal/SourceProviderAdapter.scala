/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.Future

import akka.NotUsed
import akka.annotation.InternalApi
import akka.projection.javadsl
import akka.projection.scaladsl
import akka.stream.scaladsl.Source

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider
 */
@InternalApi private[projection] class SourceProviderAdapter[Offset, Envelope](
    delegate: javadsl.SourceProvider[Offset, Envelope])
    extends scaladsl.SourceProvider[Offset, Envelope] {

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] = {
    // the parasitic context is used to convert the Optional to Option and a java streams Source to a scala Source,	
    // it _should_ not be used for the blocking operation of getting offsets themselves	
    val ec = akka.dispatch.ExecutionContexts.parasitic
    val offsetAdapter = new Supplier[CompletionStage[Optional[Offset]]] {
      override def get(): CompletionStage[Optional[Offset]] = offset().map(_.asJava)(ec).toJava
    }
    delegate.source(offsetAdapter).toScala.map(_.asScala)(ec)
  }

  def extractOffset(envelope: Envelope): Offset = delegate.extractOffset(envelope)

  def extractCreationTime(envelope: Envelope): Long = delegate.extractCreationTime(envelope)
}
