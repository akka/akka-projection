/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.compat.java8.OptionConverters._

import akka.annotation.InternalApi
import akka.projection.javadsl
import akka.projection.scaladsl
import akka.stream.scaladsl.Source

/**
 * INTERNAL API: Adapter from javadsl.SourceProvider to scaladsl.SourceProvider
 */
@InternalApi private[akka] class SourceProviderAdapter[Offset, Envelope](
    delegate: javadsl.SourceProvider[Offset, Envelope])
    extends scaladsl.SourceProvider[Offset, Envelope] {

  def source(offset: Option[Offset]): Source[Envelope, _] =
    delegate.source(offset.asJava).asScala

  def extractOffset(envelope: Envelope): Offset =
    delegate.extractOffset(envelope)

}
