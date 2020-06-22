/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.Future

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

  def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, _]] = delegate.toScalaSource(offset)

  def extractOffset(envelope: Envelope): Offset = delegate.extractOffset(envelope)
}
