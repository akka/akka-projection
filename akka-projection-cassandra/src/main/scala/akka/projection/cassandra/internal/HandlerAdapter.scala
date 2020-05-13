/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.projection.javadsl
import akka.projection.scaladsl

/**
 * INTERNAL API: Adapter from javadsl.Handler to scaladsl.Handler
 */
@InternalApi private[akka] class HandlerAdapter[Envelope](delegate: javadsl.Handler[Envelope])
    extends scaladsl.Handler[Envelope] {

  override def process(envelope: Envelope): Future[Done] = {
    delegate.process(envelope).toScala
  }

  override def start(): Future[Done] =
    delegate.start().toScala

  override def stop(): Future[Done] =
    delegate.stop().toScala

}
