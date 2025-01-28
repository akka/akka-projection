/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import scala.collection.immutable
import scala.jdk.FutureConverters._
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

import akka.Done
import akka.annotation.InternalApi
import akka.projection.r2dbc.javadsl
import akka.projection.r2dbc.javadsl.R2dbcSession
import akka.projection.r2dbc.scaladsl

/**
 * INTERNAL API: Adapter from javadsl.R2dbcHandler to scaladsl.R2dbcHandler
 */
@InternalApi private[projection] class R2dbcHandlerAdapter[Envelope](delegate: javadsl.R2dbcHandler[Envelope])
    extends scaladsl.R2dbcHandler[Envelope] {

  override def process(session: scaladsl.R2dbcSession, envelope: Envelope): Future[Done] = {
    delegate.process(new R2dbcSession(session.connection)(session.ec, session.system), envelope).asScala
  }

  override def start(): Future[Done] =
    delegate.start().asScala

  override def stop(): Future[Done] =
    delegate.stop().asScala

}

/**
 * INTERNAL API: Adapter from `javadsl.R2dbcHandler[java.util.List[Envelope]]` to
 * `scaladsl.R2dbcHandler[immutable.Seq[Envelope]]`
 */
@InternalApi private[projection] class R2dbcGroupedHandlerAdapter[Envelope](
    delegate: javadsl.R2dbcHandler[java.util.List[Envelope]])
    extends scaladsl.R2dbcHandler[immutable.Seq[Envelope]] {

  override def process(session: scaladsl.R2dbcSession, envelopes: immutable.Seq[Envelope]): Future[Done] = {
    delegate.process(new R2dbcSession(session.connection)(session.ec, session.system), envelopes.asJava).asScala
  }

  override def start(): Future[Done] =
    delegate.start().asScala

  override def stop(): Future[Done] =
    delegate.stop().asScala

}
