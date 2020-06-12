/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.javadsl
import akka.projection.jdbc.scaladsl

/**
 * INTERNAL API: Adapter from javadsl.Handler to scaladsl.Handler
 */
@InternalApi private[projection] class JdbcHandlerAdapter[Envelope, S <: JdbcSession](
    delegate: javadsl.JdbcHandler[Envelope, S])
    extends scaladsl.JdbcHandler[Envelope, S] {

  override def process(session: S, envelope: Envelope): Unit = {
    delegate.process(session, envelope)
  }

  override def start(): Future[Done] =
    delegate.start().toScala

  override def stop(): Future[Done] =
    delegate.stop().toScala
}
