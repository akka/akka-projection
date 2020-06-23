/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.collection.immutable
import scala.compat.java8.FutureConverters._
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.javadsl
import akka.projection.jdbc.scaladsl
import akka.util.ccompat.JavaConverters._

/**
 * INTERNAL API: Adapter from javadsl.JdbcHandler to scaladsl.JdbcHandler
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

/**
 * INTERNAL API: Adapter from `javadsl.Handler[java.util.List[Envelope]]` to `scaladsl.Handler[immutable.Seq[Envelope]]`
 */
@InternalApi private[projection] class GroupedJdbcHandlerAdapter[Envelope, S <: JdbcSession](
    delegate: javadsl.JdbcHandler[java.util.List[Envelope], S])
    extends scaladsl.JdbcHandler[immutable.Seq[Envelope], S] {

  override def process(session: S, envelopes: immutable.Seq[Envelope]): Unit = {
    delegate.process(session, envelopes.asJava)
  }

  override def start(): Future[Done] =
    delegate.start().toScala

  override def stop(): Future[Done] =
    delegate.stop().toScala
}
