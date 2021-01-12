/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.collection.immutable
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

  override def start(): Unit = delegate.start()
  override def stop(): Unit = delegate.stop()
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

  override def start(): Unit = delegate.start()
  override def stop(): Unit = delegate.stop()
}
