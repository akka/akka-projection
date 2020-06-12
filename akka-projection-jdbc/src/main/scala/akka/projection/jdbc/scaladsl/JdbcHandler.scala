/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.scaladsl

import akka.projection.jdbc.JdbcSession
import akka.projection.scaladsl.HandlerLifecycle

trait JdbcHandler[Envelope, S <: JdbcSession] extends HandlerLifecycle {

  def process(session: S, envelope: Envelope): Unit

}
