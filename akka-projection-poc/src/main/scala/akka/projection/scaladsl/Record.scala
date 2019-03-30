/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

/**
 * This simulates a potential Envelope.
 */
case class Record(offset: Long, payload: String)