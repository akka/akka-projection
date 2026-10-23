/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import java.util.Locale

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import akka.annotation.InternalApi
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ConfigHelpers {

  // missing path is treated as off since settings can be created from a hand-built Config without reference fallback
  def optionalDuration(config: Config, path: String): Option[FiniteDuration] =
    if (!config.hasPath(path)) None
    else
      config.getString(path).toLowerCase(Locale.ROOT) match {
        case "off" | "none" | "" => None
        case _                   => Some(config.getDuration(path).toScala)
      }
}
