/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi
private[projection] case class SlickSettings(config: Config) {

  def schema: Option[String] =
    Option(config.getString("offset-store.schema")).filterNot(_.trim.isEmpty)
  def table: String = config.getString("offset-store.table")
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object SlickSettings {

  val configPath = "akka.projection.slick"

  def apply(system: ActorSystem[_]): SlickSettings =
    SlickSettings(system.classicSystem.settings.config.getConfig(configPath))

}
