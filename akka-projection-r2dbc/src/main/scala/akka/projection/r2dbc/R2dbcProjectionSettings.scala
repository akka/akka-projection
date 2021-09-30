/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import akka.actor.typed.ActorSystem
import com.typesafe.config.Config

final class R2dbcProjectionSettings(config: Config) {

  val schema: Option[String] =
    Option(config.getString("offset-store.schema")).filterNot(_.trim.isEmpty)

  val table: String = config.getString("offset-store.table")

  val tableWithSchema: String = schema.map("." + _).getOrElse("") + table

  val managementTable: String = config.getString("offset-store.management-table")

  val useConnectionFactory: String = config.getString("use-connection-factory")

  val verboseLoggingEnabled: Boolean = config.getBoolean("debug.verbose-offset-store-logging")

}

object R2dbcProjectionSettings {

  val DefaultConfigPath = "akka.projection.r2dbc"

  def apply(config: Config): R2dbcProjectionSettings =
    new R2dbcProjectionSettings(config)

  def apply(system: ActorSystem[_]): R2dbcProjectionSettings =
    new R2dbcProjectionSettings(system.settings.config.getConfig(DefaultConfigPath))
}
