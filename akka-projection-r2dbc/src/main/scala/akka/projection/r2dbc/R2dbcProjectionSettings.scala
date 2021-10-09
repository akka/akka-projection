/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.{ Duration => JDuration }
import akka.actor.typed.ActorSystem
import com.typesafe.config.Config

object R2dbcProjectionSettings {

  val DefaultConfigPath = "akka.projection.r2dbc"

  def apply(config: Config): R2dbcProjectionSettings = {
    R2dbcProjectionSettings(
      schema = Option(config.getString("offset-store.schema")).filterNot(_.trim.isEmpty),
      offsetTable = config.getString("offset-store.offset-table"),
      timestampOffsetTable = config.getString("offset-store.timestamp-offset-table"),
      managementTable = config.getString("offset-store.management-table"),
      useConnectionFactory = config.getString("use-connection-factory"),
      verboseLoggingEnabled = config.getBoolean("debug.verbose-offset-store-logging"),
      timeWindow = config.getDuration("offset-store.time-window"),
      evictInterval = config.getDuration("offset-store.evict-interval"),
      deleteInterval = config.getDuration("offset-store.delete-interval"),
      acceptNewSequenceNumberAfterAge = config.getDuration("offset-store.accept-new-sequence-number-after-age"))
  }

  def apply(system: ActorSystem[_]): R2dbcProjectionSettings =
    apply(system.settings.config.getConfig(DefaultConfigPath))
}

// FIXME remove case class, and add `with` methods
final case class R2dbcProjectionSettings(
    schema: Option[String],
    offsetTable: String,
    timestampOffsetTable: String,
    managementTable: String,
    useConnectionFactory: String,
    verboseLoggingEnabled: Boolean,
    timeWindow: JDuration,
    evictInterval: JDuration,
    deleteInterval: JDuration,
    acceptNewSequenceNumberAfterAge: JDuration) {
  val offsetTableWithSchema: String = schema.map("." + _).getOrElse("") + offsetTable
  val timestampOffsetTableWithSchema: String = schema.map("." + _).getOrElse("") + timestampOffsetTable
  val managementTableWithSchema: String = schema.map("." + _).getOrElse("") + managementTable
}
