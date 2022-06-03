/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.{ Duration => JDuration }
import java.util.Locale

import scala.concurrent.duration._

import akka.util.JavaDurationConverters._
import akka.actor.typed.ActorSystem
import com.typesafe.config.Config

object R2dbcProjectionSettings {

  val DefaultConfigPath = "akka.projection.r2dbc"

  def apply(config: Config): R2dbcProjectionSettings = {
    val logDbCallsExceeding: FiniteDuration =
      config.getString("log-db-calls-exceeding").toLowerCase(Locale.ROOT) match {
        case "off" => -1.millis
        case _     => config.getDuration("log-db-calls-exceeding").asScala
      }

    R2dbcProjectionSettings(
      schema = Option(config.getString("offset-store.schema")).filterNot(_.trim.isEmpty),
      offsetTable = config.getString("offset-store.offset-table"),
      timestampOffsetTable = config.getString("offset-store.timestamp-offset-table"),
      managementTable = config.getString("offset-store.management-table"),
      useConnectionFactory = config.getString("use-connection-factory"),
      timeWindow = config.getDuration("offset-store.time-window"),
      keepNumberOfEntries = config.getInt("offset-store.keep-number-of-entries"),
      evictInterval = config.getDuration("offset-store.evict-interval"),
      deleteInterval = config.getDuration("offset-store.delete-interval"),
      logDbCallsExceeding)
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
    timeWindow: JDuration,
    keepNumberOfEntries: Int,
    evictInterval: JDuration,
    deleteInterval: JDuration,
    logDbCallsExceeding: FiniteDuration) {
  val offsetTableWithSchema: String = schema.map(_ + ".").getOrElse("") + offsetTable
  val timestampOffsetTableWithSchema: String = schema.map(_ + ".").getOrElse("") + timestampOffsetTable
  val managementTableWithSchema: String = schema.map(_ + ".").getOrElse("") + managementTable

  def isOffsetTableDefined: Boolean = offsetTable.nonEmpty
}
