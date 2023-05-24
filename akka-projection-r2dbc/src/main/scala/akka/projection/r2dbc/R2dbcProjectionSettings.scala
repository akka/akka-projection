/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
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

  /**
   * Scala API: Load configuration from `akka.projection.r2dbc`.
   */
  def apply(system: ActorSystem[_]): R2dbcProjectionSettings =
    apply(system.settings.config.getConfig(DefaultConfigPath))

  /**
   * Java API: Load configuration from `akka.projection.r2dbc`.
   */
  def create(system: ActorSystem[_]): R2dbcProjectionSettings =
    apply(system)

  /**
   * Scala API: From custom configuration corresponding to `akka.projection.r2dbc`.
   */
  def apply(config: Config): R2dbcProjectionSettings = {
    val logDbCallsExceeding: FiniteDuration =
      config.getString("log-db-calls-exceeding").toLowerCase(Locale.ROOT) match {
        case "off" => -1.millis
        case _     => config.getDuration("log-db-calls-exceeding").asScala
      }

    new R2dbcProjectionSettings(
      schema = Option(config.getString("offset-store.schema")).filterNot(_.trim.isEmpty),
      offsetTable = config.getString("offset-store.offset-table"),
      timestampOffsetTable = config.getString("offset-store.timestamp-offset-table"),
      managementTable = config.getString("offset-store.management-table"),
      useConnectionFactory = config.getString("use-connection-factory"),
      timeWindow = config.getDuration("offset-store.time-window"),
      keepNumberOfEntries = config.getInt("offset-store.keep-number-of-entries"),
      evictInterval = config.getDuration("offset-store.evict-interval"),
      deleteInterval = config.getDuration("offset-store.delete-interval"),
      logDbCallsExceeding,
      warnAboutFilteredEventsInFlow = config.getBoolean("warn-about-filtered-events-in-flow"),
      offsetBatchSize = config.getInt("offset-store.offset-batch-size"))
  }

  /**
   * Java API: From custom configuration corresponding to `akka.projection.r2dbc`.
   */
  def create(config: Config): R2dbcProjectionSettings =
    apply(config)

}

final class R2dbcProjectionSettings private (
    val schema: Option[String],
    val offsetTable: String,
    val timestampOffsetTable: String,
    val managementTable: String,
    val useConnectionFactory: String,
    val timeWindow: JDuration,
    val keepNumberOfEntries: Int,
    val evictInterval: JDuration,
    val deleteInterval: JDuration,
    val logDbCallsExceeding: FiniteDuration,
    val warnAboutFilteredEventsInFlow: Boolean,
    val offsetBatchSize: Int) {

  val offsetTableWithSchema: String = schema.map(_ + ".").getOrElse("") + offsetTable
  val timestampOffsetTableWithSchema: String = schema.map(_ + ".").getOrElse("") + timestampOffsetTable
  val managementTableWithSchema: String = schema.map(_ + ".").getOrElse("") + managementTable

  def isOffsetTableDefined: Boolean = offsetTable.nonEmpty

  def withSchema(schema: String): R2dbcProjectionSettings = copy(schema = Some(schema))

  def withOffsetTable(offsetTable: String): R2dbcProjectionSettings = copy(offsetTable = offsetTable)

  def withTimestampOffsetTable(timestampOffsetTable: String): R2dbcProjectionSettings =
    copy(timestampOffsetTable = timestampOffsetTable)

  def withManagementTable(managementTable: String): R2dbcProjectionSettings =
    copy(managementTable = managementTable)

  def withUseConnectionFactory(useConnectionFactory: String): R2dbcProjectionSettings =
    copy(useConnectionFactory = useConnectionFactory)

  def withTimeWindow(timeWindow: FiniteDuration): R2dbcProjectionSettings =
    copy(timeWindow = timeWindow.asJava)

  def withTimeWindow(timeWindow: JDuration): R2dbcProjectionSettings =
    copy(timeWindow = timeWindow)

  def withKeepNumberOfEntries(keepNumberOfEntries: Int): R2dbcProjectionSettings =
    copy(keepNumberOfEntries = keepNumberOfEntries)

  def withEvictInterval(evictInterval: FiniteDuration): R2dbcProjectionSettings =
    copy(evictInterval = evictInterval.asJava)

  def withEvictInterval(evictInterval: JDuration): R2dbcProjectionSettings =
    copy(evictInterval = evictInterval)

  def withDeleteInterval(deleteInterval: FiniteDuration): R2dbcProjectionSettings =
    copy(deleteInterval = deleteInterval.asJava)

  def withDeleteInterval(deleteInterval: JDuration): R2dbcProjectionSettings =
    copy(deleteInterval = deleteInterval)

  def withLogDbCallsExceeding(logDbCallsExceeding: FiniteDuration): R2dbcProjectionSettings =
    copy(logDbCallsExceeding = logDbCallsExceeding)

  def withLogDbCallsExceeding(logDbCallsExceeding: JDuration): R2dbcProjectionSettings =
    copy(logDbCallsExceeding = logDbCallsExceeding.asScala)

  def withWarnAboutFilteredEventsInFlow(warnAboutFilteredEventsInFlow: Boolean): R2dbcProjectionSettings =
    copy(warnAboutFilteredEventsInFlow = warnAboutFilteredEventsInFlow)

  def withOffsetBatchSize(offsetBatchSize: Int): R2dbcProjectionSettings =
    copy(offsetBatchSize = offsetBatchSize)

  private def copy(
      schema: Option[String] = schema,
      offsetTable: String = offsetTable,
      timestampOffsetTable: String = timestampOffsetTable,
      managementTable: String = managementTable,
      useConnectionFactory: String = useConnectionFactory,
      timeWindow: JDuration = timeWindow,
      keepNumberOfEntries: Int = keepNumberOfEntries,
      evictInterval: JDuration = evictInterval,
      deleteInterval: JDuration = deleteInterval,
      logDbCallsExceeding: FiniteDuration = logDbCallsExceeding,
      warnAboutFilteredEventsInFlow: Boolean = warnAboutFilteredEventsInFlow,
      offsetBatchSize: Int = offsetBatchSize) =
    new R2dbcProjectionSettings(
      schema,
      offsetTable,
      timestampOffsetTable,
      managementTable,
      useConnectionFactory,
      timeWindow,
      keepNumberOfEntries,
      evictInterval,
      deleteInterval,
      logDbCallsExceeding,
      warnAboutFilteredEventsInFlow,
      offsetBatchSize)

  override def toString =
    s"R2dbcProjectionSettings($schema, $offsetTable, $timestampOffsetTable, $managementTable, $useConnectionFactory, $timeWindow, $keepNumberOfEntries, $evictInterval, $deleteInterval, $logDbCallsExceeding, $warnAboutFilteredEventsInFlow, $offsetBatchSize)"
}
