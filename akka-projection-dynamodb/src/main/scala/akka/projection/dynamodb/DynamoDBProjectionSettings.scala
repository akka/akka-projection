/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import java.time.{ Duration => JDuration }

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

import akka.actor.typed.ActorSystem
import akka.persistence.dynamodb.ConfigHelpers
import akka.persistence.dynamodb.WildcardMap
import com.typesafe.config.Config
import com.typesafe.config.ConfigObject

object DynamoDBProjectionSettings {

  val DefaultConfigPath = "akka.projection.dynamodb"

  /**
   * Scala API: Load configuration from `akka.projection.dynamodb`.
   */
  def apply(system: ActorSystem[_]): DynamoDBProjectionSettings =
    apply(system.settings.config.getConfig(DefaultConfigPath))

  /**
   * Java API: Load configuration from `akka.projection.dynamodb`.
   */
  def create(system: ActorSystem[_]): DynamoDBProjectionSettings =
    apply(system)

  /**
   * Scala API: From custom configuration corresponding to `akka.projection.dynamodb`.
   */
  def apply(config: Config): DynamoDBProjectionSettings = {
    new DynamoDBProjectionSettings(
      timestampOffsetTable = config.getString("offset-store.timestamp-offset-table"),
      useClient = config.getString("use-client"),
      timeWindow = config.getDuration("offset-store.time-window"),
      keepNumberOfEntries = config.getInt("offset-store.keep-number-of-entries"),
      evictInterval = config.getDuration("offset-store.evict-interval"),
      warnAboutFilteredEventsInFlow = config.getBoolean("warn-about-filtered-events-in-flow"),
      offsetBatchSize = config.getInt("offset-store.offset-batch-size"),
      timeToLiveSettings = TimeToLiveSettings(config.getConfig("time-to-live")))
  }

  /**
   * Java API: From custom configuration corresponding to `akka.projection.dynamodb`.
   */
  def create(config: Config): DynamoDBProjectionSettings =
    apply(config)

}

final class DynamoDBProjectionSettings private (
    val timestampOffsetTable: String,
    val useClient: String,
    val timeWindow: JDuration,
    val keepNumberOfEntries: Int,
    val evictInterval: JDuration,
    val warnAboutFilteredEventsInFlow: Boolean,
    val offsetBatchSize: Int,
    val timeToLiveSettings: TimeToLiveSettings) {

  def withTimestampOffsetTable(timestampOffsetTable: String): DynamoDBProjectionSettings =
    copy(timestampOffsetTable = timestampOffsetTable)

  def withUseClient(clientConfigPath: String): DynamoDBProjectionSettings =
    copy(useClient = clientConfigPath)

  def withTimeWindow(timeWindow: FiniteDuration): DynamoDBProjectionSettings =
    copy(timeWindow = timeWindow.toJava)

  def withTimeWindow(timeWindow: JDuration): DynamoDBProjectionSettings =
    copy(timeWindow = timeWindow)

  def withKeepNumberOfEntries(keepNumberOfEntries: Int): DynamoDBProjectionSettings =
    copy(keepNumberOfEntries = keepNumberOfEntries)

  def withEvictInterval(evictInterval: FiniteDuration): DynamoDBProjectionSettings =
    copy(evictInterval = evictInterval.toJava)

  def withEvictInterval(evictInterval: JDuration): DynamoDBProjectionSettings =
    copy(evictInterval = evictInterval)

  def withWarnAboutFilteredEventsInFlow(warnAboutFilteredEventsInFlow: Boolean): DynamoDBProjectionSettings =
    copy(warnAboutFilteredEventsInFlow = warnAboutFilteredEventsInFlow)

  def withOffsetBatchSize(offsetBatchSize: Int): DynamoDBProjectionSettings =
    copy(offsetBatchSize = offsetBatchSize)

  def withTimeToLiveSettings(timeToLiveSettings: TimeToLiveSettings): DynamoDBProjectionSettings =
    copy(timeToLiveSettings = timeToLiveSettings)

  private def copy(
      timestampOffsetTable: String = timestampOffsetTable,
      useClient: String = useClient,
      timeWindow: JDuration = timeWindow,
      keepNumberOfEntries: Int = keepNumberOfEntries,
      evictInterval: JDuration = evictInterval,
      warnAboutFilteredEventsInFlow: Boolean = warnAboutFilteredEventsInFlow,
      offsetBatchSize: Int = offsetBatchSize,
      timeToLiveSettings: TimeToLiveSettings = timeToLiveSettings) =
    new DynamoDBProjectionSettings(
      timestampOffsetTable,
      useClient,
      timeWindow,
      keepNumberOfEntries,
      evictInterval,
      warnAboutFilteredEventsInFlow,
      offsetBatchSize,
      timeToLiveSettings)

  override def toString =
    s"DynamoDBProjectionSettings($timestampOffsetTable, $useClient, $timeWindow, $keepNumberOfEntries, $evictInterval, $warnAboutFilteredEventsInFlow, $offsetBatchSize)"
}

object TimeToLiveSettings {
  val defaults: TimeToLiveSettings =
    new TimeToLiveSettings(projections = WildcardMap(Seq.empty, ProjectionTimeToLiveSettings.defaults))

  /**
   * Scala API: Create from configuration corresponding to `akka.projection.dynamodb.time-to-live`.
   */
  def apply(config: Config): TimeToLiveSettings = {
    val projections: WildcardMap[ProjectionTimeToLiveSettings] = {
      val defaults = config.getConfig("projection-defaults")
      val defaultSettings = ProjectionTimeToLiveSettings(defaults)
      val entries = config.getConfig("projections").root.entrySet.asScala
      val perEntitySettings = entries.toSeq.flatMap { entry =>
        (entry.getKey, entry.getValue) match {
          case (key: String, value: ConfigObject) =>
            val settings = ProjectionTimeToLiveSettings(value.toConfig.withFallback(defaults))
            Some(key -> settings)
          case _ => None
        }
      }
      WildcardMap(perEntitySettings, defaultSettings)
    }
    new TimeToLiveSettings(projections)
  }

  /**
   * Java API: Create from configuration corresponding to `akka.projection.dynamodb.time-to-live`.
   */
  def create(config: Config): TimeToLiveSettings = apply(config)
}

final class TimeToLiveSettings private (val projections: WildcardMap[ProjectionTimeToLiveSettings]) {

  def withProjection(name: String, settings: ProjectionTimeToLiveSettings): TimeToLiveSettings =
    copy(projections = projections.updated(name, settings))

  private def copy(projections: WildcardMap[ProjectionTimeToLiveSettings]): TimeToLiveSettings =
    new TimeToLiveSettings(projections)
}

object ProjectionTimeToLiveSettings {
  val defaults: ProjectionTimeToLiveSettings =
    new ProjectionTimeToLiveSettings(offsetTimeToLive = None)

  /**
   * Scala API: Create from configuration corresponding to `akka.projection.dynamodb.time-to-live.projections`.
   */
  def apply(config: Config): ProjectionTimeToLiveSettings =
    new ProjectionTimeToLiveSettings(offsetTimeToLive = ConfigHelpers.optDuration(config, "offset-time-to-live"))

  /**
   * Java API: Create from configuration corresponding to `akka.projection.dynamodb.time-to-live.projections`.
   */
  def create(config: Config): ProjectionTimeToLiveSettings = apply(config)
}

final class ProjectionTimeToLiveSettings private (val offsetTimeToLive: Option[FiniteDuration]) {

  def withOffsetTimeToLive(offsetTimeToLive: FiniteDuration): ProjectionTimeToLiveSettings =
    copy(offsetTimeToLive = Some(offsetTimeToLive))

  def withOffsetTimeToLive(offsetTimeToLive: JDuration): ProjectionTimeToLiveSettings =
    copy(offsetTimeToLive = Some(offsetTimeToLive.toScala))

  def withNoOffsetTimeToLive(): ProjectionTimeToLiveSettings =
    copy(offsetTimeToLive = None)

  private def copy(offsetTimeToLive: Option[FiniteDuration]): ProjectionTimeToLiveSettings =
    new ProjectionTimeToLiveSettings(offsetTimeToLive)
}
