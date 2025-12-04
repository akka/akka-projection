/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.Locale

import scala.annotation.nowarn
import scala.concurrent.duration._
import scala.jdk.DurationConverters._

import akka.actor.typed.ActorSystem
import com.typesafe.config.Config
import io.r2dbc.spi.ConnectionFactory

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
        case "off" | "none" | "" => -1.millis
        case _                   => config.getDuration("log-db-calls-exceeding").toScala
      }

    val deleteInterval = config.getString("offset-store.delete-interval").toLowerCase(Locale.ROOT) match {
      case "off" | "none" | "" => JDuration.ZERO
      case _                   => config.getDuration("offset-store.delete-interval")
    }

    val adoptInterval = config.getString("offset-store.adopt-interval").toLowerCase(Locale.ROOT) match {
      case "off" | "none" | "" => JDuration.ZERO
      case _                   => config.getDuration("offset-store.adopt-interval")
    }

    val backtrackingWindow = config.getDuration("offset-store.backtracking-window")
    val timeWindow = {
      val d = config.getDuration("offset-store.time-window")
      // can't be less then backtrackingWindow
      if (d.compareTo(backtrackingWindow) < 0) backtrackingWindow
      else d
    }
    val deleteAfter = {
      val d = config.getDuration("offset-store.delete-after")
      // can't be less then timeWindow
      if (d.compareTo(timeWindow) < 0) timeWindow
      else d
    }

    val deleteAfterConsumed = config.getDuration("offset-store.delete-after-consumed")

    val acceptWhenPreviousTimestampBefore =
      config.getString("offset-store.accept-when-previous-timestamp-before") match {
        case "off" | "none" | "" => None
        case s                   => Some(Instant.parse(s))
      }

    val acceptSequenceNumberResetAfter =
      config.getString("offset-store.accept-sequence-number-reset-after").toLowerCase(Locale.ROOT) match {
        case "off" | "none" | "" => None
        case _                   => Some(config.getDuration("offset-store.accept-sequence-number-reset-after"))
      }

    new R2dbcProjectionSettings(
      schema = Option(config.getString("offset-store.schema")).filterNot(_.trim.isEmpty),
      offsetTable = config.getString("offset-store.offset-table"),
      timestampOffsetTable = config.getString("offset-store.timestamp-offset-table"),
      managementTable = config.getString("offset-store.management-table"),
      useConnectionFactory = config.getString("use-connection-factory"),
      timeWindow,
      backtrackingWindow,
      deleteAfter,
      deleteAfterConsumed,
      keepNumberOfEntries = 0,
      evictInterval = JDuration.ZERO,
      deleteInterval,
      adoptInterval,
      logDbCallsExceeding,
      warnAboutFilteredEventsInFlow = config.getBoolean("warn-about-filtered-events-in-flow"),
      offsetBatchSize = config.getInt("offset-store.offset-batch-size"),
      customConnectionFactory = None,
      offsetSliceReadParallelism = config.getInt("offset-store.offset-slice-read-parallelism"),
      offsetSliceReadLimit = config.getInt("offset-store.offset-slice-read-limit"),
      replayOnRejectedSequenceNumbers = config.getBoolean("replay-on-rejected-sequence-numbers"),
      acceptWhenPreviousTimestampBefore,
      acceptSequenceNumberResetAfter)
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
    val backtrackingWindow: JDuration,
    val deleteAfter: JDuration,
    val deleteAfterConsumed: JDuration,
    @deprecated("Not used, evict is only based on time window", "1.6.6")
    val keepNumberOfEntries: Int,
    @deprecated("Not used, evict is not periodic", "1.6.6")
    val evictInterval: JDuration,
    val deleteInterval: JDuration,
    val adoptInterval: JDuration,
    val logDbCallsExceeding: FiniteDuration,
    val warnAboutFilteredEventsInFlow: Boolean,
    val offsetBatchSize: Int,
    val customConnectionFactory: Option[ConnectionFactory],
    val offsetSliceReadParallelism: Int,
    val offsetSliceReadLimit: Int,
    val replayOnRejectedSequenceNumbers: Boolean,
    val acceptWhenPreviousTimestampBefore: Option[Instant],
    val acceptSequenceNumberResetAfter: Option[JDuration]) {

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
    copy(timeWindow = timeWindow.toJava)

  def withTimeWindow(timeWindow: JDuration): R2dbcProjectionSettings =
    copy(timeWindow = timeWindow)

  def withBacktrackingWindow(backtrackingWindow: FiniteDuration): R2dbcProjectionSettings =
    copy(backtrackingWindow = backtrackingWindow.toJava)

  def withBacktrackingWindow(backtrackingWindow: JDuration): R2dbcProjectionSettings =
    copy(backtrackingWindow = backtrackingWindow)

  def withDeleteAfter(deleteAfter: FiniteDuration): R2dbcProjectionSettings =
    copy(deleteAfter = deleteAfter.toJava)

  def withDeleteAfter(deleteAfter: JDuration): R2dbcProjectionSettings =
    copy(deleteAfter = deleteAfter)

  def withDeleteAfterConsumed(deleteAfterConsumed: FiniteDuration): R2dbcProjectionSettings =
    copy(deleteAfterConsumed = deleteAfterConsumed.toJava)

  def withDeleteAfterConsumed(deleteAfterConsumed: JDuration): R2dbcProjectionSettings =
    copy(deleteAfterConsumed = deleteAfterConsumed)

  @deprecated("Not used, evict is only based on time window", "1.6.6")
  def withKeepNumberOfEntries(keepNumberOfEntries: Int): R2dbcProjectionSettings =
    this

  @deprecated("Not used, evict is not periodic", "1.6.6")
  def withEvictInterval(evictInterval: FiniteDuration): R2dbcProjectionSettings =
    this

  @deprecated("Not used, evict is not periodic", "1.6.6")
  def withEvictInterval(evictInterval: JDuration): R2dbcProjectionSettings =
    this

  def withDeleteInterval(deleteInterval: FiniteDuration): R2dbcProjectionSettings =
    copy(deleteInterval = deleteInterval.toJava)

  def withDeleteInterval(deleteInterval: JDuration): R2dbcProjectionSettings =
    copy(deleteInterval = deleteInterval)

  def withAdoptInterval(adoptInterval: FiniteDuration): R2dbcProjectionSettings =
    copy(adoptInterval = adoptInterval.toJava)

  def withAdoptInterval(adoptInterval: JDuration): R2dbcProjectionSettings =
    copy(adoptInterval = adoptInterval)

  def withLogDbCallsExceeding(logDbCallsExceeding: FiniteDuration): R2dbcProjectionSettings =
    copy(logDbCallsExceeding = logDbCallsExceeding)

  def withLogDbCallsExceeding(logDbCallsExceeding: JDuration): R2dbcProjectionSettings =
    copy(logDbCallsExceeding = logDbCallsExceeding.toScala)

  def withWarnAboutFilteredEventsInFlow(warnAboutFilteredEventsInFlow: Boolean): R2dbcProjectionSettings =
    copy(warnAboutFilteredEventsInFlow = warnAboutFilteredEventsInFlow)

  def withOffsetBatchSize(offsetBatchSize: Int): R2dbcProjectionSettings =
    copy(offsetBatchSize = offsetBatchSize)

  def withCustomConnectionFactory(customConnectionFactory: ConnectionFactory): R2dbcProjectionSettings =
    copy(customConnectionFactory = Some(customConnectionFactory))

  def withOffsetSliceReadParallelism(offsetSliceReadParallelism: Int): R2dbcProjectionSettings =
    copy(offsetSliceReadParallelism = offsetSliceReadParallelism)

  def withOffsetSliceReadLimit(offsetSliceReadLimit: Int): R2dbcProjectionSettings =
    copy(offsetSliceReadLimit = offsetSliceReadLimit)

  def withReplayOnRejectedSequenceNumbers(replayOnRejectedSequenceNumbers: Boolean): R2dbcProjectionSettings =
    copy(replayOnRejectedSequenceNumbers = replayOnRejectedSequenceNumbers)

  def withAcceptWhenPreviousTimestampBefore(acceptWhenPreviousTimestampBefore: Instant): R2dbcProjectionSettings =
    copy(acceptWhenPreviousTimestampBefore = Option(acceptWhenPreviousTimestampBefore))

  def withAcceptSequenceNumberResetAfter(acceptSequenceNumberResetAfter: FiniteDuration): R2dbcProjectionSettings =
    copy(acceptSequenceNumberResetAfter = Some(acceptSequenceNumberResetAfter.toJava))

  def withAcceptSequenceNumberResetAfter(acceptSequenceNumberResetAfter: JDuration): R2dbcProjectionSettings =
    copy(acceptSequenceNumberResetAfter = Some(acceptSequenceNumberResetAfter))

  @nowarn("msg=deprecated")
  private def copy(
      schema: Option[String] = schema,
      offsetTable: String = offsetTable,
      timestampOffsetTable: String = timestampOffsetTable,
      managementTable: String = managementTable,
      useConnectionFactory: String = useConnectionFactory,
      timeWindow: JDuration = timeWindow,
      backtrackingWindow: JDuration = backtrackingWindow,
      deleteAfter: JDuration = deleteAfter,
      deleteAfterConsumed: JDuration = deleteAfterConsumed,
      deleteInterval: JDuration = deleteInterval,
      adoptInterval: JDuration = adoptInterval,
      logDbCallsExceeding: FiniteDuration = logDbCallsExceeding,
      warnAboutFilteredEventsInFlow: Boolean = warnAboutFilteredEventsInFlow,
      offsetBatchSize: Int = offsetBatchSize,
      customConnectionFactory: Option[ConnectionFactory] = customConnectionFactory,
      offsetSliceReadParallelism: Int = offsetSliceReadParallelism,
      offsetSliceReadLimit: Int = offsetSliceReadLimit,
      replayOnRejectedSequenceNumbers: Boolean = replayOnRejectedSequenceNumbers,
      acceptWhenPreviousTimestampBefore: Option[Instant] = acceptWhenPreviousTimestampBefore,
      acceptSequenceNumberResetAfter: Option[JDuration] = acceptSequenceNumberResetAfter) =
    new R2dbcProjectionSettings(
      schema,
      offsetTable,
      timestampOffsetTable,
      managementTable,
      useConnectionFactory,
      timeWindow,
      backtrackingWindow,
      deleteAfter,
      deleteAfterConsumed,
      keepNumberOfEntries,
      evictInterval,
      deleteInterval,
      adoptInterval,
      logDbCallsExceeding,
      warnAboutFilteredEventsInFlow,
      offsetBatchSize,
      customConnectionFactory,
      offsetSliceReadParallelism,
      offsetSliceReadLimit,
      replayOnRejectedSequenceNumbers,
      acceptWhenPreviousTimestampBefore,
      acceptSequenceNumberResetAfter)

  override def toString =
    s"R2dbcProjectionSettings($schema, $offsetTable, $timestampOffsetTable, $managementTable, $useConnectionFactory, $timeWindow, $deleteAfter, $deleteAfterConsumed, $deleteInterval, $logDbCallsExceeding, $warnAboutFilteredEventsInFlow, $offsetBatchSize, $customConnectionFactory, &offsetSliceReadParallelism, $offsetSliceReadLimit, $replayOnRejectedSequenceNumbers, $acceptWhenPreviousTimestampBefore, $acceptSequenceNumberResetAfter)"
}
