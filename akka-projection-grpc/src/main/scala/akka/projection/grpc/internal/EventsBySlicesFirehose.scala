/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.nowarn
import scala.util.control.NoStackTrace

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.persistence.query.Offset
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Source
import org.slf4j.LoggerFactory

object EventsBySlicesFirehose extends ExtensionId[EventsBySlicesFirehose] {
  private val log = LoggerFactory.getLogger(classOf[EventsBySlicesFirehose])

  def createExtension(system: ActorSystem[_]): EventsBySlicesFirehose =
    new EventsBySlicesFirehose(system)

  // Java API
  def get(system: ActorSystem[_]): EventsBySlicesFirehose = apply(system)

  final class SlowConsumerException(message: String) extends RuntimeException(message) with NoStackTrace

  final case class FirehoseKey(entityType: String, sliceRange: Range)

  final case class ConsumerTracking(count: Long, timestamp: Instant, consumerKillSwitch: KillSwitch)

  class Firehose(val firehoseHub: Source[EventEnvelope[Any], NotUsed], bufferSize: Int) {
    val inCount: AtomicLong = new AtomicLong
    val consumerTracking: ConcurrentHashMap[String, ConsumerTracking] = new ConcurrentHashMap
    private val gapTreshold = math.max(2, bufferSize - 4)

    def updateConsumerTracking(consumerId: String, timestamp: Instant, consumerKillSwitch: KillSwitch): Unit = {
      val tracking = consumerTracking.get(consumerId) match {
        case null =>
          ConsumerTracking(count = 1, timestamp, consumerKillSwitch)
        case existing =>
          if (timestamp.compareTo(existing.timestamp) > 0)
            existing.copy(count = existing.count + 1, timestamp)
          else
            existing.copy(count = existing.count + 1)
      }
      // FIXME trace
      log.debug("Consumer [{}] count [{}], in count [{}]", consumerId, tracking.count, inCount.get)
      // no concurrent updates for a given consumerId
      consumerTracking.put(consumerId, tracking)

      val slowConsumers = findLaggingBehindMost()
      if (slowConsumers.nonEmpty) {
        log.info("[{}] slow consumers are aborted, behind by at least [{}].", slowConsumers.size, gapTreshold)
        // FIXME should wait a while until aborting slow consumers, might be temporary glitch?
        slowConsumers.foreach { tracking =>
          tracking.consumerKillSwitch.abort(new SlowConsumerException(s"Consumer is too slow."))
        }
      }
    }

    private def findLaggingBehindMost(): Vector[ConsumerTracking] = {
      import akka.util.ccompat.JavaConverters._
      val inCountValue = inCount.get
      consumerTracking.values.iterator.asScala.filter(inCountValue - _.count >= gapTreshold).toVector
    }
  }

  sealed trait WrappedEventEnvelope

  final case class FirehoseEventEnvelope(env: EventEnvelope[Any]) extends WrappedEventEnvelope
  final case class CatchupEventEnvelope(env: EventEnvelope[Any]) extends WrappedEventEnvelope

  def timestampOffset(env: EventEnvelope[Any]): TimestampOffset =
    env match {
      case eventEnvelope: EventEnvelope[_] if eventEnvelope.offset.isInstanceOf[TimestampOffset] =>
        eventEnvelope.offset.asInstanceOf[TimestampOffset]
      case _ =>
        throw new IllegalArgumentException(s"Expected TimestampOffset, but was [${env.offset.getClass.getName}]")
    }

  def isDurationGreaterThan(from: Instant, to: Instant, duration: JDuration): Boolean =
    JDuration.between(from, to).compareTo(duration) > 0
}

class EventsBySlicesFirehose(system: ActorSystem[_]) extends Extension {
  import EventsBySlicesFirehose._
  private val firehoses = new ConcurrentHashMap[FirehoseKey, Firehose]()

  def getFirehose(entityType: String, sliceRange: Range): Firehose =
    firehoses.computeIfAbsent(FirehoseKey(entityType, sliceRange), key => createFirehose(key))

  private def createFirehose(key: FirehoseKey): Firehose = {
    implicit val sys: ActorSystem[_] = system

    val bufferSize = 32 // FIXME config

    val firehoseHub =
      eventsBySlices[Any](
        key.entityType,
        key.sliceRange.min,
        key.sliceRange.max,
        TimestampOffset(Instant.now(), Map.empty),
        firehose = true)
        .map { env =>
          // increment counter before the hub
          firehoses.get(key) match {
            case null     => throw new IllegalStateException(s"Firehose [$key] not created. This is a bug.")
            case firehose => firehose.inCount.incrementAndGet()
          }
          env
        }
        .runWith(BroadcastHub.sink[EventEnvelope[Any]](bufferSize))

    // FIXME hub will be closed when last consumer is closed, need to detect that and start again (difficult concurrency)?

    new Firehose(firehoseHub, bufferSize)
  }

  // FIXME can this be in a ReadJournal
  def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {

    val firehose = getFirehose(entityType, minSlice to maxSlice)
    val consumerId = UUID.randomUUID().toString
    val catchupKillSwitch = KillSwitches.shared("catchupKillSwitch")
    val catchup =
      eventsBySlices[Event](entityType, minSlice, maxSlice, offset, firehose = false)
        .map {
          case env: EventEnvelope[Any] @unchecked => CatchupEventEnvelope(env)
        }
        .via(catchupKillSwitch.flow)

    val consumerKillSwitch = KillSwitches.shared("consumerKillSwitch")

    firehose.firehoseHub
      .map { env =>
        firehose.updateConsumerTracking(consumerId, timestampOffset(env).timestamp, consumerKillSwitch)
        FirehoseEventEnvelope(env)
      }
      .merge(catchup)
      .via(MergeSelector.flow(consumerId, catchupKillSwitch))
      .map(_.asInstanceOf[EventEnvelope[Event]])
      .via(consumerKillSwitch.flow)
  }

  // can be overridden in tests
  @nowarn("msg=never used") // firehose param used in tests
  protected def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset,
      firehose: Boolean): Source[EventEnvelope[Event], NotUsed] = {
    val queryPluginId = "akka.persistence.r2dbc.query"
    PersistenceQuery(system)
      .readJournalFor[EventsBySliceQuery](queryPluginId)
      .eventsBySlices(entityType, minSlice, maxSlice, offset)
  }

}

// FIXME maybe write as GraphStage instead
object MergeSelector {
  import EventsBySlicesFirehose._
  private sealed trait Mode
  private case object CatchUpOnly extends Mode
  private final case class Both(caughtUpTimestamp: Instant) extends Mode
  private case object FirehoseOnly extends Mode

  private val log = LoggerFactory.getLogger(classOf[MergeSelector.type])

  def flow(consumerId: String, catchupKillSwitch: KillSwitch): Flow[WrappedEventEnvelope, EventEnvelope[Any], NotUsed] =
    Flow[WrappedEventEnvelope].statefulMapConcat {
      var mode: Mode = CatchUpOnly
      var firehoseOffset: TimestampOffset = TimestampOffset(Instant.EPOCH, Map.empty)

      def updateFirehoseOffset(env: EventEnvelope[Any]): Unit = {
        val offset = timestampOffset(env)
        if (offset.timestamp.isAfter(firehoseOffset.timestamp))
          firehoseOffset = offset // update when newer
      }

      def isCaughtUp(env: EventEnvelope[Any]): Boolean = {
        val offset = timestampOffset(env)
        firehoseOffset.timestamp != Instant.EPOCH && !firehoseOffset.timestamp.isAfter(offset.timestamp)
      }

      () => {
        case FirehoseEventEnvelope(env) =>
          mode match {
            case CatchUpOnly =>
              updateFirehoseOffset(env)
              Nil // skip
            case Both(_) =>
              updateFirehoseOffset(env)
              env :: Nil // emit
            case FirehoseOnly =>
              env :: Nil // emit
          }

        case CatchupEventEnvelope(env) =>
          mode match {
            case CatchUpOnly =>
              if (isCaughtUp(env)) {
                val timestamp = timestampOffset(env).timestamp
                log.debug("Consumer [{}] caught up at [{}]", consumerId, timestamp)
                mode = Both(timestamp)
              }
              env :: Nil // emit
            case Both(caughtUpTimestamp) =>
              val timestamp = timestampOffset(env).timestamp
              if (isCaughtUp(env) && isDurationGreaterThan(caughtUpTimestamp, timestamp, JDuration.ofSeconds(20))) {
                // FIXME config duration ^
                log.debug("Consumer [{}] switching to firehose only [{}]", consumerId, timestamp)
                catchupKillSwitch.shutdown()
                mode = FirehoseOnly
              }
              // FIXME do we need to fall back to CatchUpOnly if it's not caught up for a longer period of time?

              env :: Nil // emit
            case FirehoseOnly =>
              Nil // skip
          }

      }
    }

}
