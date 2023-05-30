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
import akka.stream.Attributes
import akka.stream.FanInShape2
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.KillSwitch
import akka.stream.KillSwitches
import akka.stream.Outlet
import akka.stream.scaladsl.BroadcastHub
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.OptionVal
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
    val catchupSource =
      eventsBySlices[Any](entityType, minSlice, maxSlice, offset, firehose = false)
        .via(catchupKillSwitch.flow)

    val consumerKillSwitch = KillSwitches.shared("consumerKillSwitch")

    val firehoseSource = firehose.firehoseHub
      .map { env =>
        firehose.updateConsumerTracking(consumerId, timestampOffset(env).timestamp, consumerKillSwitch)
        env
      }

    import GraphDSL.Implicits._
    val catchupOrFirehose = GraphDSL.createGraph(catchupSource) { implicit b => r =>
      val merge = b.add(new CatchupOrFirehose(consumerId, catchupKillSwitch))
      r ~> merge.in1
      FlowShape(merge.in0, merge.out)
    }

    firehoseSource
      .via(catchupOrFirehose)
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

object CatchupOrFirehose {
  private sealed trait Mode
  private case object CatchUpOnly extends Mode
  private final case class Both(caughtUpTimestamp: Instant) extends Mode
  private case object FirehoseOnly extends Mode

  private val log = LoggerFactory.getLogger(classOf[CatchupOrFirehose])

}

class CatchupOrFirehose(consumerId: String, catchupKillSwitch: KillSwitch)
    extends GraphStage[FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]]] {
  import CatchupOrFirehose._
  import EventsBySlicesFirehose.isDurationGreaterThan
  import EventsBySlicesFirehose.timestampOffset

  override def initialAttributes = Attributes.name("CatchupOrFirehose")
  override val shape: FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]] =
    new FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]]("CatchupOrFirehose")
  def out: Outlet[EventEnvelope[Any]] = shape.out
  val in0: Inlet[EventEnvelope[Any]] = shape.in0
  val in1: Inlet[EventEnvelope[Any]] = shape.in1

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    // Without this the completion signalling would take one extra pull
    private def willShutDown: Boolean = isClosed(in0)

    private val firehoseInlet = new FirehoseInlet(in0)
    private val catchupInlet = new CatchupInlet(in1)

    private var mode: Mode = CatchUpOnly

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        tryPushOutput()
        tryPullAllIfNeeded()
      }
    })

    setHandler(in0, firehoseInlet)
    setHandler(in1, catchupInlet)

    private def tryPushOutput(): Unit = {
      def tryPushFirehoseValue(): Boolean =
        firehoseInlet.value match {
          case OptionVal.Some(env) =>
            firehoseInlet.value = OptionVal.None
            log.debug(
              "Consumer [{}] push from firehose [{}] seqNr [{}]",
              consumerId,
              env.persistenceId,
              env.sequenceNr
            ) // FIXME trace
            push(out, env)
            true
          case _ =>
            false
        }

      def tryPushCatchupValue(): Boolean =
        catchupInlet.value match {
          case OptionVal.Some(env) =>
            catchupInlet.value = OptionVal.None
            log.debug(
              "Consumer [{}] push from catchup [{}] seqNr [{}]",
              consumerId,
              env.persistenceId,
              env.sequenceNr
            ) // FIXME trace
            push(out, env)
            true
          case _ =>
            false
        }

      if (isAvailable(out)) {
        mode match {
          case FirehoseOnly =>
            // there can be one final value from catchup when switching to FirehoseOnly
            if (!tryPushCatchupValue())
              tryPushFirehoseValue()
          case Both(_) =>
            if (!tryPushFirehoseValue())
              tryPushCatchupValue()
          case CatchUpOnly =>
            tryPushCatchupValue()
        }
      }

      if (willShutDown) completeStage()
    }

    private def tryPullAllIfNeeded(): Unit = {
      if (isClosed(in0)) {
        completeStage()
      } else {
        if (!hasBeenPulled(in0) && firehoseInlet.value.isEmpty) {
          tryPull(in0)
        }
        if (mode != FirehoseOnly && !hasBeenPulled(in1) && catchupInlet.value.isEmpty) {
          tryPull(in1)
        }
      }
    }

    def isCaughtUp(env: EventEnvelope[Any]): Boolean = {
      if (env.source == "") {
        val offset = timestampOffset(env)
        firehoseInlet.firehoseOffset.timestamp != Instant.EPOCH && !firehoseInlet.firehoseOffset.timestamp
          .isAfter(offset.timestamp)
      } else
        false // don't look at pub-sub or backtracking events
    }

    private class FirehoseInlet(in: Inlet[EventEnvelope[Any]]) extends InHandler {
      var value: OptionVal[EventEnvelope[Any]] = OptionVal.None
      var firehoseOffset: TimestampOffset = TimestampOffset(Instant.EPOCH, Map.empty)

      def updateFirehoseOffset(env: EventEnvelope[Any]): Unit = {
        // don't look at pub-sub or backtracking events
        if (env.source == "") {
          val offset = timestampOffset(env)
          if (offset.timestamp.isAfter(firehoseOffset.timestamp))
            firehoseOffset = offset // update when newer
        }
      }

      override def onPush(): Unit = {
        if (value.isDefined)
          throw new IllegalStateException("FirehoseInlet.onPush but has already value. This is a bug.")

        val env = grab(in)

        mode match {
          case FirehoseOnly =>
            value = OptionVal.Some(env)
          case Both(_) =>
            updateFirehoseOffset(env)
            value = OptionVal.Some(env)
          case CatchUpOnly =>
            updateFirehoseOffset(env)
        }

        tryPushOutput()
        tryPullAllIfNeeded()
      }
    }

    private class CatchupInlet(in: Inlet[EventEnvelope[Any]]) extends InHandler {
      var value: OptionVal[EventEnvelope[Any]] = OptionVal.None

      override def onPush(): Unit = {
        if (value.isDefined)
          throw new IllegalStateException("CatchupInlet.onPush but has already value. This is a bug.")

        val env = grab(in)

        mode match {
          case CatchUpOnly =>
            if (isCaughtUp(env)) {
              val timestamp = timestampOffset(env).timestamp
              log.debug("Consumer [{}] caught up at [{}]", consumerId, timestamp)
              mode = Both(timestamp)
            }
            value = OptionVal.Some(env)

          case Both(caughtUpTimestamp) =>
            // don't look at pub-sub or backtracking events
            if (env.source == "") {
              val timestamp = timestampOffset(env).timestamp
              if (isCaughtUp(env) && isDurationGreaterThan(caughtUpTimestamp, timestamp, JDuration.ofSeconds(20))) {
                // FIXME config duration ^
                log.debug("Consumer [{}] switching to firehose only [{}]", consumerId, timestamp)
                catchupKillSwitch.shutdown()
                mode = FirehoseOnly
              }
              // FIXME do we need to fall back to CatchUpOnly if it's not caught up for a longer period of time?
            }

            value = OptionVal.Some(env)

          case FirehoseOnly =>
          // skip
        }

        tryPushOutput()
        tryPullAllIfNeeded()
      }

      override def onUpstreamFinish(): Unit = {
        // important to override onUpstreamFinish, otherwise it will close everything
        log.debug("Consumer [{}] catchup closed", consumerId)
      }
    }

  }

  override def toString = "CatchupOrFirehose"
}
