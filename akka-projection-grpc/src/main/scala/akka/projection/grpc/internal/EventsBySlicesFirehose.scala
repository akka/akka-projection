/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import java.time.Instant
import java.time.{ Duration => JDuration }
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.annotation.nowarn
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.control.NoStackTrace

import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
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
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.util.OptionVal
import org.slf4j.LoggerFactory

object EventsBySlicesFirehose extends ExtensionId[EventsBySlicesFirehose] with ExtensionIdProvider {
  private val log = LoggerFactory.getLogger(classOf[EventsBySlicesFirehose])

  override def get(system: ActorSystem): EventsBySlicesFirehose = super.get(system)

  override def get(system: ClassicActorSystemProvider): EventsBySlicesFirehose = super.get(system)

  override def lookup = EventsBySlicesFirehose

  override def createExtension(system: ExtendedActorSystem): EventsBySlicesFirehose =
    new EventsBySlicesFirehose(system)

  final class SlowConsumerException(message: String) extends RuntimeException(message) with NoStackTrace

  final case class FirehoseKey(entityType: String, sliceRange: Range)

  final case class ConsumerTracking(
      consumerId: String,
      timestamp: Instant,
      firehoseOnly: Boolean,
      consumerKillSwitch: KillSwitch,
      slowConsumerCandidate: Option[Instant])

  final class Firehose(val firehoseHub: Source[EventEnvelope[Any], NotUsed], firehoseKillSwitch: KillSwitch) {
    val consumerTracking: ConcurrentHashMap[String, ConsumerTracking] = new ConcurrentHashMap
    @volatile private var firehoseIsShutdown = false

    def consumerStarted(consumerId: String, consumerKillSwitch: KillSwitch): Unit = {
      log.debug("Consumer [{}] started", consumerId)
      consumerTracking.putIfAbsent(
        consumerId,
        ConsumerTracking(consumerId, Instant.EPOCH, firehoseOnly = false, consumerKillSwitch, None))
    }

    def consumerTerminated(consumerId: String): Int = {
      log.debug("Consumer [{}] terminated", consumerId)
      consumerTracking.remove(consumerId)
      consumerTracking.size
    }

    def shutdownFirehoseIfNoConsumers(): Boolean = {
      if (consumerTracking.isEmpty) {
        log.debug("Shutdown firehose, no consumers")
        firehoseIsShutdown = true
        firehoseKillSwitch.shutdown()
        true
      } else
        false
    }

    def isShutdown: Boolean =
      firehoseIsShutdown

    @tailrec def updateConsumerTracking(
        consumerId: String,
        timestamp: Instant,
        consumerKillSwitch: KillSwitch): Unit = {

      val existingTracking = consumerTracking.get(consumerId)
      val tracking = existingTracking match {
        case null =>
          ConsumerTracking(consumerId, timestamp, firehoseOnly = false, consumerKillSwitch, None)
        case existing =>
          if (timestamp.isAfter(existing.timestamp))
            existing.copy(timestamp = timestamp)
          else
            existing
      }

      if (!consumerTracking.replace(consumerId, existingTracking, tracking)) {
        // concurrent update, try again
        updateConsumerTracking(consumerId, timestamp, consumerKillSwitch)
      }
    }

    def detectSlowConsumers(now: Instant): Unit = {
      import akka.util.ccompat.JavaConverters._
      val consumerTrackingValues = consumerTracking.values.iterator.asScala.toVector
      if (consumerTrackingValues.size > 1) {
        val slowestConsumer = consumerTrackingValues.minBy(_.timestamp)
        val fastestConsumer = consumerTrackingValues.maxBy(_.timestamp)

        val slowConsumers = consumerTrackingValues.collect {
          case t
              if t.firehoseOnly &&
              isDurationGreaterThan(t.timestamp, fastestConsumer.timestamp, JDuration.ofSeconds(5)) // FIXME config
              =>
            t.consumerId -> t
        }.toMap

        val changedConsumerTrackingValues = consumerTrackingValues.flatMap { tracking =>
          if (slowConsumers.contains(tracking.consumerId)) {
            if (tracking.slowConsumerCandidate.isDefined)
              None // keep original
            else
              Some(tracking.copy(slowConsumerCandidate = Some(now)))
          } else if (tracking.slowConsumerCandidate.isDefined) {
            Some(tracking.copy(slowConsumerCandidate = None)) // not slow any more
          } else {
            None
          }
        }

        changedConsumerTrackingValues.foreach { tracking =>
          consumerTracking.merge(
            tracking.consumerId,
            tracking,
            (existing, _) => existing.copy(slowConsumerCandidate = tracking.slowConsumerCandidate))
        }

        val newConsumerTrackingValues = consumerTracking.values.iterator.asScala.toVector

        // FIXME trace
        if (log.isDebugEnabled && newConsumerTrackingValues.iterator.map(_.timestamp).toSet.size > 1) {
          newConsumerTrackingValues.foreach { tracking =>
            val diffFastest = fastestConsumer.timestamp.toEpochMilli - tracking.timestamp.toEpochMilli
            val diffFastestStr =
              if (diffFastest > 0) s"behind fastest [$diffFastest] ms"
              else if (diffFastest < 0) s"ahead of fastest [$diffFastest] ms" // not possible
              else "same as fastest"
            val diffSlowest = slowestConsumer.timestamp.toEpochMilli - tracking.timestamp.toEpochMilli
            val diffSlowestStr =
              if (diffSlowest > 0) s"behind slowest [$diffSlowest] ms" // not possible
              else if (diffSlowest < 0) s"ahead of slowest [${-diffSlowest}] ms"
              else "same as slowest"
            log.debug(
              "Consumer [{}], {}, {}, firehoseOnly [{}]",
              tracking.consumerId,
              diffFastestStr,
              diffSlowestStr,
              tracking.firehoseOnly)
          }
        }

        val firehoseConsumerCount = newConsumerTrackingValues.count(_.firehoseOnly)
        val confirmedSlowConsumers = newConsumerTrackingValues.filter { tracking =>
          tracking.slowConsumerCandidate match {
            case None => false
            case Some(detectedTimestamp) =>
              isDurationGreaterThan(detectedTimestamp, now, JDuration.ofSeconds(3))
          }
        }

        if (confirmedSlowConsumers.nonEmpty && confirmedSlowConsumers.size < firehoseConsumerCount) {
          if (log.isInfoEnabled)
            log.info(
              "[{}] slow consumers are aborted [{}], behind by at least [{}] ms. [{}] firehose consumers, [{}] catchup consumers.",
              slowConsumers.size,
              slowConsumers.keysIterator.mkString(", "),
              JDuration
                .between(slowConsumers.valuesIterator.maxBy(_.timestamp).timestamp, fastestConsumer.timestamp)
                .toMillis,
              firehoseConsumerCount,
              newConsumerTrackingValues.size - firehoseConsumerCount)

          confirmedSlowConsumers.foreach { tracking =>
            tracking.consumerKillSwitch.abort(
              new SlowConsumerException(s"Consumer [${tracking.consumerId}] is too slow."))
          }
        }
      }
    }

    @tailrec def updateConsumerFirehoseOnly(consumerId: String): Unit = {
      val existingTracking = consumerTracking.get(consumerId)
      val tracking = existingTracking match {
        case null =>
          throw new IllegalStateException(s"Expected existing tracking for consumer [$consumerId]")
        case existing =>
          existing.copy(firehoseOnly = true)
      }

      if (!consumerTracking.replace(consumerId, existingTracking, tracking))
        // concurrent update, try again
        updateConsumerFirehoseOnly(consumerId)
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

class EventsBySlicesFirehose(system: ActorSystem) extends Extension {
  import EventsBySlicesFirehose._
  private val firehoses = new ConcurrentHashMap[FirehoseKey, Firehose]()

  // FIXME config
  system.scheduler.scheduleWithFixedDelay(2.seconds, 2.seconds) { () =>
    import akka.util.ccompat.JavaConverters._
    firehoses.values().asScala.foreach { firehose =>
      if (!firehose.isShutdown)
        firehose.detectSlowConsumers(Instant.now)
    }
  }(system.dispatcher)

  @tailrec final def getFirehose(entityType: String, sliceRange: Range): Firehose = {
    val key = FirehoseKey(entityType, sliceRange)
    val firehose = firehoses.computeIfAbsent(key, key => createFirehose(key))
    if (firehose.isShutdown) {
      // concurrency race condition, but it should be removed
      firehoses.remove(key, firehose)
      getFirehose(entityType, sliceRange) // try again
    } else
      firehose
  }

  private def createFirehose(key: FirehoseKey): Firehose = {
    implicit val sys: ActorSystem = system

    log.debug("Create firehose entityType [{}], sliceRange [{}]", key.entityType, key.sliceRange)

    val bufferSize = 256 // FIXME config

    val firehoseKillSwitch = KillSwitches.shared("firehoseKillSwitch")

    val firehoseHub =
      eventsBySlices[Any](
        key.entityType,
        key.sliceRange.min,
        key.sliceRange.max,
        TimestampOffset(Instant.now(), Map.empty),
        firehose = true)
        .via(firehoseKillSwitch.flow)
        .runWith(BroadcastHub.sink[EventEnvelope[Any]](bufferSize))

    // FIXME hub will be closed when last consumer is closed, need to detect that and start again (difficult concurrency)?

    new Firehose(firehoseHub, firehoseKillSwitch)
  }

  // FIXME can this be in a ReadJournal
  def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {
    val sliceRange = minSlice to maxSlice
    val firehose = getFirehose(entityType, sliceRange)
    val consumerId = UUID.randomUUID().toString

    def consumerTerminated(): Unit = {
      if (firehose.consumerTerminated(consumerId) == 0) {
        // Don't shutdown firehose immediately because Projection it should survive Projection restart
        system.scheduler.scheduleOnce(2.seconds) {
          // FIXME config ^
          if (firehose.shutdownFirehoseIfNoConsumers())
            firehoses.remove(FirehoseKey(entityType, sliceRange))
        }(system.dispatcher)
      }
    }

    val catchupKillSwitch = KillSwitches.shared("catchupKillSwitch")
    val catchupSource =
      eventsBySlices[Any](entityType, minSlice, maxSlice, offset, firehose = false)
        .via(catchupKillSwitch.flow)

    val consumerKillSwitch = KillSwitches.shared("consumerKillSwitch")

    val firehoseSource = firehose.firehoseHub
      .map { env =>
        // don't look at pub-sub or backtracking events
        if (env.source == "")
          firehose.updateConsumerTracking(consumerId, timestampOffset(env).timestamp, consumerKillSwitch)
        env
      }

    import GraphDSL.Implicits._
    val catchupOrFirehose = GraphDSL.createGraph(catchupSource) { implicit b => r =>
      val merge = b.add(new CatchupOrFirehose(consumerId, firehose, catchupKillSwitch))
      r ~> merge.in1
      FlowShape(merge.in0, merge.out)
    }

    firehoseSource
      .via(catchupOrFirehose)
      .map(_.asInstanceOf[EventEnvelope[Event]])
      .via(consumerKillSwitch.flow)
      .watchTermination()(Keep.right)
      .mapMaterializedValue { termination =>
        firehose.consumerStarted(consumerId, consumerKillSwitch)
        termination.onComplete { _ =>
          consumerTerminated()
        }(system.dispatcher)
        NotUsed
      }
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

class CatchupOrFirehose(consumerId: String, firehose: EventsBySlicesFirehose.Firehose, catchupKillSwitch: KillSwitch)
    extends GraphStage[FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]]] {
  import CatchupOrFirehose._
  import EventsBySlicesFirehose.isDurationGreaterThan
  import EventsBySlicesFirehose.timestampOffset

  override def initialAttributes = Attributes.name("CatchupOrFirehose")
  override val shape: FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]] =
    new FanInShape2[EventEnvelope[Any], EventEnvelope[Any], EventEnvelope[Any]]("CatchupOrFirehose")
  def out: Outlet[EventEnvelope[Any]] = shape.out
  val firehoseInlet: Inlet[EventEnvelope[Any]] = shape.in0
  val catchupInlet: Inlet[EventEnvelope[Any]] = shape.in1

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    // Without this the completion signalling would take one extra pull
    private def willShutDown: Boolean = isClosed(firehoseInlet)

    private val firehoseHandler = new FirehoseHandler(firehoseInlet)
    private val catchupHandler = new CatchupHandler(catchupInlet)

    private var mode: Mode = CatchUpOnly

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        tryPushOutput()
        tryPullAllIfNeeded()
      }
    })

    setHandler(firehoseInlet, firehoseHandler)
    setHandler(catchupInlet, catchupHandler)

    private def tryPushOutput(): Unit = {
      def tryPushFirehoseValue(): Boolean =
        firehoseHandler.value match {
          case OptionVal.Some(env) =>
            firehoseHandler.value = OptionVal.None
            log.debug(
              "Consumer [{}] push from firehose [{}] seqNr [{}], source [{}]",
              consumerId,
              env.persistenceId,
              env.sequenceNr,
              env.source
            ) // FIXME trace
            push(out, env)
            true
          case _ =>
            false
        }

      def tryPushCatchupValue(): Boolean =
        catchupHandler.value match {
          case OptionVal.Some(env) =>
            catchupHandler.value = OptionVal.None
            log.debug(
              "Consumer [{}] push from catchup [{}] seqNr [{}], source [{}]",
              consumerId,
              env.persistenceId,
              env.sequenceNr,
              env.source
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
      if (isClosed(firehoseInlet)) {
        completeStage()
      } else {
        if (!hasBeenPulled(firehoseInlet) && firehoseHandler.value.isEmpty) {
          tryPull(firehoseInlet)
        }
        if (mode != FirehoseOnly && !hasBeenPulled(catchupInlet) && catchupHandler.value.isEmpty) {
          tryPull(catchupInlet)
        }
      }
    }

    def isCaughtUp(env: EventEnvelope[Any]): Boolean = {
      if (env.source == "") {
        val offset = timestampOffset(env)
        firehoseHandler.firehoseOffset.timestamp != Instant.EPOCH && !firehoseHandler.firehoseOffset.timestamp
          .isAfter(offset.timestamp)
      } else
        false // don't look at pub-sub or backtracking events
    }

    private class FirehoseHandler(in: Inlet[EventEnvelope[Any]]) extends InHandler {
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

    private class CatchupHandler(in: Inlet[EventEnvelope[Any]]) extends InHandler {
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
              if (isDurationGreaterThan(caughtUpTimestamp, timestamp, JDuration.ofSeconds(10))) {
                firehose.updateConsumerFirehoseOnly(consumerId)
                // FIXME config duration ^
                log.debug("Consumer [{}] switching to firehose only [{}]", consumerId, timestamp)
                catchupKillSwitch.shutdown()
                mode = FirehoseOnly
              }
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
