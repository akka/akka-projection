/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object CatchupSpec {

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.projection.r2dbc.replay-on-rejected-sequence-numbers=off
    akka.projection {
      restart-backoff {
        min-backoff = 20 ms
        max-backoff = 2 s
      }
    }
    """)
    .withFallback(TestConfig.config)

  final case class Processed(projectionId: ProjectionId, envelope: EventEnvelope[String])

  class FailingTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean],
      failEvents: ConcurrentHashMap[String, Int])
      extends Handler[EventEnvelope[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[String]): Future[Done] = {
      val failCount = failEvents.getOrDefault(envelope.event, 0)
      if (failCount > 0) {
        failEvents.put(envelope.event, failCount - 1)
        log.debug(
          "{} Fail event [{}], pid [{}], seqNr [{}]",
          projectionId.key,
          envelope.event,
          envelope.persistenceId,
          envelope.sequenceNr)
        throw TestException(s"Fail event [${envelope.event}]")
      } else {
        log.debug(
          "{} Processed {} [{}], pid [{}], seqNr [{}]",
          projectionId.key,
          if (processedEvents.containsKey(envelope.event)) "duplicate event" else "event",
          envelope.event,
          envelope.persistenceId,
          envelope.sequenceNr)
        val wasAbsent = processedEvents.putIfAbsent(envelope.event, true) == null
        // duplicates are ok but not reported to probe
        if (wasAbsent)
          probe ! Processed(projectionId, envelope)
        Future.successful(Done)
      }
    }
  }

}

class CatchupSpec
    extends ScalaTestWithActorTestKit(CatchupSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {
  import CatchupSpec._

  override def typedSystem: ActorSystem[_] = system

  private val log = LoggerFactory.getLogger(getClass)

  private val entityType = nextEntityType()
  private val sliceRange = 0 to 1023
  private val projectionId = genRandomProjectionId()

  private def slice(pid: String): Int = Persistence(system).sliceForPersistenceId(pid)

  def spawnAtLeastOnceProjection(handler: Handler[EventEnvelope[String]]): ActorRef[ProjectionBehavior.Command] = {
    val sourceProvider =
      EventSourcedProvider
        .eventsBySlices[String](system, R2dbcReadJournal.Identifier, entityType, sliceRange.min, sliceRange.max)

    spawn(
      ProjectionBehavior(
        R2dbcProjection
          .atLeastOnceAsync(projectionId, settings = None, sourceProvider = sourceProvider, handler = () => handler)))
  }

  "A Projection" must {
    "catchup old events without rejections and replays" in {
      // note config replay-on-rejected-sequence-numbers=off
      // so if there is an invalid rejection the test will fail

      // increase this to 50k for more thorough testing
      val numEvents = {
        if (r2dbcSettings.dialectName == "yugabyte")
          500
        else
          5000
      }
      val seed = System.currentTimeMillis()
      val rnd = new Random(seed)
      val t0 = InstantFactory.now().minus(10, ChronoUnit.DAYS)

      // corresponds to first backtracking window, and some more
      val moreThanBacktrackingWindow = r2dbcProjectionSettings.backtrackingWindow
        .plusMillis(r2dbcSettings.querySettings.backtrackingBehindCurrentTime.toMillis)
        .plusSeconds(10)

      val processedEvents = new ConcurrentHashMap[String, java.lang.Boolean]
      val failEvents = new ConcurrentHashMap[String, Int]
      val processedProbe = createTestProbe[Processed]()
      val handler = new FailingTestHandler(projectionId, processedProbe.ref, processedEvents, failEvents)

      var t = t0
      val numPids = 2 + rnd.nextInt(5)
      val pids = (1 to numPids).map(_ => nextPid(entityType))
      var seqNrs = pids.map(_ -> 0L).toMap

      log.info("Random seed [{}], using [{}] pids and [{}] events", seed, pids.size, numEvents)

      (1 to numEvents).foreach { _ =>

        val failEvent = rnd.nextDouble() < 0.01

        if (rnd.nextDouble() < 0.01)
          t = t.plus(moreThanBacktrackingWindow)
        else
          t = t.plusMillis(rnd.nextInt(100))

        val pid = pids(rnd.nextInt(pids.size))
        val seqNr = seqNrs(pid) + 1
        seqNrs = seqNrs.updated(pid, seqNr)
        val event = s"$pid-$seqNr"
        writeEvent(slice(pid), pid, seqNr, t, event)
        if (failEvent)
          failEvents.put(event, 1)
      }

      val projection = spawnAtLeastOnceProjection(handler)
      val processed = processedProbe.receiveMessages(numEvents, 10.seconds + (3 * numEvents).millis)
      val byPid = processed.groupBy(_.envelope.persistenceId)
      byPid.foreach {
        case (_, processedByPid) =>
          // all events of a pid must be processed by the same projection instance
          processedByPid.map(_.projectionId).toSet.size shouldBe 1
          // processed events in right order
          val processedSeqNrs = processedByPid.map(_.envelope.sequenceNr).toVector
          (processedSeqNrs.last - processedSeqNrs.head + 1) shouldBe processedByPid.size
          processedSeqNrs shouldBe (processedSeqNrs.head to processedSeqNrs.last).toVector
      }
      projection ! ProjectionBehavior.Stop
      createTestProbe().expectTerminated(projection)

    }

  }

}
