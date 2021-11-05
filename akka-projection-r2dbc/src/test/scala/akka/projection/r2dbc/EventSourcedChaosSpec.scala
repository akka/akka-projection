/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider2
import akka.projection.r2dbc.EventSourcedChaosSpec.FailingTestHandler
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.r2dbc.scaladsl.R2dbcSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object EventSourcedChaosSpec {
  import akka.projection.r2dbc.EventSourcedEndToEndSpec.Processed

  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
        # Stress more by using a smaller buffer (sql limit) than default.
        # Note that it shouldn't be set too small since that would prevent progress if more than
        # this number of events are stored for the same timestamp.
        buffer-size = 100
      }
    }
    akka.projection {
      restart-backoff {
        min-backoff = 500 ms
        max-backoff = 2 s
      }
    }
    """)
    .withFallback(TestConfig.config)
    .withFallback(ConfigFactory.parseString("""
      # TODO in nightly CI we can set this to 10
      akka.projection.r2dbc.ChaosSpec.nr-iterations = 2
      """))

  class FailingTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      failEvents: ConcurrentHashMap[String, Int])
      extends R2dbcHandler[EventEnvelope[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(session: R2dbcSession, envelope: EventEnvelope[String]): Future[Done] = {
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
          "{} Processed event [{}], pid [{}], seqNr [{}]",
          projectionId.key,
          envelope.event,
          envelope.persistenceId,
          envelope.sequenceNr)
        probe ! Processed(projectionId, envelope)
        Future.successful(Done)
      }
    }
  }
}

class EventSourcedChaosSpec
    extends ScalaTestWithActorTestKit(EventSourcedChaosSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventSourcedEndToEndSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  private val settings = R2dbcProjectionSettings(testKit.system)
  private val log = LoggerFactory.getLogger(getClass)

  private val seed = System.currentTimeMillis()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def mkEvent(n: Int): String = {
    val template = "0000000"
    val s = n.toString
    "e" + (template + s).takeRight(5)
  }

  "A R2DBC projection under random conditions" must {

    "handle all events exactlyOnce" in {
      val rnd = new Random(seed)
      val numberOfIterations = system.settings.config.getInt("akka.projection.r2dbc.ChaosSpec.nr-iterations")
      val numberOfEntities = 20 + rnd.nextInt(80)
      val numberOfProjections = 1 << rnd.nextInt(4) // 1 to 8 projections
      val entityType = nextEntityType()
      val failProbability = 0.01
      val stopProbability = 0.01

      var startedEntities = Map.empty[Int, ActorRef[Persister.Command]]
      def entity(i: Int): ActorRef[Persister.Command] = {
        startedEntities.get(i) match {
          case Some(ref) => ref
          case None =>
            val persistenceId = PersistenceId(entityType, s"p$i")
            val ref = spawn(Persister(persistenceId), s"p$i")
            startedEntities = startedEntities.updated(i, ref)
            ref
        }
      }

      val projectionName = UUID.randomUUID().toString
      log.debug("Using [{}] projection instances, with name [{}]", numberOfProjections, projectionName)
      val processedProbe = createTestProbe[Processed]()
      val failEvents = new ConcurrentHashMap[String, Int]
      val probe = createTestProbe()

      var runningProjections = Map.empty[Int, ActorRef[ProjectionBehavior.Command]]
      def startProjection(projectionIndex: Int): ActorRef[ProjectionBehavior.Command] = {
        runningProjections.get(projectionIndex) match {
          case Some(ref) => ref
          case None =>
            val range = EventSourcedProvider2.sliceRanges(system, R2dbcReadJournal.Identifier, numberOfProjections)(
              projectionIndex)

            val projectionId = ProjectionId(projectionName, s"${range.min}-${range.max}")
            log.debug("Starting projection index [{}] with projectionId [{}]", projectionIndex, projectionId)
            val sourceProvider =
              EventSourcedProvider2
                .eventsBySlices[String](system, R2dbcReadJournal.Identifier, entityType, range.min, range.max)
            val projection = R2dbcProjection
              .exactlyOnce(
                projectionId,
                Some(settings),
                sourceProvider = sourceProvider,
                handler = () => new FailingTestHandler(projectionId, processedProbe.ref, failEvents))
            val ref = spawn(ProjectionBehavior(projection))
            runningProjections = runningProjections.updated(projectionIndex, ref)
            ref

        }
      }

      var verifiedEventCounter = 0
      var eventCounter = 0

      def verify(iteration: Int): Unit = {
        (0 until numberOfProjections).foreach(startProjection)

        val pingProbe = createTestProbe[Done]()
        startedEntities.valuesIterator.foreach { ref =>
          ref ! Persister.Ping(pingProbe.ref)
        }
        pingProbe.receiveMessages(startedEntities.size, 20.seconds)

        var processed = Vector.empty[Processed]
        val expectedEventCounts = eventCounter - verifiedEventCounter
        val expectedEvents = (verifiedEventCounter + 1 to eventCounter).map(mkEvent).toVector
        (1 to expectedEventCounts).foreach { _ =>
          // not using receiveMessages(expectedEvents) for better logging in case of failure
          try {
            processed :+= processedProbe.receiveMessage(15.seconds)
          } catch {
            case e: AssertionError =>
              val missing = expectedEvents.diff(processed.map(_.envelope.event))
              log.error(
                s"Iteration #$iteration. Processed [${processed.size}] events, but expected [$expectedEventCounts]. " +
                s"Missing [${missing.mkString(",")}]. " +
                s"Received [${processed.map(p => s"(${p.envelope.event}, ${p.envelope.persistenceId}, ${p.envelope.sequenceNr})").mkString(", ")}]. " +
                s"Seed [$seed].")
              throw e
          }
        }

        val processedEvents = processed.map(_.envelope.event)
        processedEvents.toSet shouldBe expectedEvents.toSet
        processedEvents.size shouldBe expectedEvents.size

        val byPid = processed.groupBy(_.envelope.persistenceId)
        val logMsg =
          s"Iteration #$iteration. Processed [$expectedEventCounts] events in [$numberOfProjections] projections from [${byPid.size}] pids."
        log.debug(logMsg)
        byPid.foreach { case (_, processedByPid) =>
          // all events of a pid must be processed by the same projection instance
          processedByPid.map(_.projectionId).toSet.size shouldBe 1
          // processed events in right order
          val seqNrs = processedByPid.map(_.envelope.sequenceNr).toVector
          (seqNrs.last - seqNrs.head + 1) shouldBe processedByPid.size
          seqNrs shouldBe (seqNrs.head to seqNrs.last).toVector
        }
        info(logMsg)

        verifiedEventCounter = eventCounter
      }

      for (iteration <- 1 to numberOfIterations) {
        log.info("Starting iteration [{}] with seed [{}]", iteration, seed)
        for (_ <- 1 to 200) {
          // Start projection
          if (rnd.nextDouble() < 0.1) {
            val projectionIndex = rnd.nextInt(numberOfProjections)
            startProjection(projectionIndex)
          }

          // Stop projection
          if (rnd.nextDouble() < stopProbability) {
            val projectionIndex = rnd.nextInt(numberOfProjections)
            runningProjections.get(projectionIndex).foreach { ref =>
              log.debug("Stopping projection [{}]", projectionIndex)
              ref ! ProjectionBehavior.Stop
              probe.expectTerminated(ref, 5.seconds)
              runningProjections -= projectionIndex
            }
          }

          // persist 1 event
          if (rnd.nextDouble() < 0.9) {
            val entityIndex = rnd.nextInt(numberOfEntities)
            eventCounter += 1
            val event = mkEvent(eventCounter)
            if (rnd.nextDouble() <= failProbability) {
              val failCount = 1 + rnd.nextInt(3)
              failEvents.put(event, failCount)
              log.debug("Persisting event [{}], it will fail in projection [{}] times", event, failCount)
            } else {
              log.debug("Persisting event [{}]", event)
            }
            entity(entityIndex) ! Persister.Persist(event)
          }

          // persist all (2 to 5) events
          if (rnd.nextDouble() < 0.1) {
            val entityIndex = rnd.nextInt(numberOfEntities)
            val numberOfEvents = 2 + rnd.nextInt(3)
            val events = (1 to numberOfEvents).map(e => mkEvent(e + eventCounter)).toList
            eventCounter += numberOfEvents
            if (rnd.nextDouble() < failProbability) {
              val failCount = 1 + rnd.nextInt(3)
              val i = rnd.nextInt(events.size)
              failEvents.put(events(i), failCount)
              log.debug(
                "Persisting events [{}], it will fail [{}] in projection [{}] times",
                events.mkString(", "),
                events(i),
                failCount)
            } else {
              log.debug("Persisting events [{}]", events.mkString(", "))
            }

            entity(entityIndex) ! Persister.PersistAll(events)
          }

          // short sleep
          if (rnd.nextDouble() <= 0.05) {
            Thread.sleep(100)
          }

          // long sleep
          if (rnd.nextDouble() <= 0.01) {
            log.debug("Sleeping")
            Thread.sleep(2000)
          }

        }

        verify(iteration)
      }

      runningProjections.valuesIterator.foreach(_ ! ProjectionBehavior.Stop)
    }
  }

}
