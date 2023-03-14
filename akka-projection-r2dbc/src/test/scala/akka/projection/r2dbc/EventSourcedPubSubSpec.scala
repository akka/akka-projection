/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.r2dbc.scaladsl.R2dbcSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object EventSourcedPubSubSpec {

  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc {
      journal.publish-events = on
      query {
        refresh-interval = 3 seconds
        # simulate lost messages by overflowing the buffer
        buffer-size = 10

        backtracking {
          behind-current-time = 5 seconds
          window = 20 seconds
        }
      }
    }
    """)
    .withFallback(TestConfig.config)

  final case class Processed(projectionId: ProjectionId, envelope: EventEnvelope[String])

  class TestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      whenDone: EventEnvelope[String] => Future[Done])(implicit ec: ExecutionContext)
      extends R2dbcHandler[EventEnvelope[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(session: R2dbcSession, envelope: EventEnvelope[String]): Future[Done] = {
      whenDone(envelope).map { _ =>
        val timestampOffset = envelope.offset.asInstanceOf[TimestampOffset]
        val directReplication = timestampOffset.timestamp == timestampOffset.readTimestamp
        log.debugN(
          "{} Processed {}, pid {}, seqNr {}, direct {}",
          projectionId.key,
          envelope.event,
          envelope.persistenceId,
          envelope.sequenceNr,
          directReplication)
        probe ! Processed(projectionId, envelope)
        Done
      }
    }
  }

}

class EventSourcedPubSubSpec
    extends ScalaTestWithActorTestKit(EventSourcedPubSubSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventSourcedEndToEndSpec.Persister
  import EventSourcedPubSubSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  private val log = LoggerFactory.getLogger(getClass)

  private val projectionSettings = R2dbcProjectionSettings(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def startProjections(
      entityType: String,
      projectionName: String,
      nrOfProjections: Int,
      processedProbe: ActorRef[Processed],
      whenDone: EventEnvelope[String] => Future[Done]): Vector[ActorRef[ProjectionBehavior.Command]] = {
    val sliceRanges = EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, nrOfProjections)

    sliceRanges.map { range =>
      val projectionId = ProjectionId(projectionName, s"${range.min}-${range.max}")
      val sourceProvider =
        EventSourcedProvider.eventsBySlices[String](
          system,
          R2dbcReadJournal.Identifier,
          entityType,
          range.min,
          range.max)
      val projection = R2dbcProjection
        .exactlyOnce(
          projectionId,
          Some(projectionSettings),
          sourceProvider = sourceProvider,
          handler = () => new TestHandler(projectionId, processedProbe.ref, whenDone))
      spawn(ProjectionBehavior(projection))
    }.toVector
  }

  private def mkEvent(n: Int): String = {
    val template = "0000000"
    val s = n.toString
    "e" + (template + s).takeRight(5)
  }

  def expectProcessed(processedProbe: TestProbe[Processed], expectedFrom: Int, expectedTo: Int): Vector[Processed] = {
    val numberOfEvents = expectedTo - expectedFrom + 1
    var processed = Vector.empty[Processed]
    val expectedEvents = (1 to numberOfEvents).map(mkEvent).toVector
    (1 to numberOfEvents).foreach { _ =>
      // not using receiveMessages(expectedEvents) for better logging in case of failure
      try {
        processed :+= processedProbe.receiveMessage(25.seconds)
      } catch {
        case e: AssertionError =>
          val missing = expectedEvents.diff(processed.map(_.envelope.event))
          log.error(s"Processed [${processed.size}] events, but expected [$numberOfEvents]. " +
          s"Missing [${missing.mkString(",")}]. " +
          s"Received [${processed.map(p => s"(${p.envelope.event}, ${p.envelope.persistenceId}, ${p.envelope.sequenceNr})").mkString(", ")}]. ")
          throw e
      }
    }
    processed
  }

  "A R2DBC projection with eventsBySlices source and publish-events" must {

    "handle all events exactlyOnce" in {
      val numberOfEntities = 20
      val numberOfEvents = numberOfEntities * 10
      val entityType = nextEntityType()

      val entities = (0 until numberOfEntities).map { n =>
        val persistenceId = PersistenceId(entityType, s"p$n")
        spawn(Persister(persistenceId), s"p$n")
      }

      // write some before starting the projections
      (1 to 10).foreach { n =>
        val p = n % numberOfEntities
        entities(p) ! Persister.Persist(mkEvent(n))
      }

      val projectionName = UUID.randomUUID().toString
      val processedProbe = createTestProbe[Processed]()
      val slowEvents = Set(mkEvent(31), mkEvent(32), mkEvent(33))
      val whenDone: EventEnvelope[String] => Future[Done] = { env =>
        if (slowEvents.contains(env.event))
          akka.pattern.after(500.millis)(Future.successful(Done))
        else
          Future.successful(Done)
      }
      val projections = startProjections(entityType, projectionName, nrOfProjections = 4, processedProbe.ref, whenDone)

      // give them some time to start before writing more events, but it should work anyway
      Thread.sleep(500)

      (11 to 20).foreach { n =>
        val p = n % numberOfEntities
        entities(p) ! Persister.Persist(mkEvent(n))
      }

      var processed = Vector.empty[Processed]
      processed ++= expectProcessed(processedProbe, 1, 20)

      (21 to 30).foreach { n =>
        val p = n % numberOfEntities
        entities(p) ! Persister.Persist(mkEvent(n))
      }
      processed ++= expectProcessed(processedProbe, 21, 30)

      // Processing of 31 is slow in the handler, see whenDone above.
      // This will overflow the buffer for the subscribers, simulating lost messages,
      // but they should be picked by the queries.
      (31 to numberOfEvents - 10).foreach { n =>
        val p = n % numberOfEntities
        entities(p) ! Persister.Persist(mkEvent(n))
      }
      processed ++= expectProcessed(processedProbe, 31, numberOfEvents - 10)

      (numberOfEvents - 10 + 1 to numberOfEvents).foreach { n =>
        val p = n % numberOfEntities
        entities(p) ! Persister.Persist(mkEvent(n))
      }

      processed ++= expectProcessed(processedProbe, numberOfEvents - 10 + 1, numberOfEvents)

      val byPid = processed.groupBy(_.envelope.persistenceId)
      byPid.foreach { case (pid, processedByPid) =>
        // all events of a pid must be processed by the same projection instance
        processedByPid.map(_.projectionId).toSet.size shouldBe 1
        // processed events in right order
        processedByPid.map(_.envelope.sequenceNr) shouldBe (1 to processedByPid.size).toVector

        val viaPubSub =
          processedByPid.filter(p =>
            p.envelope.offset.asInstanceOf[TimestampOffset].timestamp == p.envelope.offset
              .asInstanceOf[TimestampOffset]
              .readTimestamp)
        log.info2("via pub-sub {}: {}", pid, viaPubSub.map(_.envelope.sequenceNr).mkString(", "))
      }

      val countViaPubSub = processed.count(p =>
        p.envelope.offset.asInstanceOf[TimestampOffset].timestamp == p.envelope.offset
          .asInstanceOf[TimestampOffset]
          .readTimestamp)
      log.info("Total via pub-sub: {}", countViaPubSub)
      countViaPubSub shouldBe >(0)

      projections.foreach(_ ! ProjectionBehavior.Stop)
    }
  }

}
