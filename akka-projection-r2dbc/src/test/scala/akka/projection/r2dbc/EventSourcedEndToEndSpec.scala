/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.util.UUID
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.r2dbc.scaladsl.R2dbcSession
import akka.serialization.SerializationExtension
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object EventSourcedEndToEndSpec {

  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc {
      query {
        refresh-interval = 500 millis
        # stress more by using a small buffer (sql limit)
        buffer-size = 10

        backtracking.behind-current-time = 5 seconds
      }
    }
    """)
    .withFallback(TestConfig.config)

  object Persister {
    sealed trait Command
    final case class Persist(payload: Any) extends Command
    final case class PersistWithAck(payload: Any, replyTo: ActorRef[Done]) extends Command
    final case class PersistAll(payloads: List[Any]) extends Command
    final case class Ping(replyTo: ActorRef[Done]) extends Command
    final case class Stop(replyTo: ActorRef[Done]) extends Command

    def apply(pid: PersistenceId): Behavior[Command] = {
      Behaviors.setup { context =>
        EventSourcedBehavior[Command, Any, String](
          persistenceId = pid,
          "",
          { (_, command) =>
            command match {
              case command: Persist =>
                context.log.debugN(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload)
              case command: PersistWithAck =>
                context.log.debugN(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload).thenRun(_ => command.replyTo ! Done)
              case command: PersistAll =>
                if (context.log.isDebugEnabled)
                  context.log.debugN(
                    "PersistAll [{}], pid [{}], seqNr [{}]",
                    command.payloads.mkString(","),
                    pid.id,
                    EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payloads)
              case Ping(replyTo) =>
                replyTo ! Done
                Effect.none
              case Stop(replyTo) =>
                replyTo ! Done
                Effect.stop()
            }
          },
          (_, _) => "")
      }
    }
  }

  final case class Processed(projectionId: ProjectionId, envelope: EventEnvelope[String])

  class TestHandler(projectionId: ProjectionId, probe: ActorRef[Processed])
      extends R2dbcHandler[EventEnvelope[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(session: R2dbcSession, envelope: EventEnvelope[String]): Future[Done] = {
      log.debug2("{} Processed {}", projectionId.key, envelope.event)
      probe ! Processed(projectionId, envelope)
      Future.successful(Done)
    }
  }

}

class EventSourcedEndToEndSpec
    extends ScalaTestWithActorTestKit(EventSourcedEndToEndSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventSourcedEndToEndSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  private val log = LoggerFactory.getLogger(getClass)

  private val journalSettings = new R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))
  private val projectionSettings = R2dbcProjectionSettings(system)
  private val stringSerializer = SerializationExtension(system).serializerFor(classOf[String])

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  // to be able to store events with specific timestamps
  private def writeEvent(persistenceId: String, seqNr: Long, timestamp: Instant, event: String): Unit = {
    log.debugN("Write test event [{}] [{}] [{}] at time [{}]", persistenceId, seqNr, event, timestamp)
    val insertEventSql = sql"""
      INSERT INTO ${journalSettings.journalTableWithSchema}
      (slice, entity_type, persistence_id, seq_nr, db_timestamp, writer, adapter_manifest, event_ser_id, event_ser_manifest, event_payload)
      VALUES (?, ?, ?, ?, ?, '', '', ?, '', ?)"""

    val entityType = PersistenceId.extractEntityType(persistenceId)
    val slice = persistenceExt.sliceForPersistenceId(persistenceId)

    val result = r2dbcExecutor.updateOne("test writeEvent") { connection =>
      connection
        .createStatement(insertEventSql)
        .bind(0, slice)
        .bind(1, entityType)
        .bind(2, persistenceId)
        .bind(3, seqNr)
        .bind(4, timestamp)
        .bind(5, stringSerializer.identifier)
        .bind(6, stringSerializer.toBinary(event))
    }
    result.futureValue shouldBe 1
  }

  private def startProjections(
      entityType: String,
      projectionName: String,
      nrOfProjections: Int,
      processedProbe: ActorRef[Processed]): Vector[ActorRef[ProjectionBehavior.Command]] = {
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
          handler = () => new TestHandler(projectionId, processedProbe.ref))
      spawn(ProjectionBehavior(projection))
    }.toVector
  }

  private def mkEvent(n: Int): String = {
    val template = "0000000"
    val s = n.toString
    "e" + (template + s).takeRight(5)
  }

  "A R2DBC projection with eventsBySlices source" must {

    "handle all events exactlyOnce" in {
      val numberOfEntities = 20
      val numberOfEvents = numberOfEntities * 10
      val entityType = nextEntityType()

      val entities = (0 until numberOfEntities).map { n =>
        val persistenceId = PersistenceId(entityType, s"p$n")
        spawn(Persister(persistenceId), s"p$n")
      }

      // write some before starting the projections
      var n = 1
      while (n <= 50) {
        val p = n % numberOfEntities
        // mix some persist 1 and persist 3 events
        if (n % 7 == 0) {
          entities(p) ! Persister.PersistAll((0 until 3).map(i => mkEvent(n + i)).toList)
          n += 3
        } else {
          entities(p) ! Persister.Persist(mkEvent(n))
          n += 1
        }
      }

      val projectionName = UUID.randomUUID().toString
      val processedProbe = createTestProbe[Processed]()
      val projections = startProjections(entityType, projectionName, nrOfProjections = 4, processedProbe.ref)

      // give them some time to start before writing more events
      Thread.sleep(500)

      while (n <= numberOfEvents) {
        val p = n % numberOfEntities
        entities(p) ! Persister.Persist(mkEvent(n))

        // stop projections
        if (n == numberOfEvents / 2) {
          val probe = createTestProbe()
          projections.foreach { ref =>
            ref ! ProjectionBehavior.Stop
            probe.expectTerminated(ref)
          }
        }

        // resume projections again
        if (n == (numberOfEvents / 2) + 20)
          startProjections(entityType, projectionName, nrOfProjections = 4, processedProbe.ref)

        if (n % 10 == 0)
          Thread.sleep(50)
        else if (n % 25 == 0)
          Thread.sleep(1500)

        n += 1
      }

      var processed = Vector.empty[Processed]
      val expectedEvents = (1 to numberOfEvents).map(mkEvent).toVector
      (1 to numberOfEvents).foreach { _ =>
        // not using receiveMessages(expectedEvents) for better logging in case of failure
        try {
          processed :+= processedProbe.receiveMessage(15.seconds)
        } catch {
          case e: AssertionError =>
            val missing = expectedEvents.diff(processed.map(_.envelope.event))
            log.error(s"Processed [${processed.size}] events, but expected [$numberOfEvents]. " +
            s"Missing [${missing.mkString(",")}]. " +
            s"Received [${processed.map(p => s"(${p.envelope.event}, ${p.envelope.persistenceId}, ${p.envelope.sequenceNr})").mkString(", ")}]. ")
            throw e
        }
      }

      val byPid = processed.groupBy(_.envelope.persistenceId)
      byPid.foreach { case (_, processedByPid) =>
        // all events of a pid must be processed by the same projection instance
        processedByPid.map(_.projectionId).toSet.size shouldBe 1
        // processed events in right order
        processedByPid.map(_.envelope.sequenceNr).toVector shouldBe (1 to processedByPid.size).toVector
      }

      projections.foreach(_ ! ProjectionBehavior.Stop)
    }

    "accept unknown sequence number if previous is old" in {
      val entityType = nextEntityType()
      val pid1 = nextPid(entityType)
      val pid2 = nextPid(entityType)
      val pid3 = nextPid(entityType)

      val startTime = Instant.now()
      val oldTime = startTime.minus(projectionSettings.timeWindow).minusSeconds(60)
      writeEvent(pid1, 1L, startTime, "e1-1")

      val projectionName = UUID.randomUUID().toString
      val processedProbe = createTestProbe[Processed]()
      val projection = startProjections(entityType, projectionName, nrOfProjections = 1, processedProbe.ref).head

      processedProbe.receiveMessage().envelope.event shouldBe "e1-1"

      // old event for pid2, seqN3. will not be picked up by backtracking because outside time window
      writeEvent(pid2, 3L, oldTime, "e2-3")
      // pid2, seqNr 3 is unknown when receiving 4 so will lookup timestamp of 3
      // and accept 4 because 3 was older than time window
      writeEvent(pid2, 4L, startTime.plusMillis(1), "e2-4")
      processedProbe.receiveMessage().envelope.event shouldBe "e2-4"

      // pid3, seqNr 6 is unknown when receiving 7 so will lookup 6, but not found
      // and that will be accepted (could have been deleted)
      writeEvent(pid3, 7L, startTime.plusMillis(2), "e3-7")
      processedProbe.receiveMessage().envelope.event shouldBe "e3-7"

      // pid3, seqNr 8 is missing (knows 7) when receiving 9
      writeEvent(pid3, 9L, startTime.plusMillis(4), "e3-9")
      processedProbe.expectNoMessage(journalSettings.querySettings.refreshInterval + 2000.millis)

      // but backtracking can fill in the gaps, backtracking will pick up pid3 seqNr 8 and 9
      writeEvent(pid3, 8L, startTime.plusMillis(3), "e3-8")
      val possibleDelay =
        journalSettings.querySettings.backtrackingBehindCurrentTime + journalSettings.querySettings.refreshInterval + processedProbe.remainingOrDefault
      processedProbe.receiveMessage(possibleDelay).envelope.event shouldBe "e3-8"
      processedProbe.receiveMessage(possibleDelay).envelope.event shouldBe "e3-9"

      projection ! ProjectionBehavior.Stop
    }
  }

}
