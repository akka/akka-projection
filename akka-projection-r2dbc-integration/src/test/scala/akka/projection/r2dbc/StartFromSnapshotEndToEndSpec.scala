/*
 * Copyright (C) 2022-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.query.typed.EventEnvelope
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
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.concurrent.Future

object StartFromSnapshotEndToEndSpec {

  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.snapshot-store.plugin = "akka.persistence.r2dbc.snapshot"
    akka.persistence.r2dbc {
      query {
        refresh-interval = 500 millis
        # stress more by using a small buffer (sql limit)
        buffer-size = 10

        backtracking.behind-current-time = 5 seconds

        start-from-snapshot.enabled = true
      }
    }
    """)
    .withFallback(TestConfig.config)

  object Persister {
    sealed trait Command
    final case class PersistWithAck(payload: String, replyTo: ActorRef[Done]) extends Command
    final case class Ping(replyTo: ActorRef[Done]) extends Command
    final case class Stop(replyTo: ActorRef[Done]) extends Command

    def apply(pid: PersistenceId): Behavior[Command] = {
      Behaviors.setup { context =>
        EventSourcedBehavior[Command, String, String](
          persistenceId = pid,
          "", { (_, command) =>
            command match {
              case command: PersistWithAck =>
                context.log.debug(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload).thenRun(_ => command.replyTo ! Done)
              case Ping(replyTo) =>
                replyTo ! Done
                Effect.none
              case Stop(replyTo) =>
                replyTo ! Done
                Effect.stop()
            }
          },
          (state, evt) => if (state.isBlank) evt else s"$state,$evt").snapshotWhen((_, evt, _) =>
          evt.endsWith("snapit!"))
      }
    }
  }

  sealed trait HandlerEvt
  final case class Processed(projectionId: ProjectionId, envelope: EventEnvelope[String]) extends HandlerEvt
  case object Stopped extends HandlerEvt

  class TestHandler(projectionId: ProjectionId, probe: ActorRef[HandlerEvt])
      extends R2dbcHandler[EventEnvelope[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(session: R2dbcSession, envelope: EventEnvelope[String]): Future[Done] = {
      log.debug("{} Processed {}", projectionId.key, envelope.event)
      probe ! Processed(projectionId, envelope)
      Future.successful(Done)
    }

    override def stop(): Future[Done] = {
      probe ! Stopped
      Future.successful(Done)
    }
  }

}

class StartFromSnapshotEndToEndSpec
    extends ScalaTestWithActorTestKit(StartFromSnapshotEndToEndSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import StartFromSnapshotEndToEndSpec._

  override def typedSystem: ActorSystem[_] = system

  private val projectionSettings = R2dbcProjectionSettings(system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def startProjections(
      entityType: String,
      projectionName: String,
      nrOfProjections: Int,
      processedProbe: ActorRef[HandlerEvt]): Vector[ActorRef[ProjectionBehavior.Command]] = {
    val sliceRanges = EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, nrOfProjections)

    sliceRanges.map { range =>
      val projectionId = ProjectionId(projectionName, s"${range.min}-${range.max}")
      val sourceProvider =
        EventSourcedProvider
          .eventsBySlicesStartingFromSnapshots[String, String](
            system,
            R2dbcReadJournal.Identifier,
            entityType,
            range.min,
            range.max,
            identity)
      val projection = R2dbcProjection
        .exactlyOnce(
          projectionId,
          Some(projectionSettings),
          sourceProvider = sourceProvider,
          handler = () => new TestHandler(projectionId, processedProbe.ref))
      spawn(ProjectionBehavior(projection))
    }.toVector
  }

  s"A R2DBC projection starting from snapshots (dialect ${r2dbcSettings.dialectName})" must {

    "work when no previous events seen" in {
      val entityType = nextEntityType()

      val persistenceId = PersistenceId(entityType, s"p1")
      val entity = spawn(Persister(persistenceId), s"$entityType-p1")

      // write some before starting the projections
      val ackProbe = createTestProbe[Done]()

      (1 to 5).foreach { n =>
        entity ! Persister.PersistWithAck(n.toString, ackProbe.ref)
      }
      entity ! Persister.PersistWithAck("6-snapit!", ackProbe.ref)

      ackProbe.receiveMessages(6)

      val projectionName = UUID.randomUUID().toString
      val processedProbe = createTestProbe[HandlerEvt]()
      val projections = startProjections(entityType, projectionName, nrOfProjections = 1, processedProbe.ref)

      val firstSeenEnvelope = processedProbe.expectMessageType[Processed].envelope
      // full state
      firstSeenEnvelope.event should ===("1,2,3,4,5,6-snapit!")
      firstSeenEnvelope.sequenceNr should ===(6L)

      // persist events after snapshot
      entity ! Persister.PersistWithAck("7", ackProbe.ref)
      ackProbe.receiveMessage()

      val afterSnap = processedProbe.expectMessageType[Processed]
      afterSnap.envelope.event should ===("7")
      afterSnap.envelope.sequenceNr should ===(7L)

      projections.foreach(_ ! ProjectionBehavior.Stop)
      processedProbe.expectMessage(Stopped)
    }

    "work when previous events seen" in {
      val entityType = nextEntityType()

      val persistenceId = PersistenceId(entityType, s"p1")
      val entity = spawn(Persister(persistenceId), s"$entityType-p1")

      // write some before starting the projections
      val ackProbe = createTestProbe[Done]()

      (1 to 5).foreach { n =>
        entity ! Persister.PersistWithAck(n.toString, ackProbe.ref)
      }
      ackProbe.receiveMessages(5)

      val projectionName = UUID.randomUUID().toString
      val handlerProbe = createTestProbe[HandlerEvt]()
      val projections = startProjections(entityType, projectionName, nrOfProjections = 1, handlerProbe.ref)

      handlerProbe.receiveMessages(5)

      // pause projection
      projections.foreach(_ ! ProjectionBehavior.Stop)
      handlerProbe.expectMessage(Stopped)

      // trigger snapshot
      entity ! Persister.PersistWithAck("6", ackProbe.ref)
      ackProbe.receiveMessage()
      entity ! Persister.PersistWithAck("7snapit!", ackProbe.ref)
      ackProbe.receiveMessage()

      // restart projection
      val secondIncarnationOfProjections =
        startProjections(entityType, projectionName, nrOfProjections = 1, handlerProbe.ref)

      val afterSnap = handlerProbe.expectMessageType[Processed]
      // we now started with snap event even though there was one inbetween (seqNr 6 lost)
      afterSnap.envelope.event should ===("1,2,3,4,5,6,7snapit!")
      afterSnap.envelope.sequenceNr should ===(7L)

      secondIncarnationOfProjections.foreach(_ ! ProjectionBehavior.Stop)
      handlerProbe.expectMessage(Stopped)
    }
  }

}
