/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

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
import akka.persistence.query.PersistenceQuery
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider2
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.r2dbc.scaladsl.R2dbcSession
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object EndToEndSpec {

  val config: Config = ConfigFactory
    .parseString("""
    akka.persistence.r2dbc {
      query {
        refresh-interval = 1s
        # stress more by using a small buffer (sql limit)
        buffer-size = 10
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
                context.log.debug(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload)
              case command: PersistWithAck =>
                context.log.debug(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload).thenRun(_ => command.replyTo ! Done)
              case command: PersistAll =>
                if (context.log.isDebugEnabled)
                  context.log.debug(
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
      log.debug("{} Processed {}", projectionId.key, envelope.event)
      probe ! Processed(projectionId, envelope)
      Future.successful(Done)
    }
  }

}

class EndToEndSpec
    extends ScalaTestWithActorTestKit(EndToEndSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EndToEndSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  private val settings = R2dbcProjectionSettings(testKit.system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def startProjections(
      entityTypeHint: String,
      projectionName: String,
      nrOfProjections: Int,
      processedProbe: ActorRef[Processed]): Vector[ActorRef[ProjectionBehavior.Command]] = {
    val sliceRanges = EventSourcedProvider2.sliceRanges(system, R2dbcReadJournal.Identifier, nrOfProjections)

    sliceRanges.map { range =>
      val projectionId = ProjectionId(projectionName, s"${range.min}-${range.max}")
      val sourceProvider =
        EventSourcedProvider2.eventsBySlices[String](
          system,
          R2dbcReadJournal.Identifier,
          entityTypeHint,
          range.min,
          range.max)
      val projection = R2dbcProjection
        .exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider,
          handler = () => new TestHandler(projectionId, processedProbe.ref))
      spawn(ProjectionBehavior(projection))
    }.toVector
  }

  "A R2DBC projection with eventsBySlices source" must {

    "handle all events exactlyOnce" in {
      val numberOfEntities = 20
      val numberOfEvents = numberOfEntities * 10
      val entityTypeHint = nextEntityTypeHint()

      val entities = (0 until numberOfEntities).map { n =>
        val persistenceId = PersistenceId(entityTypeHint, s"p$n")
        spawn(Persister(persistenceId), s"p$n")
      }

      // write some before starting the projections
      var n = 1
      while (n <= 50) {
        val p = n % numberOfEntities
        // mix some persist 1 and persist 3 events
        if (n % 7 == 0) {
          entities(p) ! Persister.PersistAll((0 until 3).map(i => s"e$p-${n + i}").toList)
          n += 3
        } else {
          entities(p) ! Persister.Persist(s"e$p-$n")
          n += 1
        }
      }

      val projectionName = UUID.randomUUID().toString
      val processedProbe = createTestProbe[Processed]()
      var projections = startProjections(entityTypeHint, projectionName, nrOfProjections = 4, processedProbe.ref)

      // give them some time to start before writing more events
      Thread.sleep(500)

      while (n <= numberOfEvents) {
        val p = n % numberOfEntities
        entities(p) ! Persister.Persist(s"e$p-$n")

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
          startProjections(entityTypeHint, projectionName, nrOfProjections = 4, processedProbe.ref)

        if (n % 10 == 0)
          Thread.sleep(50)
        else if (n % 25 == 0)
          Thread.sleep(1500)

        n += 1
      }

      val processed = processedProbe.receiveMessages(numberOfEvents, 20.seconds)

      val byPid = processed.groupBy(_.envelope.persistenceId)
      byPid.foreach { case (_, processedByPid) =>
        // all events of a pid must be processed by the same projection instance
        processedByPid.map(_.projectionId).toSet.size shouldBe 1
        // processed events in right order
        processedByPid.map(_.envelope.sequenceNr).toVector shouldBe (1 to processedByPid.size).toVector
      }

      projections.foreach(_ ! ProjectionBehavior.Stop)
    }
  }

}
