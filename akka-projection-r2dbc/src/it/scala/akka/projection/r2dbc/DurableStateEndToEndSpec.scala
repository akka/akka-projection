/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.util.UUID
import scala.concurrent.Future
import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.persistence.query.DurableStateChange
import akka.persistence.query.UpdatedDurableState
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.r2dbc.scaladsl.R2dbcSession
import akka.projection.state.scaladsl.DurableStateSourceProvider
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object DurableStateEndToEndSpec {

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

  object DurableStatePersister {
    import akka.persistence.typed.state.scaladsl.Effect

    sealed trait Command
    final case class Persist(payload: Any) extends Command
    final case class PersistWithAck(payload: Any, replyTo: ActorRef[Done]) extends Command
    final case class Ping(replyTo: ActorRef[Done]) extends Command
    final case class Stop(replyTo: ActorRef[Done]) extends Command

    def apply(pid: String): Behavior[Command] =
      apply(PersistenceId.ofUniqueId(pid))

    def apply(pid: PersistenceId): Behavior[Command] = {
      Behaviors.setup { context =>
        DurableStateBehavior[Command, Any](persistenceId = pid, "", {
          (_, command) =>
            command match {
              case command: Persist =>
                context.log.debugN(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  DurableStateBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload)
              case command: PersistWithAck =>
                context.log.debugN(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  DurableStateBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload).thenRun(_ => command.replyTo ! Done)
              case Ping(replyTo) =>
                replyTo ! Done
                Effect.none
              case Stop(replyTo) =>
                replyTo ! Done
                Effect.stop()
            }
        })
      }
    }
  }

  class TestHandler(val projectionId: ProjectionId, val sliceRange: Range)
      extends R2dbcHandler[DurableStateChange[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    var processed = Vector.empty[DurableStateChange[String]]

    override def process(session: R2dbcSession, envelope: DurableStateChange[String]): Future[Done] = {
      envelope match {
        case upd: UpdatedDurableState[String] =>
          log.debugN("{} Processed {} revision {}", projectionId.key, upd.value, upd.revision)
        case _ =>
      }
      processed :+= envelope
      Future.successful(Done)
    }
  }

}

class DurableStateEndToEndSpec
    extends ScalaTestWithActorTestKit(DurableStateEndToEndSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import DurableStateEndToEndSpec._

  override def typedSystem: ActorSystem[_] = system

  private val settings = R2dbcProjectionSettings(testKit.system)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  private def createHandlers(projectionName: String, nrOfProjections: Int): Map[ProjectionId, TestHandler] = {
    val sliceRanges =
      DurableStateSourceProvider.sliceRanges(system, R2dbcDurableStateStore.Identifier, nrOfProjections)
    sliceRanges.map { range =>
      val projectionId = ProjectionId(projectionName, s"${range.min}-${range.max}")
      projectionId -> new TestHandler(projectionId, range)
    }.toMap
  }

  private def startProjections(
      entityType: String,
      projectionName: String,
      nrOfProjections: Int,
      handlers: Map[ProjectionId, TestHandler]): Vector[ActorRef[ProjectionBehavior.Command]] = {
    val sliceRanges =
      DurableStateSourceProvider.sliceRanges(system, R2dbcDurableStateStore.Identifier, nrOfProjections)

    sliceRanges.map { range =>
      val projectionId = ProjectionId(projectionName, s"${range.min}-${range.max}")
      val sourceProvider =
        DurableStateSourceProvider
          .changesBySlices[String](system, R2dbcDurableStateStore.Identifier, entityType, range.min, range.max)
      val projection = R2dbcProjection
        .exactlyOnce(
          projectionId,
          Some(settings),
          sourceProvider = sourceProvider,
          handler = () => handlers(projectionId))
      spawn(ProjectionBehavior(projection))
    }.toVector
  }

  s"A R2DBC projection with changesBySlices source (dialect ${r2dbcSettings.dialectName})" must {

    "handle latest updated state exactlyOnce" in {
      val numberOfEntities = 20
      val numberOfChanges = 10 * numberOfEntities
      val entityType = nextEntityType()

      val entities = (0 until numberOfEntities).map { n =>
        val persistenceId = PersistenceId(entityType, s"p$n")
        spawn(DurableStatePersister(persistenceId), s"p$n")
      }

      var revisionPerEntity = Map.empty[Int, Int]

      // write some before starting the projections
      (1 to 50).foreach { n =>
        val p = n % numberOfEntities
        val revision = revisionPerEntity.getOrElse(p, 0) + 1
        revisionPerEntity = revisionPerEntity.updated(p, revision)
        entities(p) ! DurableStatePersister.Persist(s"s$p-$revision")
      }

      val projectionName = UUID.randomUUID().toString
      val handlers = createHandlers(projectionName, nrOfProjections = 4)
      val projections = startProjections(entityType, projectionName, nrOfProjections = 4, handlers)

      // give them some time to start before writing more events
      Thread.sleep(500)

      var n = 51
      while (n <= numberOfChanges) {
        val p = n % numberOfEntities
        val revision = revisionPerEntity.getOrElse(p, 0) + 1
        revisionPerEntity = revisionPerEntity.updated(p, revision)
        entities(p) ! DurableStatePersister.Persist(s"s$p-$revision")

        // stop projections
        if (n == numberOfChanges / 2) {
          val probe = createTestProbe()
          projections.foreach { ref =>
            ref ! ProjectionBehavior.Stop
            probe.expectTerminated(ref)
          }
        }

        // resume projections again
        if (n == (numberOfChanges / 2) + 20)
          startProjections(entityType, projectionName, nrOfProjections = 4, handlers)

        if (n % 10 == 0)
          Thread.sleep(50)
        else if (n % 25 == 0)
          Thread.sleep(1500)

        n += 1
      }

      handlers.foreach {
        case (projectionId, handler) =>
          (0 until numberOfEntities).foreach { p =>
            val persistenceId = PersistenceId(entityType, s"p$p")
            val slice = DurableStateSourceProvider.sliceForPersistenceId(
              system,
              R2dbcDurableStateStore.Identifier,
              persistenceId.id)
            withClue(s"projectionId $projectionId, persistenceId $persistenceId, slice $slice: ") {
              if (handler.sliceRange.contains(slice)) {
                eventually {
                  val updates = handler.processed.collect {
                    case upd: UpdatedDurableState[String] if upd.persistenceId == persistenceId.id => upd
                  }
                  val revision = revisionPerEntity(p)
                  updates.last.revision shouldBe revision
                  updates.last.value shouldBe s"s$p-$revision"
                  // processed events in right order
                  updates shouldBe updates.sortBy(_.revision)
                }
              }
            }
          }
      }

      projections.foreach(_ ! ProjectionBehavior.Stop)
    }
  }

}
