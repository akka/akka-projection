/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset.toTimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.internal.Sql.InterpolationWithAdapter
import akka.persistence.r2dbc.internal.codec.TimestampCodec.TimestampCodecRichStatement
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.Projection
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.r2dbc.scaladsl.R2dbcSession
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.serialization.SerializationExtension
import akka.stream.scaladsl.FlowWithContext
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object EventSourcedEndToEndSpec {

  private val log = LoggerFactory.getLogger(classOf[EventSourcedEndToEndSpec])

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
        EventSourcedBehavior[Command, Any, String](persistenceId = pid, "", {
          (_, command) =>
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
        }, (_, _) => "")
      }
    }
  }

  final case class Processed(projectionId: ProjectionId, envelope: EventEnvelope[String])

  final case class StartParams(
      entityType: String,
      projectionName: String,
      nrOfProjections: Int,
      processedProbe: TestProbe[Processed])

  class AsyncTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean],
      exactlyOnce: Boolean = false)
      extends Handler[EventEnvelope[String]] {

    override def process(envelope: EventEnvelope[String]): Future[Done] = {
      log.debug(
        "{} Processed {} [{}], pid [{}], seqNr [{}]",
        projectionId.key,
        if (processedEvents.containsKey(envelope.event)) "duplicate event" else "event",
        envelope.event,
        envelope.persistenceId,
        envelope.sequenceNr)
      val wasAbsent = processedEvents.putIfAbsent(envelope.event, true) == null
      if (exactlyOnce || wasAbsent) {
        // if at-least-once, only mark processed the first time
        // if exactly-once, test will fail on duplicate processing
        probe ! Processed(projectionId, envelope)
      }
      Future.successful(Done)
    }
  }

  class R2dbcTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean],
      exactlyOnce: Boolean = true)
      extends R2dbcHandler[EventEnvelope[String]] {

    val delegate = new AsyncTestHandler(projectionId, probe, processedEvents, exactlyOnce)

    override def process(session: R2dbcSession, envelope: EventEnvelope[String]): Future[Done] =
      delegate.process(envelope)
  }

  class GroupedAsyncTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean])
      extends Handler[Seq[EventEnvelope[String]]] {

    val delegate = new AsyncTestHandler(projectionId, probe, processedEvents)

    override def process(envelopes: Seq[EventEnvelope[String]]): Future[Done] = {
      envelopes.foreach(delegate.process)
      Future.successful(Done)
    }
  }

  class GroupedR2dbcTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean])
      extends R2dbcHandler[Seq[EventEnvelope[String]]] {

    val delegate = new GroupedAsyncTestHandler(projectionId, probe, processedEvents)

    override def process(session: R2dbcSession, envelopes: Seq[EventEnvelope[String]]): Future[Done] =
      delegate.process(envelopes)
  }

  private def flowTestHandler(
      projectionId: ProjectionId,
      probe: ActorRef[Processed],
      processedEvents: ConcurrentHashMap[String, java.lang.Boolean]) =
    FlowWithContext[EventEnvelope[String], ProjectionContext].map { envelope =>
      log.debug(
        "{} Processed {} [{}], pid [{}], seqNr [{}]",
        projectionId.key,
        if (processedEvents.containsKey(envelope.event)) "duplicate event" else "event",
        envelope.event,
        envelope.persistenceId,
        envelope.sequenceNr)
      if (processedEvents.putIfAbsent(envelope.event, true) == null)
        probe ! Processed(projectionId, envelope)
      Done
    }

}

class EventSourcedEndToEndSpec
    extends ScalaTestWithActorTestKit(EventSourcedEndToEndSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfterEach
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventSourcedEndToEndSpec._

  override def typedSystem: ActorSystem[_] = system

  private val log = LoggerFactory.getLogger(getClass)

  private val journalSettings = R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))
  private val projectionSettings = R2dbcProjectionSettings(system)
  private val stringSerializer = SerializationExtension(system).serializerFor(classOf[String])

  import journalSettings.codecSettings.JournalImplicits._

  private var processedEventsPerProjection: Map[ProjectionId, ConcurrentHashMap[String, java.lang.Boolean]] = Map.empty

  override protected def beforeEach(): Unit = {
    processedEventsPerProjection = Map.empty
    super.beforeEach()
  }

  private def processedEvents(projectionId: ProjectionId): ConcurrentHashMap[String, java.lang.Boolean] = {
    processedEventsPerProjection.get(projectionId) match {
      case None =>
        val processedEvents = new ConcurrentHashMap[String, java.lang.Boolean]
        processedEventsPerProjection = processedEventsPerProjection.updated(projectionId, processedEvents)
        processedEvents
      case Some(processedEvents) =>
        processedEvents
    }
  }

  // to be able to store events with specific timestamps
  private def writeEvent(persistenceId: String, seqNr: Long, timestamp: Instant, event: String): Unit = {
    log.debug("Write test event [{}] [{}] [{}] at time [{}]", persistenceId, seqNr, event, timestamp)
    val insertEventSql = sql"""
      INSERT INTO ${journalSettings.journalTableWithSchema(0)}
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
        .bindTimestamp(4, timestamp)
        .bind(5, stringSerializer.identifier)
        .bind(6, stringSerializer.toBinary(event))
    }
    result.futureValue shouldBe 1
  }

  private def startExactlyOnceProjections(startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(projectionSettings),
            sourceProvider = sourceProvider,
            handler =
              () => new R2dbcTestHandler(projectionId, startParams.processedProbe.ref, processedEvents(projectionId))))
  }

  private def startAtLeastOnceProjections(startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        R2dbcProjection
          .atLeastOnce(
            projectionId,
            Some(projectionSettings),
            sourceProvider = sourceProvider,
            handler = () =>
              new R2dbcTestHandler(
                projectionId,
                startParams.processedProbe.ref,
                processedEvents(projectionId),
                exactlyOnce = false)))
  }

  private def startAtLeastOnceAsyncProjections(
      startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        R2dbcProjection
          .atLeastOnceAsync(
            projectionId,
            Some(projectionSettings),
            sourceProvider = sourceProvider,
            handler =
              () => new AsyncTestHandler(projectionId, startParams.processedProbe.ref, processedEvents(projectionId))))
  }

  private def startGroupedWithinProjections(startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        R2dbcProjection
          .groupedWithin(
            projectionId,
            Some(projectionSettings),
            sourceProvider = sourceProvider,
            handler = () =>
              new GroupedR2dbcTestHandler(projectionId, startParams.processedProbe.ref, processedEvents(projectionId)))
          .withGroup(3, groupAfterDuration = 200.millis))
  }

  private def startGroupedWithinAsyncProjections(
      startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        R2dbcProjection
          .groupedWithinAsync(
            projectionId,
            Some(projectionSettings),
            sourceProvider = sourceProvider,
            handler = () =>
              new GroupedAsyncTestHandler(projectionId, startParams.processedProbe.ref, processedEvents(projectionId)))
          .withGroup(3, groupAfterDuration = 200.millis))
  }

  private def startAtLeastOnceFlowProjections(
      startParams: StartParams): Vector[ActorRef[ProjectionBehavior.Command]] = {
    startProjections(
      startParams,
      (projectionId, sourceProvider) =>
        R2dbcProjection
          .atLeastOnceFlow(
            projectionId,
            Some(projectionSettings),
            sourceProvider = sourceProvider,
            flowTestHandler(projectionId, startParams.processedProbe.ref, processedEvents(projectionId))))
  }

  private def startProjections(
      startParams: StartParams,
      projectionFactory: (
          ProjectionId,
          SourceProvider[Offset, EventEnvelope[String]]) => Projection[EventEnvelope[String]])
      : Vector[ActorRef[ProjectionBehavior.Command]] = {
    import startParams._

    val sliceRanges = EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, nrOfProjections)

    sliceRanges.map { range =>
      val projectionId = ProjectionId(projectionName, s"${range.min}-${range.max}")

      val sourceProvider =
        EventSourcedProvider
          .eventsBySlices[String](system, R2dbcReadJournal.Identifier, entityType, range.min, range.max)

      val projection = projectionFactory(projectionId, sourceProvider)
      spawn(ProjectionBehavior(projection))
    }.toVector
  }

  private def mkEvent(n: Int): String = f"e$n%05d"

  private def assertEventsProcessed(
      expectedEvents: Vector[String],
      processedProbe: TestProbe[Processed],
      verifyProjectionId: Boolean): Unit = {
    val expectedNumberOfEvents = expectedEvents.size
    var processed = Vector.empty[Processed]

    (1 to expectedNumberOfEvents).foreach { _ =>
      // not using receiveMessages(expectedEvents) for better logging in case of failure
      try {
        processed :+= processedProbe.receiveMessage(15.seconds)
      } catch {
        case e: AssertionError =>
          val missing = expectedEvents.diff(processed.map(_.envelope.event))
          log.error(s"Processed [${processed.size}] events, but expected [$expectedNumberOfEvents]. " +
          s"Missing [${missing.mkString(",")}]. " +
          s"Received [${processed.map(p => s"(${p.envelope.event}, ${p.envelope.persistenceId}, ${p.envelope.sequenceNr})").mkString(", ")}]. ")
          throw e
      }
    }

    if (verifyProjectionId) {
      val byPid = processed.groupBy(_.envelope.persistenceId)
      byPid.foreach {
        case (pid, processedByPid) =>
          withClue(s"PersistenceId [$pid]: ") {
            // all events of a pid must be processed by the same projection instance
            processedByPid.map(_.projectionId).toSet.size shouldBe 1
            // processed events in right order
            processedByPid.map(_.envelope.sequenceNr) shouldBe (1 to processedByPid.size).toVector
          }
      }
    }
  }

  private def test(
      startParams: StartParams,
      startProjectionsFactory: () => Vector[ActorRef[ProjectionBehavior.Command]]): Unit = {
    val numberOfEntities = 20 // increase this for longer testing
    val numberOfEvents = numberOfEntities * 10

    val entities = (0 until numberOfEntities).map { n =>
      val persistenceId = PersistenceId(startParams.entityType, s"p$n")
      spawn(Persister(persistenceId), s"${startParams.entityType}-p$n")
    }

    // write some before starting the projections
    var n = 1
    while (n <= numberOfEvents / 4) {
      val p = n % numberOfEntities
      // mix some persist 1 and persist 3 events
      if (n % 7 == 0) {
        entities(p) ! Persister.PersistAll((0 until 3).map(i => mkEvent(n + i)).toList)
        n += 3
      } else {
        entities(p) ! Persister.Persist(mkEvent(n))
        n += 1
      }

      if (n % 10 == 0)
        Thread.sleep(50)
      else if (n % 25 == 0)
        Thread.sleep(1500)
    }

    var projections = startProjectionsFactory()

    // give them some time to start before writing more events
    Thread.sleep(500)

    while (n <= numberOfEvents) {
      val p = n % numberOfEntities
      entities(p) ! Persister.Persist(mkEvent(n))

      // stop projections
      if (n == numberOfEvents / 2) {
        projections.foreach { ref =>
          ref ! ProjectionBehavior.Stop
        }
      }

      // wait until stopped
      if (n == (numberOfEvents / 2) + 10) {
        val probe = createTestProbe()
        projections.foreach { ref =>
          probe.expectTerminated(ref)
        }
      }

      // resume projections again
      if (n == (numberOfEvents / 2) + 20)
        projections = startProjectionsFactory()

      if (n % 10 == 0)
        Thread.sleep(50)
      else if (n % 25 == 0)
        Thread.sleep(1500)

      n += 1
    }

    val expectedEvents = (1 to numberOfEvents).map(mkEvent).toVector
    assertEventsProcessed(expectedEvents, startParams.processedProbe, verifyProjectionId = true)

    projections.foreach(_ ! ProjectionBehavior.Stop)
    val probe = createTestProbe()
    projections.foreach { ref =>
      probe.expectTerminated(ref)
    }
  }

  private def newStartParams(): StartParams = {
    val entityType = nextEntityType()
    val projectionName = UUID.randomUUID().toString
    val processedProbe = createTestProbe[Processed]()
    StartParams(entityType, projectionName, nrOfProjections = 4, processedProbe)
  }

  s"An R2DBC projection with eventsBySlices source (dialect ${r2dbcSettings.dialectName})" must {

    "handle all events exactlyOnce" in {
      val startParams = newStartParams()
      test(startParams, () => startExactlyOnceProjections(startParams))
    }

    "handle all events atLeastOnce" in {
      val startParams = newStartParams()
      test(startParams, () => startAtLeastOnceProjections(startParams))
    }

    "handle all events atLeastOnceAsync" in {
      val startParams = newStartParams()
      test(startParams, () => startAtLeastOnceAsyncProjections(startParams))
    }

    "handle all events groupedWithin" in {
      val startParams = newStartParams()
      test(startParams, () => startGroupedWithinProjections(startParams))
    }

    "handle all events groupedWithinAsync" in {
      val startParams = newStartParams()
      test(startParams, () => startGroupedWithinAsyncProjections(startParams))
    }

    "handle all events atLeastOnceFlow" in {
      val startParams = newStartParams()
      test(startParams, () => startAtLeastOnceFlowProjections(startParams))
    }

    "accept unknown sequence number if previous is old" in {
      val entityType = "test-002"
      val pid1 = s"$entityType|p-08071" // slice 992
      val pid2 = s"$entityType|p-08192" // slice 992
      val pid3 = s"$entityType|p-09160" // slice 992

      val startTime = TestClock.nowMicros().instant()
      val oldTime = startTime.minus(projectionSettings.deleteAfter).minusSeconds(60)
      writeEvent(pid1, 1L, startTime, "e1-1")

      val projectionName = UUID.randomUUID().toString
      val processedProbe = createTestProbe[Processed]()
      val projection =
        startExactlyOnceProjections(StartParams(entityType, projectionName, nrOfProjections = 1, processedProbe)).head

      processedProbe.receiveMessage().envelope.event shouldBe "e1-1"

      // old event for pid2, seqN3. will not be picked up by backtracking because outside time window
      writeEvent(pid2, 3L, oldTime, "e2-3")
      // pid2, seqNr 3 is unknown when receiving 4 so will lookup timestamp of 3
      // and accept 4 because 3 was older than the deletion window (for tracked slice)
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

    "start from adjusted offset" in {
      val entityType = nextEntityType()
      val persistenceId1 = PersistenceId(entityType, "p1")
      val persistenceId2 = PersistenceId(entityType, "p2")
      val doneProbe = createTestProbe[Done]()

      val persister1 = spawn(Persister(persistenceId1), s"$entityType-p1")
      val persister2 = spawn(Persister(persistenceId2), s"$entityType-p2")

      persister1 ! Persister.Persist(mkEvent(1))
      persister1 ! Persister.Persist(mkEvent(2))
      persister1 ! Persister.PersistWithAck(mkEvent(3), doneProbe.ref)
      doneProbe.expectMessage(Done)

      val projectionName = UUID.randomUUID().toString
      val processedProbe = createTestProbe[Processed]()
      val sliceRange = EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, 1).head
      val projectionId = ProjectionId(projectionName, s"${sliceRange.min}-${sliceRange.max}")
      val projection1 = {
        val sourceProvider =
          EventSourcedProvider
            .eventsBySlices[String](
              system,
              R2dbcReadJournal.Identifier,
              entityType,
              sliceRange.min,
              sliceRange.max,
              adjustStartOffset = { (storedOffset: Option[Offset]) =>
                if (storedOffset.isDefined)
                  throw new IllegalStateException(s"Expected no stored offset, but was $storedOffset")
                Future.successful(storedOffset)
              })
        val projection = R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(projectionSettings),
            sourceProvider = sourceProvider,
            handler = () =>
              new R2dbcTestHandler(projectionId, processedProbe.ref, processedEvents(projectionId), exactlyOnce = true))
        spawn(ProjectionBehavior(projection))
      }

      processedProbe.receiveMessage().envelope.sequenceNr shouldBe 1L
      processedProbe.receiveMessage().envelope.sequenceNr shouldBe 2L
      val processed3 = processedProbe.receiveMessage()
      processed3.envelope.persistenceId shouldBe persistenceId1.id
      processed3.envelope.sequenceNr shouldBe 3L
      val offset3 = toTimestampOffset(processed3.envelope.offset)
      projection1 ! ProjectionBehavior.Stop
      doneProbe.expectTerminated(projection1)

      persister2 ! Persister.Persist(mkEvent(1))
      persister2 ! Persister.Persist(mkEvent(2))
      persister2 ! Persister.PersistWithAck(mkEvent(3), doneProbe.ref)
      doneProbe.expectMessage(Done)

      val projection2 = {
        val sourceProvider =
          EventSourcedProvider
            .eventsBySlices[String](
              system,
              R2dbcReadJournal.Identifier,
              entityType,
              sliceRange.min,
              sliceRange.max,
              adjustStartOffset = { (storedOffset: Option[Offset]) =>
                storedOffset match {
                  case None => throw new IllegalStateException(s"Expected stored offset, but was $storedOffset")
                  case Some(o) =>
                    if (toTimestampOffset(o).timestamp != offset3.timestamp)
                      throw new IllegalStateException(
                        s"Expected offset [offset3.timestamp], but was ${toTimestampOffset(o).timestamp}")
                }

                // increase offset so that events from p2 are not received
                val startOffset = Offset.timestamp(offset3.timestamp.plusSeconds(2))
                Future.successful(Some(startOffset))
              })
        val projection = R2dbcProjection
          .exactlyOnce(
            projectionId,
            Some(projectionSettings),
            sourceProvider = sourceProvider,
            handler = () =>
              new R2dbcTestHandler(projectionId, processedProbe.ref, processedEvents(projectionId), exactlyOnce = true))
        spawn(ProjectionBehavior(projection))
      }

      // this would fail if it didn't use the adjusted offset because then it would receive events from p2
      processedProbe.expectNoMessage(1.second)

      // the events from p1 and p2 would be retrieved via backtracking but that is 5 seconds behind

      projection2 ! ProjectionBehavior.Stop
    }

  }

}
