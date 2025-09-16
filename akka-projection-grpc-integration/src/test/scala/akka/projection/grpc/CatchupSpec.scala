/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc

import java.time.temporal.ChronoUnit
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
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.InstantFactory
import akka.persistence.typed.PersistenceId
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object CatchupSpec {

  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.loglevel = DEBUG
    akka.http.server.preview.enable-http2 = on
    akka.projection.grpc {
      producer {
        query-plugin-id = "akka.persistence.r2dbc.query"
      }
    }
    akka.projection.r2dbc.replay-on-rejected-sequence-numbers=off
//    akka.projection.r2dbc.offset-store.delete-after=3m
//    akka.projection.r2dbc.offset-store.delete-interval=5s
    akka.projection {
      restart-backoff {
        min-backoff = 20 ms
        max-backoff = 2 s
      }
    }
    akka.actor.testkit.typed.filter-leeway = 10s

    akka.persistence.r2dbc.connection-factory = $${akka.persistence.r2dbc.postgres}
    """)
    .withFallback(ConfigFactory.load("persistence.conf"))
    .resolve()

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
  private implicit val ec: ExecutionContext = system.executionContext

  private val log = LoggerFactory.getLogger(getClass)

  private val entityType = nextEntityType()
  private def streamId = entityType
  private val sliceRange = 0 to 1023
  private val projectionId = randomProjectionId()

  private val pid1 = nextPid(entityType)
  private val pid2 = nextPid(entityType)
  private val pid3 = nextPid(entityType)
  private val slice1 = slice(pid1)
  private val slice2 = slice(pid2)
  private val slice3 = slice(pid3)

  private def slice(pid: PersistenceId): Int = Persistence(system).sliceForPersistenceId(pid.id)

  private def sourceProvider =
    EventSourcedProvider.eventsBySlices[String](
      system,
      GrpcReadJournal(
        GrpcQuerySettings(streamId),
        GrpcClientSettings
          .connectToServiceAt("127.0.0.1", grpcPort)
          .withTls(false),
        protobufDescriptors = Nil),
      streamId,
      sliceRange.min,
      sliceRange.max)

  def spawnAtLeastOnceProjection(handler: Handler[EventEnvelope[String]]): ActorRef[ProjectionBehavior.Command] =
    spawn(
      ProjectionBehavior(
        R2dbcProjection
          .atLeastOnceAsync(projectionId, settings = None, sourceProvider = sourceProvider, handler = () => handler)))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val transformation =
      Transformation.empty.registerAsyncMapper((event: String) => {
        if (event.contains("*"))
          Future.successful(None)
        else
          Future.successful(Some(event.toUpperCase))
      })

    val eventProducerSource = EventProducerSource(entityType, streamId, transformation, EventProducerSettings(system))

    val eventProducerService =
      EventProducer.grpcServiceHandler(eventProducerSource)

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(eventProducerService)

    val bound =
      Http()
        .newServerAt("127.0.0.1", grpcPort)
        .bind(service)
        .map(_.addToCoordinatedShutdown(3.seconds))

    bound.futureValue
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
  }

  "A gRPC Projection" must {
    "catchup old events without using replay" in {
      // note config replay-on-rejected-sequence-numbers=off
      val t0 = InstantFactory.now()
      val t1 = t0.minus(10, ChronoUnit.DAYS)

      // corresponds to first backtracking window, and some more
      val moreThanBacktrackingWindow = r2dbcProjectionSettings.backtrackingWindow
        .plusMillis(r2dbcSettings.querySettings.backtrackingBehindCurrentTime.toMillis)
        .plusSeconds(10)
      val t2 = t1.plus(moreThanBacktrackingWindow)
      val t3 = t2.plus(moreThanBacktrackingWindow)
      val t4 = t3.plus(moreThanBacktrackingWindow)

      val processedEvents = new ConcurrentHashMap[String, java.lang.Boolean]
      val failEvents = new ConcurrentHashMap[String, Int]
      val processedProbe = createTestProbe[Processed]()
      val handler = new FailingTestHandler(projectionId, processedProbe.ref, processedEvents, failEvents)

      writeEvent(slice1, pid1.id, 1L, t1, "a1")

      // first, just process the oldest event to set a starting offset; otherwise it will consume all
      // without any replay
      val projection1 = spawnAtLeastOnceProjection(handler)
      processedProbe.receiveMessage().envelope.event shouldBe "A1"
      projection1 ! ProjectionBehavior.Stop
      createTestProbe().expectTerminated(projection1)

      log.info("End phase 1 ----------------------")

      writeEvent(slice2, pid2.id, 1, t1.plusMillis(1), "b1")
      writeEvent(slice2, pid2.id, 2, t1.plusMillis(2), "b2")
      writeEvent(slice2, pid2.id, 3, t2, "b3")

      val projection2 = spawnAtLeastOnceProjection(handler)
      processedProbe.receiveMessage().envelope.event shouldBe "B1"
      processedProbe.receiveMessage().envelope.event shouldBe "B2"
      processedProbe.receiveMessage().envelope.event shouldBe "B3"
      projection2 ! ProjectionBehavior.Stop
      createTestProbe().expectTerminated(projection2)

      log.info("End phase 2 ----------------------")

      writeEvent(slice2, pid2.id, 4, t2.plusMillis(1), "b4")
      writeEvent(slice2, pid2.id, 5, t2.plusMillis(2), "b5")
      writeEvent(slice3, pid3.id, 1, t2.plusMillis(3), "c1")
      writeEvent(slice3, pid3.id, 2, t2.plusMillis(4), "c2")
      writeEvent(slice3, pid3.id, 3, t3, "c3")
      writeEvent(slice2, pid2.id, 6, t3.plusMillis(2), "b6") // this will fail once
      failEvents.put("B6", 1)
      writeEvent(slice3, pid3.id, 4, t4, "c4")

      val projection3 = spawnAtLeastOnceProjection(handler)
      processedProbe.receiveMessage().envelope.event shouldBe "B4"
      processedProbe.receiveMessage().envelope.event shouldBe "B5"
      processedProbe.receiveMessage().envelope.event shouldBe "C1"
      processedProbe.receiveMessage().envelope.event shouldBe "C2"
      processedProbe.receiveMessage().envelope.event shouldBe "C3"
      processedProbe.receiveMessage(5.seconds).envelope.event shouldBe "B6"
      processedProbe.receiveMessage().envelope.event shouldBe "C4"
      projection3 ! ProjectionBehavior.Stop
      createTestProbe().expectTerminated(projection3)

      log.info("End phase 3 ----------------------")

      writeEvent(slice1, pid1.id, 2L, t4.plusMillis(1), "a2")
      writeEvent(slice1, pid1.id, 3L, t4.plusMillis(2), "a3")

      val projection4 = spawnAtLeastOnceProjection(handler)
      processedProbe.receiveMessage().envelope.event shouldBe "A2"
      processedProbe.receiveMessage().envelope.event shouldBe "A3"
      projection4 ! ProjectionBehavior.Stop
      createTestProbe().expectTerminated(projection4)

      log.info("End phase 4 ----------------------")

      val seed = System.currentTimeMillis() // FIXME 1758012335852L
      val rnd = new Random(seed)
      log.info("Random seed [{}]", seed)
      var t = t4.plusSeconds(20)
      var seq1 = 3L
      var seq2 = 6L
      var seq3 = 4L
      val numEvents = 50000
      (1 to numEvents).foreach { _ =>

        val failEvent = rnd.nextDouble() < 0.01

        if (rnd.nextDouble() < 0.01)
          t = t.plus(moreThanBacktrackingWindow)
        else
          t = t.plusMillis(rnd.nextInt(100))

        rnd.nextInt(3) match {
          case 0 =>
            seq1 += 1
            val event = s"a$seq1"
            writeEvent(slice1, pid1.id, seq1, t, event)
            if (failEvent)
              failEvents.put(event.toUpperCase, 1)
          case 1 =>
            seq2 += 1
            val event = s"b$seq2"
            writeEvent(slice2, pid2.id, seq2, t, event)
            if (failEvent)
              failEvents.put(event.toUpperCase, 1)
          case 2 =>
            seq3 += 1
            val event = s"c$seq3"
            writeEvent(slice3, pid3.id, seq3, t, event)
        }

      }

      val projection5 = spawnAtLeastOnceProjection(handler)
      val processed = processedProbe.receiveMessages(numEvents, 30.seconds)
      val byPid = processed.groupBy(_.envelope.persistenceId)
      byPid.foreach {
        case (_, processedByPid) =>
          // all events of a pid must be processed by the same projection instance
          processedByPid.map(_.projectionId).toSet.size shouldBe 1
          // processed events in right order
          val seqNrs = processedByPid.map(_.envelope.sequenceNr).toVector
          (seqNrs.last - seqNrs.head + 1) shouldBe processedByPid.size
          seqNrs shouldBe (seqNrs.head to seqNrs.last).toVector
      }
      projection5 ! ProjectionBehavior.Stop
      createTestProbe().expectTerminated(projection5)

    }

  }

}
