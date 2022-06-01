/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.TestData
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.SocketUtil
import org.scalatest.wordspec.AnyWordSpecLike

object EventProducerServiceSpec {
  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  class TestEventsBySliceQuery()(implicit system: ActorSystem[_])
      extends ReadJournal
      with EventsBySliceQuery {
    private val persistenceExt = Persistence(system)
    private implicit val classicSystem = system.classicSystem

    private val testPublisherPromise =
      new ConcurrentHashMap[
        String,
        Promise[TestPublisher.Probe[EventEnvelope[String]]]]()

    def testPublisher(entityType: String)
        : Future[TestPublisher.Probe[EventEnvelope[String]]] = {
      val promise = testPublisherPromise.computeIfAbsent(
        entityType,
        _ => Promise[TestPublisher.Probe[EventEnvelope[String]]]())
      promise.future
    }

    override def eventsBySlices[Event](
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        offset: Offset): Source[EventEnvelope[Event], NotUsed] = {

      TestSource
        .probe[EventEnvelope[String]]
        .mapMaterializedValue { probe =>
          val promise = testPublisherPromise.computeIfAbsent(
            entityType,
            _ => Promise[TestPublisher.Probe[EventEnvelope[String]]]())
          promise.trySuccess(probe)
          NotUsed
        }
        .asInstanceOf[Source[EventEnvelope[Event], NotUsed]]
    }

    override def sliceForPersistenceId(persistenceId: String): Int =
      persistenceExt.sliceForPersistenceId(persistenceId)

    override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
      persistenceExt.sliceRanges(numberOfRanges)
  }
}

class EventProducerServiceSpec
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with TestData
    with LogCapturing {
  import EventProducerServiceSpec._

  private implicit val sys = system.classicSystem

  private val eventsBySlicesQuery = new TestEventsBySliceQuery
  private val transformation =
    Transformation.empty.registerAsyncMapper((event: String) => {
      if (event.contains("*"))
        Future.successful(None)
      else
        Future.successful(Some(event.toUpperCase))
    })
  private val eventProducerService =
    new EventProducerServiceImpl(system, eventsBySlicesQuery, transformation)

  private def runEventsBySlices(streamIn: Source[StreamIn, NotUsed]) = {
    val probePromise = Promise[TestSubscriber.Probe[StreamOut]]()
    eventProducerService
      .eventsBySlices(streamIn)
      .toMat(TestSink.probe[StreamOut])(Keep.right)
      .mapMaterializedValue { probe =>
        probePromise.trySuccess(probe)
        NotUsed
      }
      .run()
    val probe = probePromise.future.futureValue
    probe
  }

  private def createEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String): EventEnvelope[String] = {
    val now = Instant.now()
    EventEnvelope(
      TimestampOffset(Instant.now, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      now.toEpochMilli,
      pid.entityTypeHint,
      eventsBySlicesQuery.sliceForPersistenceId(pid.id))
  }

  "EventProducerService" must {
    "emit events" in {
      val entityType = nextEntityType()
      val initReq = InitReq(entityType, 0, 1023, offset = None)
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        eventsBySlicesQuery.testPublisher(entityType).futureValue

      val env1 = createEnvelope(nextPid(entityType), 1L, "e-1")
      testPublisher.sendNext(env1)
      val env2 = createEnvelope(nextPid(entityType), 2L, "e-2")
      testPublisher.sendNext(env2)

      val out1 = probe.expectNext()
      out1.getEvent.persistenceId shouldBe env1.persistenceId
      out1.getEvent.seqNr shouldBe env1.sequenceNr

      val out2 = probe.expectNext()
      out2.getEvent.persistenceId shouldBe env2.persistenceId
      out2.getEvent.seqNr shouldBe env2.sequenceNr
    }

    "emit filtered events" in {
      val entityType = nextEntityType()
      val initReq = InitReq(entityType, 0, 1023, offset = None)
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        eventsBySlicesQuery.testPublisher(entityType).futureValue

      val pid = nextPid(entityType)
      val env1 = createEnvelope(pid, 1L, "e-1")
      testPublisher.sendNext(env1)
      val env2 = createEnvelope(
        pid,
        2L,
        "e-2*"
      ) // will be filtered by the transformation
      testPublisher.sendNext(env2)
      val env3 = createEnvelope(pid, 2L, "e-2")
      testPublisher.sendNext(env3)

      val out1 = probe.expectNext()
      out1.getEvent.persistenceId shouldBe env1.persistenceId
      out1.getEvent.seqNr shouldBe env1.sequenceNr

      val out2 = probe.expectNext()
      out2.getFilteredEvent.persistenceId shouldBe env2.persistenceId
      out2.getFilteredEvent.seqNr shouldBe env2.sequenceNr

      val out3 = probe.expectNext()
      out3.getEvent.persistenceId shouldBe env3.persistenceId
      out3.getEvent.seqNr shouldBe env3.sequenceNr
    }

  }

}
