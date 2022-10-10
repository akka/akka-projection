/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.proto.EventTimestampRequest
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.LoadEventRequest
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.grpc.producer.scaladsl.EventProducerInterceptor
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.SocketUtil
import com.typesafe.config.ConfigFactory
import io.grpc.Status
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object EventProducerServiceSpec {
  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  class TestEventsBySliceQuery()(implicit system: ActorSystem[_]) extends ReadJournal with EventsBySliceQuery {
    private val persistenceExt = Persistence(system)

    private val testPublisherPromise =
      new ConcurrentHashMap[String, Promise[TestPublisher.Probe[EventEnvelope[String]]]]()

    def testPublisher(entityType: String): Future[TestPublisher.Probe[EventEnvelope[String]]] = {
      val promise =
        testPublisherPromise.computeIfAbsent(entityType, _ => Promise[TestPublisher.Probe[EventEnvelope[String]]]())
      promise.future
    }

    override def eventsBySlices[Event](
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        offset: Offset): Source[EventEnvelope[Event], NotUsed] = {

      TestSource[EventEnvelope[String]]()
        .mapMaterializedValue { probe =>
          val promise =
            testPublisherPromise.computeIfAbsent(entityType, _ => Promise[TestPublisher.Probe[EventEnvelope[String]]]())
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
    extends ScalaTestWithActorTestKit(ConfigFactory.load())
    with AnyWordSpecLike
    with Matchers
    with TestData
    with LogCapturing {
  import EventProducerServiceSpec._

  private implicit val sys = system.classicSystem

  private val eventsBySlicesQuery1 = new TestEventsBySliceQuery
  private val eventsBySlicesQuery2 = new TestEventsBySliceQuery
  private val transformation =
    Transformation.empty.registerAsyncMapper((event: String) => {
      if (event.contains("*"))
        Future.successful(None)
      else
        Future.successful(Some(event.toUpperCase))
    })
  private val settings = EventProducerSettings(system)
  val entityType1 = nextEntityType()
  val streamId1 = "stream_id_" + entityType1
  val entityType2 = nextEntityType()
  val streamId2 = "stream_id_" + entityType2
  private val eventProducerSources = Set(
    EventProducerSource(entityType1, streamId1, transformation, settings),
    EventProducerSource(entityType2, streamId2, transformation, settings))
  val queries =
    Map(streamId1 -> eventsBySlicesQuery1, streamId2 -> eventsBySlicesQuery2)
  private val eventProducerService =
    new EventProducerServiceImpl(system, queries, eventProducerSources, None)

  private def runEventsBySlices(streamIn: Source[StreamIn, NotUsed]) = {
    val probePromise = Promise[TestSubscriber.Probe[StreamOut]]()
    eventProducerService
      .eventsBySlices(streamIn, MetadataBuilder.empty)
      .toMat(TestSink[StreamOut]())(Keep.right)
      .mapMaterializedValue { probe =>
        probePromise.trySuccess(probe)
        NotUsed
      }
      .run()
    val probe = probePromise.future.futureValue
    probe
  }

  private def createEnvelope(streamId: String, pid: PersistenceId, seqNr: Long, evt: String): EventEnvelope[String] = {
    val now = Instant.now()
    EventEnvelope(
      TimestampOffset(Instant.now, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      now.toEpochMilli,
      pid.entityTypeHint,
      queries(streamId).sliceForPersistenceId(pid.id))
  }

  "EventProducerService" must {
    "emit events" in {
      val initReq = InitReq(streamId1, 0, 1023, offset = None)
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        eventsBySlicesQuery1.testPublisher(entityType1).futureValue

      val env1 = createEnvelope(streamId1, nextPid(entityType1), 1L, "e-1")
      testPublisher.sendNext(env1)
      val env2 = createEnvelope(streamId1, nextPid(entityType1), 2L, "e-2")
      testPublisher.sendNext(env2)

      val out1 = probe.expectNext()
      out1.getEvent.persistenceId shouldBe env1.persistenceId
      out1.getEvent.seqNr shouldBe env1.sequenceNr

      val out2 = probe.expectNext()
      out2.getEvent.persistenceId shouldBe env2.persistenceId
      out2.getEvent.seqNr shouldBe env2.sequenceNr
    }

    "emit filtered events" in {
      val initReq = InitReq(streamId2, 0, 1023, offset = None)
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        eventsBySlicesQuery2.testPublisher(entityType2).futureValue

      val pid = nextPid(entityType2)
      val env1 = createEnvelope(streamId2, pid, 1L, "e-1")
      testPublisher.sendNext(env1)
      // will be filtered by the transformation
      val env2 = createEnvelope(streamId2, pid, 2L, "e-2*")
      testPublisher.sendNext(env2)
      val env3 = createEnvelope(streamId2, pid, 2L, "e-2")
      testPublisher.sendNext(env3)

      val out1 = probe.expectNext()
      out1.message.isEvent shouldBe true
      out1.getEvent.persistenceId shouldBe env1.persistenceId
      out1.getEvent.seqNr shouldBe env1.sequenceNr

      val out2 = probe.expectNext()
      out2.message.isFilteredEvent shouldBe true
      out2.getFilteredEvent.persistenceId shouldBe env2.persistenceId
      out2.getFilteredEvent.seqNr shouldBe env2.sequenceNr

      val out3 = probe.expectNext()
      out3.message.isEvent shouldBe true
      out3.getEvent.persistenceId shouldBe env3.persistenceId
      out3.getEvent.seqNr shouldBe env3.sequenceNr
    }

    "intercept and fail requests" in {
      val interceptedProducerService =
        new EventProducerServiceImpl(system, queries, eventProducerSources, Some(new EventProducerInterceptor {
          def intercept(streamId: String, requestMetadata: Metadata): Future[Done] = {
            if (streamId == "nono-direct")
              throw new GrpcServiceException(Status.PERMISSION_DENIED.withDescription("nono-direct"))
            else if (requestMetadata.getText("nono-meta-direct").isDefined)
              throw new GrpcServiceException(Status.PERMISSION_DENIED.withDescription("nono-meta-direct"))
            else if (streamId == "nono-async")
              Future.failed(new GrpcServiceException(Status.PERMISSION_DENIED.withDescription("nono-async")))
            else Future.successful(Done)
          }
        }))

      def assertGrpcStatusDenied(
          fail: Throwable,
          expectedDescription: String,
          status: Status = Status.PERMISSION_DENIED) = {
        fail shouldBe a[GrpcServiceException]
        fail.asInstanceOf[GrpcServiceException].status.getCode shouldBe (status.getCode)
        fail.asInstanceOf[GrpcServiceException].status.getDescription shouldBe (expectedDescription)
      }

      val directStreamIdFail = interceptedProducerService
        .eventsBySlices(
          Source
            .single(StreamIn(StreamIn.Message.Init(InitReq("nono-direct", 0, 1023, offset = None)))),
          MetadataBuilder.empty)
        .runWith(Sink.head)
        .failed
        .futureValue
      assertGrpcStatusDenied(directStreamIdFail, "nono-direct")

      val directMetaFail = interceptedProducerService
        .eventsBySlices(
          Source
            .single(StreamIn(StreamIn.Message.Init(InitReq("ok", 0, 1023, offset = None)))),
          new MetadataBuilder().addText("nono-meta-direct", "value").build())
        .runWith(Sink.head)
        .failed
        .futureValue
      assertGrpcStatusDenied(directMetaFail, "nono-meta-direct")

      val asyncStreamFail = interceptedProducerService
        .eventsBySlices(
          Source
            .single(StreamIn(StreamIn.Message.Init(InitReq("nono-async", 0, 1023, offset = None)))),
          MetadataBuilder.empty)
        .runWith(Sink.head)
        .failed
        .futureValue
      assertGrpcStatusDenied(asyncStreamFail, "nono-async")

      val passThrough = interceptedProducerService
        .eventsBySlices(
          Source
            .single(StreamIn(StreamIn.Message.Init(InitReq("ok", 0, 1023, offset = None)))),
          MetadataBuilder.empty)
        .runWith(Sink.head)
        .failed
        .futureValue
      // no such stream id so will still fail, but not from interceptor
      assertGrpcStatusDenied(passThrough, "Stream id [ok] is not available for consumption", Status.NOT_FOUND)

      // check the other methods as well for good measure
      val loadFailure =
        interceptedProducerService.loadEvent(LoadEventRequest("nono-async"), MetadataBuilder.empty).failed.futureValue
      assertGrpcStatusDenied(loadFailure, "nono-async")

      val timestampFailure = interceptedProducerService
        .eventTimestamp(EventTimestampRequest("nono-async"), MetadataBuilder.empty)
        .failed
        .futureValue
      assertGrpcStatusDenied(timestampFailure, "nono-async")
    }
  }

}
