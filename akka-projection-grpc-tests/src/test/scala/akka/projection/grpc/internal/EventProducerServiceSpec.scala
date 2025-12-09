/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise

import akka.Done
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
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdStartingFromSnapshotQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.persistence.typed.internal.VersionVector
import akka.projection.grpc.internal.proto.CurrentEventsByPersistenceIdRequest
import akka.projection.grpc.internal.proto.EventTimestampRequest
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.LoadEventRequest
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.ReplayPersistenceId
import akka.projection.grpc.internal.proto.ReplayReq
import akka.projection.grpc.internal.proto.ReplicaInfo
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.grpc.producer.scaladsl.EventProducerInterceptor
import akka.projection.grpc.replication.internal.EventOriginFilter
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

  class TestQuery()(implicit system: ActorSystem[_])
      extends ReadJournal
      with EventsBySliceQuery
      with EventsBySliceStartingFromSnapshotsQuery
      with CurrentEventsByPersistenceIdTypedQuery
      with CurrentEventsByPersistenceIdStartingFromSnapshotQuery {
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

    override def eventsBySlicesStartingFromSnapshots[Snapshot, Event](
        entityType: String,
        minSlice: Int,
        maxSlice: Int,
        offset: Offset,
        transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] =
      eventsBySlices[Event](entityType, minSlice, maxSlice, offset)
        .map { env =>
          if (env.event.toString.contains("snap")) {
            EventEnvelope(
              env.offset,
              env.persistenceId,
              env.sequenceNr,
              transformSnapshot(env.event.asInstanceOf[Snapshot]),
              env.timestamp,
              env.entityType,
              env.slice,
              env.filtered,
              env.source,
              env.tags)
          } else
            env
        }

    override def sliceForPersistenceId(persistenceId: String): Int =
      persistenceExt.sliceForPersistenceId(persistenceId)

    override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
      persistenceExt.sliceRanges(numberOfRanges)

    override def currentEventsByPersistenceIdTyped[Event](
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] = {
      Source((fromSequenceNr to 3).map { n =>
        createEnvelope(PersistenceId.ofUniqueId(persistenceId), n, s"e-$n")
      }).asInstanceOf[Source[EventEnvelope[Event], NotUsed]]
    }

    override def currentEventsByPersistenceIdStartingFromSnapshot[Snapshot, Event](
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long,
        transformSnapshot: Snapshot => Event): Source[EventEnvelope[Event], NotUsed] = {
      Source((math.max(fromSequenceNr, 2) to 3).map { n =>
        if (n == 2) {
          val transform = transformSnapshot.asInstanceOf[String => String]
          createEnvelope(PersistenceId.ofUniqueId(persistenceId), n, transform(s"e-$n-snap"))
        } else
          createEnvelope(PersistenceId.ofUniqueId(persistenceId), n, s"e-$n")
      }).asInstanceOf[Source[EventEnvelope[Event], NotUsed]]
    }
  }

  private def createEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
      tags: Set[String] = Set.empty,
      source: String = ""): EventEnvelope[String] = {
    val now = Instant.now()
    val slice = math.abs(pid.hashCode % 1024)
    val env = EventEnvelope(
      TimestampOffset(Instant.now, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      now.toEpochMilli,
      pid.entityTypeHint,
      slice,
      filtered = false,
      source = source,
      tags)

    if (source == "BT")
      env.withEventOption(None)
    else
      env
  }

  private def createEnvelope(pid: PersistenceId, seqNr: Long, evt: String, origin: String): EventEnvelope[String] =
    createEnvelope(pid, seqNr, evt)
      .withMetadata(ReplicatedEventMetadata(ReplicaId(origin), seqNr, VersionVector.empty, concurrent = false))

  private def createBacktrackingEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
      origin: String): EventEnvelope[String] =
    createEnvelope(pid, seqNr, evt, source = "BT")
      .withMetadata(ReplicatedEventMetadata(ReplicaId(origin), seqNr, VersionVector.empty, concurrent = false))

}

class EventProducerServiceSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.load())
    with AnyWordSpecLike
    with Matchers
    with TestData
    with LogCapturing {
  import EventProducerServiceSpec._

  private implicit val sys: akka.actor.ActorSystem = system.classicSystem

  private val query = new TestQuery
  private val transformation =
    Transformation.empty.registerAsyncMapper((event: String) => {
      if (event.contains("*"))
        Future.successful(None)
      else
        Future.successful(Some(event.toUpperCase))
    })
  private val settings = EventProducerSettings(system)

  // one per test
  val entityType1 = nextEntityType()
  val streamId1 = "stream_id_" + entityType1
  val entityType2 = nextEntityType()
  val streamId2 = "stream_id_" + entityType2
  val entityType3 = nextEntityType()
  val streamId3 = "stream_id_" + entityType3
  val entityType4 = nextEntityType() + "_snap"
  val streamId4 = "stream_id_" + entityType4
  val entityType5 = nextEntityType() + "_snap"
  val streamId5 = "stream_id_" + entityType5
  val entityType6 = nextEntityType()
  val streamId6 = "stream_id_" + entityType6
  val entityType7 = nextEntityType()
  val streamId7 = "stream_id_" + entityType7

  private val eventsBySlicesQueries =
    Map(streamId1 -> query, streamId2 -> query, streamId3 -> query, streamId6 -> query, streamId7 -> query)
  private val eventsBySlicesStartingFromSnapshotsQueries =
    Map(streamId4 -> query, streamId5 -> query)
  private val currentEventsByPersistenceIdQueries =
    Map(streamId1 -> query, streamId2 -> query, streamId3 -> query)
  private val currentEventsByPersistenceIdStartingFromSnapshotQueries =
    Map(streamId4 -> query, streamId5 -> query)

  private val eventProducerSources = Set(
    EventProducerSource(entityType1, streamId1, transformation, settings),
    EventProducerSource(entityType2, streamId2, transformation, settings),
    EventProducerSource(entityType3, streamId3, transformation, settings),
    EventProducerSource(entityType4, streamId4, transformation, settings)
      .withStartingFromSnapshots[String, String] { evt =>
        if (evt.contains("-snap"))
          evt.replace("-snap", "")
        else
          evt
      },
    EventProducerSource(entityType5, streamId5, transformation, settings)
      .withStartingFromSnapshots[String, String] { evt =>
        if (evt.contains("-snap"))
          evt.replace("-snap", "")
        else
          evt
      },
    EventProducerSource(entityType6, streamId6, transformation, settings)
      .withReplicatedEventOriginFilter(new EventOriginFilter(ReplicaId("replica1"))),
    EventProducerSource(entityType7, streamId7, transformation, settings)
      .withReplicatedEventMetadataTransformation(
        env =>
          if (env.metadata[ReplicatedEventMetadata].isDefined) None
          else {
            // migrated from non-replicated, fill in metadata
            Some(
              ReplicatedEventMetadata(
                originReplica = ReplicaId.empty,
                originSequenceNr = env.sequenceNr,
                version = VersionVector(ReplicaId.empty.id, env.sequenceNr),
                concurrent = false))
          }))

  private val eventProducerService =
    new EventProducerServiceImpl(
      system,
      eventsBySlicesQueries,
      eventsBySlicesStartingFromSnapshotsQueries,
      currentEventsByPersistenceIdQueries,
      currentEventsByPersistenceIdStartingFromSnapshotQueries,
      eventProducerSources,
      None)

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

  "EventProducerService" must {
    "emit events" in {
      val initReq = InitReq(streamId1, 0, 1023, offset = Seq.empty)
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        query.testPublisher(entityType1).futureValue

      val env1 = createEnvelope(nextPid(entityType1), 1L, "e-1")
      testPublisher.sendNext(env1)
      val env2 = createEnvelope(nextPid(entityType1), 2L, "e-2", tags = Set("tag-a", "tag-b"))
      testPublisher.sendNext(env2)
      val env1b = createEnvelope(nextPid(entityType1), 1L, "e-1", source = "BT")
      testPublisher.sendNext(env1b)

      val out1 = probe.expectNext()
      out1.getEvent.persistenceId shouldBe env1.persistenceId
      out1.getEvent.seqNr shouldBe env1.sequenceNr

      val out2 = probe.expectNext()
      out2.getEvent.persistenceId shouldBe env2.persistenceId
      out2.getEvent.seqNr shouldBe env2.sequenceNr
      out2.getEvent.tags.toSet shouldBe Set("tag-a", "tag-b")

      val out3 = probe.expectNext()
      out3.getEvent.persistenceId shouldBe env1b.persistenceId
      out3.getEvent.seqNr shouldBe env1b.sequenceNr
      out3.getEvent.source shouldBe "BT"
      out3.getEvent.payload shouldBe None
    }

    "emit filtered events" in {
      val initReq = InitReq(streamId2, 0, 1023, offset = Seq.empty)
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        query.testPublisher(entityType2).futureValue

      val pid = nextPid(entityType2)
      val env1 = createEnvelope(pid, 1L, "e-1")
      testPublisher.sendNext(env1)
      // will be filtered by the transformation
      val env2 = createEnvelope(pid, 2L, "e-2*")
      testPublisher.sendNext(env2)
      val env3 = createEnvelope(pid, 3L, "e-3")
      testPublisher.sendNext(env3)
      val env4 = createEnvelope(pid, 4L, "e-4", source = "PS")
      testPublisher.sendNext(env4)
      // will be filtered by the transformation
      val env5 = createEnvelope(pid, 5L, "e-5*", source = "PS")
      testPublisher.sendNext(env5)

      val out1 = probe.expectNext()
      out1.message.isEvent shouldBe true
      out1.getEvent.persistenceId shouldBe env1.persistenceId
      out1.getEvent.seqNr shouldBe env1.sequenceNr
      out1.getEvent.source shouldBe ""

      val out2 = probe.expectNext()
      out2.message.isFilteredEvent shouldBe true
      out2.getFilteredEvent.persistenceId shouldBe env2.persistenceId
      out2.getFilteredEvent.seqNr shouldBe env2.sequenceNr
      out2.getFilteredEvent.source shouldBe ""

      val out3 = probe.expectNext()
      out3.message.isEvent shouldBe true
      out3.getEvent.persistenceId shouldBe env3.persistenceId
      out3.getEvent.seqNr shouldBe env3.sequenceNr
      out3.getEvent.source shouldBe ""

      val out4 = probe.expectNext()
      out4.message.isEvent shouldBe true
      out4.getEvent.persistenceId shouldBe env4.persistenceId
      out4.getEvent.seqNr shouldBe env4.sequenceNr
      out4.getEvent.source shouldBe "PS"

      val out5 = probe.expectNext()
      out5.message.isFilteredEvent shouldBe true
      out5.getFilteredEvent.persistenceId shouldBe env5.persistenceId
      out5.getFilteredEvent.seqNr shouldBe env5.sequenceNr
      out5.getFilteredEvent.source shouldBe "PS"
    }

    "intercept and fail requests" in {
      val interceptedProducerService =
        new EventProducerServiceImpl(
          system,
          eventsBySlicesQueries,
          eventsBySlicesStartingFromSnapshotsQueries,
          currentEventsByPersistenceIdQueries,
          currentEventsByPersistenceIdStartingFromSnapshotQueries,
          eventProducerSources,
          Some(new EventProducerInterceptor {
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
            .single(StreamIn(StreamIn.Message.Init(InitReq("nono-direct", 0, 1023, offset = Seq.empty)))),
          MetadataBuilder.empty)
        .runWith(Sink.head)
        .failed
        .futureValue
      assertGrpcStatusDenied(directStreamIdFail, "nono-direct")

      val directMetaFail = interceptedProducerService
        .eventsBySlices(
          Source
            .single(StreamIn(StreamIn.Message.Init(InitReq("ok", 0, 1023, offset = Seq.empty)))),
          new MetadataBuilder().addText("nono-meta-direct", "value").build())
        .runWith(Sink.head)
        .failed
        .futureValue
      assertGrpcStatusDenied(directMetaFail, "nono-meta-direct")

      val asyncStreamFail = interceptedProducerService
        .eventsBySlices(
          Source
            .single(StreamIn(StreamIn.Message.Init(InitReq("nono-async", 0, 1023, offset = Seq.empty)))),
          MetadataBuilder.empty)
        .runWith(Sink.head)
        .failed
        .futureValue
      assertGrpcStatusDenied(asyncStreamFail, "nono-async")

      val passThrough = interceptedProducerService
        .eventsBySlices(
          Source
            .single(StreamIn(StreamIn.Message.Init(InitReq("ok", 0, 1023, offset = Seq.empty)))),
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

      val currentEventsFailure =
        interceptedProducerService
          .currentEventsByPersistenceId(CurrentEventsByPersistenceIdRequest("nono-async"), MetadataBuilder.empty)
          .runWith(Sink.head)
          .failed
          .futureValue
      assertGrpcStatusDenied(currentEventsFailure, "nono-async")
    }

    "replay events" in {
      val persistenceId = nextPid(entityType3)
      val initReq = InitReq(streamId3, 0, 1023, offset = Seq.empty)
      val replayReq = ReplayReq(replayPersistenceIds =
        List(ReplayPersistenceId(Some(PersistenceIdSeqNr(persistenceId.id, 2L)), filterAfterSeqNr = Long.MaxValue)))
      val streamIn =
        Source(List(StreamIn(StreamIn.Message.Init(initReq)), StreamIn(StreamIn.Message.Replay(replayReq))))
          .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)
      probe.request(100)
      val testPublisher =
        query.testPublisher(entityType3).futureValue

      // it will not emit replayed event until there is some progress from the ordinary envSource, probably ok
      val env1 = createEnvelope(nextPid(entityType3), 1L, "e-1")
      testPublisher.sendNext(env1)
      val out1 = probe.expectNext()
      out1.getEvent.persistenceId shouldBe env1.persistenceId
      out1.getEvent.seqNr shouldBe env1.sequenceNr

      val out2 = probe.expectNext()
      out2.getEvent.persistenceId shouldBe persistenceId.id
      out2.getEvent.seqNr shouldBe 2L

      val out3 = probe.expectNext()
      out3.getEvent.persistenceId shouldBe persistenceId.id
      out3.getEvent.seqNr shouldBe 3L
    }

    "emit events StartingFromSnapshots" in {
      val initReq = InitReq(streamId4, 0, 1023, offset = Seq.empty)
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        query.testPublisher(entityType4).futureValue

      val pid = nextPid(entityType4)
      val env2 = createEnvelope(pid, 2L, "e-2-snap")
      testPublisher.sendNext(env2)
      val env3 = createEnvelope(pid, 3L, "e-3")
      testPublisher.sendNext(env3)

      val protoAnySerialization = new ProtoAnySerialization(system)

      val out1 = probe.expectNext()
      out1.getEvent.persistenceId shouldBe env2.persistenceId
      out1.getEvent.seqNr shouldBe env2.sequenceNr
      // transformSnapshot removes the "-snap"
      protoAnySerialization.deserialize(out1.getEvent.payload.get) shouldBe "E-2"

      val out2 = probe.expectNext()
      out2.getEvent.persistenceId shouldBe env3.persistenceId
      out2.getEvent.seqNr shouldBe env3.sequenceNr
      protoAnySerialization.deserialize(out2.getEvent.payload.get) shouldBe "E-3"
    }

    "replay events StartingFromSnapshots" in {
      val persistenceId = nextPid(entityType5)
      val initReq = InitReq(streamId5, 0, 1023, offset = Seq.empty)
      val replayReq = ReplayReq(replayPersistenceIds =
        List(ReplayPersistenceId(Some(PersistenceIdSeqNr(persistenceId.id, 1L)), filterAfterSeqNr = Long.MaxValue)))
      val streamIn =
        Source(List(StreamIn(StreamIn.Message.Init(initReq)), StreamIn(StreamIn.Message.Replay(replayReq))))
          .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)
      probe.request(100)
      val testPublisher =
        query.testPublisher(entityType5).futureValue

      // it will not emit replayed event until there is some progress from the ordinary envSource, probably ok
      val env1 = createEnvelope(nextPid(entityType5), 1L, "e-1")
      testPublisher.sendNext(env1)
      val out1 = probe.expectNext()
      out1.getEvent.persistenceId shouldBe env1.persistenceId
      out1.getEvent.seqNr shouldBe env1.sequenceNr

      val protoAnySerialization = new ProtoAnySerialization(system)

      val out2 = probe.expectNext()
      out2.getEvent.persistenceId shouldBe persistenceId.id
      out2.getEvent.seqNr shouldBe 2L
      // transformSnapshot removes the "-snap"
      protoAnySerialization.deserialize(out2.getEvent.payload.get) shouldBe "E-2"

      val out3 = probe.expectNext()
      out3.getEvent.persistenceId shouldBe persistenceId.id
      out3.getEvent.seqNr shouldBe 3L
      // transformSnapshot removes the "-snap"
      protoAnySerialization.deserialize(out3.getEvent.payload.get) shouldBe "E-3"
    }

    "filter based on event origin" in {
      val replicaInfo = ReplicaInfo("replica2", List("replica1", "replica3"))
      val initReq = InitReq(streamId6, 0, 1023, offset = Seq.empty, replicaInfo = Some(replicaInfo))
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        query.testPublisher(entityType6).futureValue

      val pid = nextPid(entityType6)
      // emitted because replica1 is the producer self replica
      val env1 = createEnvelope(pid, 1L, "e-1", "replica1")
      testPublisher.sendNext(env1)
      // will be filtered because replica2 is the consumer self replica
      val env2 = createEnvelope(pid, 2L, "e-2", "replica2")
      testPublisher.sendNext(env2)
      // will be filtered because replica3 is included in other replicas
      val env3 = createEnvelope(pid, 3L, "e-3", "replica3")
      testPublisher.sendNext(env3)
      // emitted because replica4 is not included in other replicas
      val env4 = createEnvelope(pid, 4L, "e-4", "replica4")
      testPublisher.sendNext(env4)
      // emitted because backtracking
      val env5 = createBacktrackingEnvelope(pid, 4L, "e-4", "replica2")
      testPublisher.sendNext(env5)

      val out1 = probe.expectNext()
      out1.message.isEvent shouldBe true
      out1.getEvent.seqNr shouldBe env1.sequenceNr

      val out2 = probe.expectNext()
      out2.message.isFilteredEvent shouldBe true
      out2.getFilteredEvent.seqNr shouldBe env2.sequenceNr

      val out3 = probe.expectNext()
      out3.message.isFilteredEvent shouldBe true
      out3.getFilteredEvent.seqNr shouldBe env3.sequenceNr

      val out4 = probe.expectNext()
      out4.message.isEvent shouldBe true
      out4.getEvent.seqNr shouldBe env4.sequenceNr

      val out5 = probe.expectNext()
      out5.message.isEvent shouldBe true
      out5.getEvent.seqNr shouldBe env5.sequenceNr

    }

    "fill in missing metadata" in {
      val initReq = InitReq(streamId7, 0, 1023, offset = Nil)
      val streamIn = Source
        .single(StreamIn(StreamIn.Message.Init(initReq)))
        .concat(Source.maybe)

      val probe = runEventsBySlices(streamIn)

      probe.request(100)
      val testPublisher =
        query.testPublisher(entityType7).futureValue

      val env1 = createEnvelope(nextPid(entityType7), 1L, "e-1")
      testPublisher.sendNext(env1)
      val env2 = createEnvelope(nextPid(entityType7), 2L, "e-2")
      testPublisher.sendNext(env2)

      val protoAnySerialization = new ProtoAnySerialization(system)

      val out1 = probe.expectNext()
      protoAnySerialization.deserialize(out1.getEvent.metadata.get) shouldBe ReplicatedEventMetadata(
        originReplica = ReplicaId.empty,
        originSequenceNr = env1.sequenceNr,
        version = VersionVector(ReplicaId.empty.id, env1.sequenceNr),
        concurrent = false)

      val out2 = probe.expectNext()
      protoAnySerialization.deserialize(out2.getEvent.metadata.get) shouldBe ReplicatedEventMetadata(
        originReplica = ReplicaId.empty,
        originSequenceNr = env2.sequenceNr,
        version = VersionVector(ReplicaId.empty.id, env2.sequenceNr),
        concurrent = false)
    }

    "request events for persistenceId" in {
      val persistenceId = nextPid(entityType3)

      val events =
        eventProducerService
          .currentEventsByPersistenceId(
            CurrentEventsByPersistenceIdRequest(streamId3, persistenceId.id, 1, 3),
            MetadataBuilder.empty)
          .runWith(Sink.seq)
          .futureValue
      events.size shouldBe 3
      events.map(_.getEvent.persistenceId).toSet shouldBe Set(persistenceId.id)
    }

  }

}
