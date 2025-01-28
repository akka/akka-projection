/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.Promise

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.persistence.typed.internal.VersionVector
import akka.projection.grpc.internal.proto.EntityIdOffset
import akka.projection.grpc.internal.proto.ExcludeEntityIds
import akka.projection.grpc.internal.proto.ExcludeRegexEntityIds
import akka.projection.grpc.internal.proto.ExcludeTags
import akka.projection.grpc.internal.proto.FilterCriteria
import akka.projection.grpc.internal.proto.FilterReq
import akka.projection.grpc.internal.proto.IncludeEntityIds
import akka.projection.grpc.internal.proto.IncludeTags
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.ReplayReq
import akka.projection.grpc.internal.proto.ReplayPersistenceId
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.producer.EventProducerSettings
import akka.stream.scaladsl.BidiFlow
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.wordspec.AnyWordSpecLike

class FilterStageSpec extends ScalaTestWithActorTestKit("""
    akka.loglevel = DEBUG
    """) with AnyWordSpecLike with LogCapturing {
  private val entityType = "EntityA"
  private val streamId = "EntityAStream"

  private val persistence = Persistence(system)

  private val producerSettings = EventProducerSettings(system)

  private def createEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
      tags: Set[String] = Set.empty): EventEnvelope[Any] = {
    val now = Instant.now()
    EventEnvelope(
      TimestampOffset(Instant.now, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      now.toEpochMilli,
      pid.entityTypeHint,
      persistence.sliceForPersistenceId(pid.id),
      filtered = false,
      source = "",
      tags = tags)
  }

  private def createRESEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
      tags: Set[String] = Set.empty): EventEnvelope[Any] = {
    val replicationId = ReplicationId.fromString(pid.id)
    createEnvelope(pid, seqNr, evt, tags).withMetadata(
      ReplicatedEventMetadata(
        replicationId.replicaId,
        seqNr,
        VersionVector.empty + replicationId.replicaId.id,
        concurrent = true))
  }

  private val envelopes = Vector(
    createEnvelope(PersistenceId(entityType, "a"), 1, "a1"),
    createEnvelope(PersistenceId(entityType, "b"), 1, "b1"),
    createEnvelope(PersistenceId(entityType, "c"), 1, "c1"))

  private class Setup {
    def allEnvelopes: Vector[EventEnvelope[Any]] = envelopes

    def initFilter: Iterable[FilterCriteria] = Nil

    def initProducerFilter: EventEnvelope[Any] => Boolean = _ => true

    def envelopesFor(entityId: String): Vector[EventEnvelope[Any]] =
      allEnvelopes.filter(_.persistenceId == PersistenceId(entityType, entityId).id)

    private val eventsByPersistenceIdConcurrency = new AtomicInteger()

    private def testCurrentEventsByPersistenceIdQuery(
        allEnvelopes: Vector[EventEnvelope[Any]]): (String, Long) => Source[EventEnvelope[Any], NotUsed] = {
      (persistenceId, fromSequenceNr) =>
        val filtered = allEnvelopes
          .filter(env => env.persistenceId == persistenceId && env.sequenceNr >= fromSequenceNr)
          .sortBy(_.sequenceNr)
        // simulate initial delay for more realistic testing, and concurrency check
        import scala.concurrent.duration._

        import akka.pattern.{ after => futureAfter }
        if (eventsByPersistenceIdConcurrency.incrementAndGet() > producerSettings.replayParallelism)
          throw new IllegalStateException("Unexpected, too many concurrent calls to currentEventsByPersistenceId")
        Source
          .futureSource(futureAfter(10.millis) {
            eventsByPersistenceIdConcurrency.decrementAndGet()
            Future.successful(Source(filtered))
          })
          .mapMaterializedValue(_ => NotUsed)
    }

    private val envPublisherPromise = Promise[TestPublisher.Probe[EventEnvelope[Any]]]()
    private val envSource: Source[EventEnvelope[Any], _] =
      TestSource()
        .mapMaterializedValue(envPublisherPromise.success)
    private val envFlow: Flow[StreamIn, EventEnvelope[Any], NotUsed] =
      BidiFlow
        .fromGraph(
          new FilterStage(
            streamId,
            entityType,
            0 until persistence.numberOfSlices,
            initFilter,
            testCurrentEventsByPersistenceIdQuery(allEnvelopes),
            producerFilter = initProducerFilter,
            replicatedEventOriginFilter = _ => true,
            topicTagPrefix = producerSettings.topicTagPrefix,
            replayParallelism = producerSettings.replayParallelism))
        .join(Flow.fromSinkAndSource(Sink.ignore, envSource))
    private val streamIn: Source[StreamIn, TestPublisher.Probe[StreamIn]] = TestSource()

    val (inPublisher, outProbe) = streamIn.via(envFlow).toMat(TestSink())(Keep.both).run()
    val envPublisher = envPublisherPromise.future.futureValue
  }

  private def streamInReplayReq(pid: String, fromSeqNr: Long, filterAfterSeqNr: Long = Long.MaxValue): StreamIn =
    StreamIn(
      StreamIn.Message.Replay(
        ReplayReq(replayPersistenceIds = List(replayPersistenceId(pid, fromSeqNr, filterAfterSeqNr)))))

  private def replayPersistenceId(pid: String, fromSeqNr: Long, filterAfterSeqNr: Long = Long.MaxValue) =
    ReplayPersistenceId(Some(PersistenceIdSeqNr(pid, fromSeqNr)), filterAfterSeqNr)

  "FilterStage" must {
    "emit EventEnvelope" in new Setup {
      allEnvelopes.foreach(envPublisher.sendNext)
      outProbe.request(10)
      outProbe.expectNextN(envelopes.size) shouldBe envelopes
    }

    "use init filter" in new Setup {
      override lazy val initFilter =
        List(FilterCriteria(FilterCriteria.Message.ExcludeEntityIds(ExcludeEntityIds(List("b")))))
      allEnvelopes.foreach(envPublisher.sendNext)
      outProbe.request(10)
      val expected = envelopes.filterNot(_.persistenceId == PersistenceId(entityType, "b").id)
      outProbe.expectNextN(expected.size) shouldBe expected
    }

    "use filter request" in new Setup {
      override lazy val allEnvelopes = envelopes ++
        Vector(
          createEnvelope(PersistenceId(entityType, "d"), 1, "d1", tags = Set("t1")),
          createEnvelope(PersistenceId(entityType, "d"), 2, "d2"))

      val filterCriteria = List(
        FilterCriteria(FilterCriteria.Message.ExcludeTags(ExcludeTags(List("t1")))),
        FilterCriteria(FilterCriteria.Message.ExcludeEntityIds(ExcludeEntityIds(List("b")))))
      inPublisher.sendNext(StreamIn(StreamIn.Message.Filter(FilterReq(filterCriteria))))

      allEnvelopes.foreach(envPublisher.sendNext)
      outProbe.request(10)
      outProbe.expectNext(envelopesFor("a").head)
      // b filtered out by ExcludeEntityIds
      outProbe.expectNext(envelopesFor("c").head)
      // d1 filtered out by ExcludeTags
      outProbe.expectNext(envelopesFor("d").last)
      outProbe.expectNoMessage()
    }

    "apply producer filter before consumer filters" in new Setup {

      val envFilterProbe = createTestProbe[Offset]()
      override def initProducerFilter = { envelope =>
        envFilterProbe.ref ! envelope.offset
        envelope.tags.contains("replicate-it")
      }

      val envelopes = Vector(
        createEnvelope(PersistenceId(entityType, "a"), 1, "a1"),
        createEnvelope(PersistenceId(entityType, "b"), 1, "b1"),
        createEnvelope(PersistenceId(entityType, "c"), 1, "c1"),
        createEnvelope(PersistenceId(entityType, "a"), 1, "a2", tags = Set("replicate-it")))

      envelopes.foreach(envPublisher.sendNext)
      outProbe.request(10)
      // each goes through the envelope filter
      envFilterProbe.receiveMessages(envelopes.size)
      // only last should be let through, consumer filter would allow all,
      // (consumer will trigger replay if not first seqnr once the envelope gets there)
      outProbe.expectNext() shouldBe envelopes.last
    }

    "replay from IncludeEntityIds in FilterReq" in new Setup {
      // some more envelopes
      override lazy val allEnvelopes = envelopes ++
        Vector(
          createEnvelope(PersistenceId(entityType, "d"), 1, "d1"),
          createEnvelope(PersistenceId(entityType, "d"), 2, "d2"))

      val filterCriteria = List(
        FilterCriteria(FilterCriteria.Message.ExcludeMatchingEntityIds(ExcludeRegexEntityIds(List(".*")))),
        FilterCriteria(
          FilterCriteria.Message.IncludeEntityIds(
            IncludeEntityIds(List(EntityIdOffset("b", 1L), EntityIdOffset("c", 1L))))))
      inPublisher.sendNext(StreamIn(StreamIn.Message.Filter(FilterReq(filterCriteria))))

      outProbe.request(10)
      // no guarantee of order between b and c
      outProbe.expectNextN(2).map(_.event).toSet shouldBe Set("b1", "c1")
      outProbe.expectNoMessage()

      // replay done, now from ordinary envSource
      allEnvelopes.foreach(envPublisher.sendNext)
      outProbe.expectNext(envelopesFor("b").head)
      outProbe.expectNext(envelopesFor("c").head)
      outProbe.expectNoMessage()

      val filterCriteria2 =
        List(FilterCriteria(FilterCriteria.Message.IncludeEntityIds(IncludeEntityIds(List(EntityIdOffset("d", 1L))))))
      inPublisher.sendNext(StreamIn(StreamIn.Message.Filter(FilterReq(filterCriteria2))))
      // it will not emit replayed event until there is some progress from the ordinary envSource, probably ok
      outProbe.expectNoMessage()
      envPublisher.sendNext(createEnvelope(PersistenceId(entityType, "e"), 1, "e1"))
      outProbe.expectNext().event shouldBe "d1"
      outProbe.expectNext().event shouldBe "d2"
    }

    "replay from ReplayReq" in new Setup {
      // some more envelopes
      override lazy val allEnvelopes = envelopes ++
        Vector(
          createEnvelope(PersistenceId(entityType, "d"), 1, "d1"),
          createEnvelope(PersistenceId(entityType, "d"), 2, "d2", tags = Set("WIP")))

      // filter should not exclude events from replay, e.g. d1 without the WIP tag
      val filterCriteria = List(
        FilterCriteria(FilterCriteria.Message.ExcludeMatchingEntityIds(ExcludeRegexEntityIds(List(".*")))),
        FilterCriteria(FilterCriteria.Message.IncludeTags(IncludeTags(List("WIP")))))
      inPublisher.sendNext(StreamIn(StreamIn.Message.Filter(FilterReq(filterCriteria))))

      inPublisher.sendNext(
        StreamIn(
          StreamIn.Message.Replay(ReplayReq(replayPersistenceIds = List(
            replayPersistenceId(PersistenceId(entityType, "b").id, 1L),
            replayPersistenceId(PersistenceId(entityType, "c").id, 1L))))))

      outProbe.request(10)
      // no guarantee of order between b and c
      outProbe.expectNextN(2).map(_.event).toSet shouldBe Set("b1", "c1")
      outProbe.expectNoMessage()

      inPublisher.sendNext(streamInReplayReq(PersistenceId(entityType, "d").id, 1L))
      // it will not emit replayed event until there is some progress from the ordinary envSource, probably ok
      outProbe.expectNoMessage()
      envPublisher.sendNext(createEnvelope(PersistenceId(entityType, "e"), 1, "e1", tags = Set("WIP")))
      outProbe.expectNext().event shouldBe "e1"
      outProbe.expectNext().event shouldBe "d1"
      outProbe.expectNext().event shouldBe "d2"
    }

    "replay from ReplayReq with RES ReplicaId" in new Setup {
      // some more envelopes
      override lazy val allEnvelopes = Vector(
        createRESEnvelope(ReplicationId(entityType, "a", ReplicaId("A")).persistenceId, 1, "a1"),
        createRESEnvelope(ReplicationId(entityType, "a", ReplicaId("B")).persistenceId, 1, "b1"),
        createRESEnvelope(ReplicationId(entityType, "a", ReplicaId("A")).persistenceId, 2, "a2"))

      envPublisher.sendNext(allEnvelopes.last)
      outProbe.request(10)
      outProbe.expectNext().event shouldBe "a2"

      inPublisher.sendNext(streamInReplayReq(ReplicationId(entityType, "a", ReplicaId("A")).persistenceId.id, 1L))
      // it will not emit replayed event until there is some progress from the ordinary envSource, probably ok
      envPublisher.sendNext(createRESEnvelope(PersistenceId(entityType, "e"), 1, "e1"))
      outProbe.expectNext().event shouldBe "e1"
      outProbe.expectNext().event shouldBe "a1"
      outProbe.expectNext().event shouldBe "a2"

      // but ignored if it's a request for another replicaId
      inPublisher.sendNext(streamInReplayReq(ReplicationId(entityType, "a", ReplicaId("B")).persistenceId.id, 1L))
      outProbe.expectNoMessage()
    }

    "handle many replay requests" in new Setup {
      lazy val entityIds = (1 to 20).map(n => s"entity-$n")
      override lazy val allEnvelopes = envelopes ++
        entityIds.map(id => createEnvelope(PersistenceId(entityType, id), 1, id))

      inPublisher.sendNext(
        StreamIn(StreamIn.Message.Replay(ReplayReq(replayPersistenceIds =
          entityIds.take(7).map(id => replayPersistenceId(PersistenceId(entityType, id).id, 1L))))))
      inPublisher.sendNext(
        StreamIn(StreamIn.Message.Replay(ReplayReq(replayPersistenceIds =
          entityIds.slice(7, 10).map(id => replayPersistenceId(PersistenceId(entityType, id).id, 1L))))))
      inPublisher.sendNext(
        StreamIn(StreamIn.Message.Replay(ReplayReq(replayPersistenceIds =
          entityIds.drop(10).map(id => replayPersistenceId(PersistenceId(entityType, id).id, 1L))))))

      outProbe.request(100)
      // no guarantee of order between different entityIds
      outProbe.expectNextN(entityIds.size).map(_.persistenceId).toSet shouldBe entityIds
        .map(PersistenceId(entityType, _).id)
        .toSet
      outProbe.expectNoMessage()

      envPublisher.sendComplete()
    }

    "replay from ReplayReq and filter after seqNr" in new Setup {
      override lazy val allEnvelopes =
        Vector(
          createEnvelope(PersistenceId(entityType, "d"), 1, "d1"),
          createEnvelope(PersistenceId(entityType, "d"), 2, "d2"),
          createEnvelope(PersistenceId(entityType, "d"), 3, "d3", tags = Set("WIP")),
          createEnvelope(PersistenceId(entityType, "d"), 4, "d4"),
          createEnvelope(PersistenceId(entityType, "d"), 5, "d5"))

      // filter should not exclude events from replay, e.g. d1 without the WIP tag, but
      // filters are applied for events with seqNr >= filterAfterSeqNr
      val filterCriteria = List(
        FilterCriteria(FilterCriteria.Message.ExcludeMatchingEntityIds(ExcludeRegexEntityIds(List(".*")))),
        FilterCriteria(FilterCriteria.Message.IncludeTags(IncludeTags(List("WIP")))))
      inPublisher.sendNext(StreamIn(StreamIn.Message.Filter(FilterReq(filterCriteria))))

      outProbe.request(10)

      inPublisher.sendNext(streamInReplayReq(PersistenceId(entityType, "d").id, 1L, filterAfterSeqNr = 4))
      // it will not emit replayed event until there is some progress from the ordinary envSource, probably ok
      outProbe.expectNoMessage()
      envPublisher.sendNext(createEnvelope(PersistenceId(entityType, "e"), 1, "e1"))
      outProbe.expectNext().event shouldBe "d1"
      outProbe.expectNext().event shouldBe "d2"
      outProbe.expectNext().event shouldBe "d3"
      outProbe.expectNoMessage() // d4 and d5 filtered out
    }

  }

}
