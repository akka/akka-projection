/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
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
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.proto.EntityIdOffset
import akka.projection.grpc.internal.proto.ExcludeEntityIds
import akka.projection.grpc.internal.proto.ExcludeRegexEntityIds
import akka.projection.grpc.internal.proto.ExcludeTags
import akka.projection.grpc.internal.proto.FilterCriteria
import akka.projection.grpc.internal.proto.FilterReq
import akka.projection.grpc.internal.proto.IncludeEntityIds
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.ReplayReq
import akka.projection.grpc.internal.proto.StreamIn
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

    private def testCurrentEventsByPersistenceIdQuery(allEnvelopes: Vector[EventEnvelope[Any]]) =
      new CurrentEventsByPersistenceIdTypedQuery {

        override def currentEventsByPersistenceIdTyped[Event](
            persistenceId: String,
            fromSequenceNr: Long,
            toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] = {
          val filtered = allEnvelopes
            .filter(env => env.persistenceId == persistenceId && env.sequenceNr >= fromSequenceNr)
            .sortBy(_.sequenceNr)
            .map(_.asInstanceOf[EventEnvelope[Event]])
          // simulate initial delay for more realistic testing, and concurrency check
          import scala.concurrent.duration._

          import akka.pattern.{ after => futureAfter }
          if (eventsByPersistenceIdConcurrency.incrementAndGet() > FilterStage.ReplayParallelism)
            throw new IllegalStateException("Unexpected, too many concurrent calls to currentEventsByPersistenceId")
          Source
            .futureSource(futureAfter(10.millis) {
              eventsByPersistenceIdConcurrency.decrementAndGet()
              Future.successful(Source(filtered))
            })
            .mapMaterializedValue(_ => NotUsed)
        }
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
            producerFilter = initProducerFilter))
        .join(Flow.fromSinkAndSource(Sink.ignore, envSource))
    private val streamIn: Source[StreamIn, TestPublisher.Probe[StreamIn]] = TestSource()

    val (inPublisher, outProbe) = streamIn.via(envFlow).toMat(TestSink())(Keep.both).run()
    val envPublisher = envPublisherPromise.future.futureValue
  }

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
          createEnvelope(PersistenceId(entityType, "d"), 2, "d2"))

      inPublisher.sendNext(
        StreamIn(
          StreamIn.Message.Replay(ReplayReq(List(
            PersistenceIdSeqNr(PersistenceId(entityType, "b").id, 1L),
            PersistenceIdSeqNr(PersistenceId(entityType, "c").id, 1L))))))

      outProbe.request(10)
      // no guarantee of order between b and c
      outProbe.expectNextN(2).map(_.event).toSet shouldBe Set("b1", "c1")
      outProbe.expectNoMessage()

      inPublisher.sendNext(
        StreamIn(StreamIn.Message.Replay(ReplayReq(List(PersistenceIdSeqNr(PersistenceId(entityType, "d").id, 1L))))))
      // it will not emit replayed event until there is some progress from the ordinary envSource, probably ok
      outProbe.expectNoMessage()
      envPublisher.sendNext(createEnvelope(PersistenceId(entityType, "e"), 1, "e1"))
      outProbe.expectNext().event shouldBe "e1"
      outProbe.expectNext().event shouldBe "d1"
      outProbe.expectNext().event shouldBe "d2"
    }

    "handle many replay requests" in new Setup {
      lazy val entityIds = (1 to 20).map(n => s"entity-$n")
      override lazy val allEnvelopes = envelopes ++
        entityIds.map(id => createEnvelope(PersistenceId(entityType, id), 1, id))

      inPublisher.sendNext(
        StreamIn(StreamIn.Message.Replay(
          ReplayReq(entityIds.take(7).map(id => PersistenceIdSeqNr(PersistenceId(entityType, id).id, 1L))))))
      inPublisher.sendNext(
        StreamIn(StreamIn.Message.Replay(
          ReplayReq(entityIds.slice(7, 10).map(id => PersistenceIdSeqNr(PersistenceId(entityType, id).id, 1L))))))
      inPublisher.sendNext(
        StreamIn(StreamIn.Message.Replay(
          ReplayReq(entityIds.drop(10).map(id => PersistenceIdSeqNr(PersistenceId(entityType, id).id, 1L))))))

      outProbe.request(100)
      // no guarantee of order between different entityIds
      outProbe.expectNextN(entityIds.size).map(_.persistenceId).toSet shouldBe entityIds
        .map(PersistenceId(entityType, _).id)
        .toSet
      outProbe.expectNoMessage()

      envPublisher.sendComplete()
    }

  }

}
