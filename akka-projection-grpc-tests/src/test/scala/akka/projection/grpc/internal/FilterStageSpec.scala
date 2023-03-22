/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.time.Instant

import scala.concurrent.Promise

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.Persistence
import akka.persistence.query
import akka.persistence.query.TimestampOffset
import akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.proto.EntityIdOffset
import akka.projection.grpc.internal.proto.ExcludeEntityIds
import akka.projection.grpc.internal.proto.ExcludeRegexEntityIds
import akka.projection.grpc.internal.proto.FilterCriteria
import akka.projection.grpc.internal.proto.FilterReq
import akka.projection.grpc.internal.proto.IncludeEntityIds
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

  private def createEnvelope(pid: PersistenceId, seqNr: Long, evt: String): EventEnvelope[Any] = {
    val now = Instant.now()
    EventEnvelope(
      TimestampOffset(Instant.now, Map(pid.id -> seqNr)),
      pid.id,
      seqNr,
      evt,
      now.toEpochMilli,
      pid.entityTypeHint,
      persistence.sliceForPersistenceId(pid.id))
  }

  private val envelopes = Vector(
    createEnvelope(PersistenceId(entityType, "a"), 1, "a1"),
    createEnvelope(PersistenceId(entityType, "b"), 1, "b1"),
    createEnvelope(PersistenceId(entityType, "c"), 1, "c1"))

  private val notUsedCurrentEventsByPersistenceIdQuery = new CurrentEventsByPersistenceIdQuery {
    override def currentEventsByPersistenceId(
        persistenceId: String,
        fromSequenceNr: Long,
        toSequenceNr: Long): Source[query.EventEnvelope, NotUsed] =
      throw new IllegalStateException("Unexpected use of currentEventsByPersistenceId")
  }

  private def testCurrentEventsByPersistenceIdQuery(allEnvelopes: Vector[EventEnvelope[Any]]) =
    new CurrentEventsByPersistenceIdQuery {
      override def currentEventsByPersistenceId(
          persistenceId: String,
          fromSequenceNr: Long,
          toSequenceNr: Long): Source[query.EventEnvelope, NotUsed] = {
        val filtered = allEnvelopes
          .filter(env => env.persistenceId == persistenceId && env.sequenceNr >= fromSequenceNr)
          .sortBy(_.sequenceNr)
        Source(filtered.map(env =>
          query.EventEnvelope(env.offset, env.persistenceId, env.sequenceNr, env.event, env.timestamp)))
      }
    }

  "FilterStage" must {
    "emit EventEnvelope" in {

      val envSource: Source[EventEnvelope[Any], NotUsed] = Source(envelopes)
      val envFlow: Flow[StreamIn, EventEnvelope[Any], NotUsed] =
        BidiFlow
          .fromGraph(
            new FilterStage(
              streamId,
              entityType,
              0 until persistence.numberOfSlices,
              Nil,
              notUsedCurrentEventsByPersistenceIdQuery))
          .join(Flow.fromSinkAndSource(Sink.ignore, envSource))
      val streamIn: Source[StreamIn, _] = Source.maybe

      val out = streamIn.via(envFlow).runWith(Sink.seq).futureValue
      out shouldBe envelopes
    }

    "use init filter" in {
      val initFilter = List(FilterCriteria(FilterCriteria.Message.ExcludeEntityIds(ExcludeEntityIds(List("b")))))
      val envSource: Source[EventEnvelope[Any], NotUsed] = Source(envelopes)
      val envFlow: Flow[StreamIn, EventEnvelope[Any], NotUsed] =
        BidiFlow
          .fromGraph(
            new FilterStage(
              streamId,
              entityType,
              0 until persistence.numberOfSlices,
              initFilter,
              notUsedCurrentEventsByPersistenceIdQuery))
          .join(Flow.fromSinkAndSource(Sink.ignore, envSource))
      val streamIn: Source[StreamIn, _] = Source.maybe

      val out = streamIn.via(envFlow).runWith(Sink.seq).futureValue
      out shouldBe envelopes.filterNot(_.persistenceId == PersistenceId(entityType, "b").id)
    }

    "use filter request" in {
      val envPublisherPromise = Promise[TestPublisher.Probe[EventEnvelope[Any]]]()
      val envSource: Source[EventEnvelope[Any], _] =
        TestSource()
          .mapMaterializedValue(envPublisherPromise.success)
      val envFlow: Flow[StreamIn, EventEnvelope[Any], NotUsed] =
        BidiFlow
          .fromGraph(
            new FilterStage(
              streamId,
              entityType,
              0 until persistence.numberOfSlices,
              Nil,
              notUsedCurrentEventsByPersistenceIdQuery))
          .join(Flow.fromSinkAndSource(Sink.ignore, envSource))
      val streamIn: Source[StreamIn, TestPublisher.Probe[StreamIn]] = TestSource()

      val (inPublisher, outFuture) = streamIn.via(envFlow).toMat(Sink.seq)(Keep.both).run()
      val envPublisher = envPublisherPromise.future.futureValue

      val filterCriteria = List(FilterCriteria(FilterCriteria.Message.ExcludeEntityIds(ExcludeEntityIds(List("b")))))
      inPublisher.sendNext(StreamIn(StreamIn.Message.Filter(FilterReq(filterCriteria))))

      envelopes.foreach(envPublisher.sendNext)
      envPublisher.sendComplete()
      outFuture.futureValue shouldBe envelopes.filterNot(_.persistenceId == PersistenceId(entityType, "b").id)
    }

    "replay from IncludeEntityIds in FilterReq" in {
      // some more envelopes
      val allEnvelopes = envelopes ++
        Vector(
          createEnvelope(PersistenceId(entityType, "d"), 1, "d1"),
          createEnvelope(PersistenceId(entityType, "d"), 2, "d2"))

      val envPublisherPromise = Promise[TestPublisher.Probe[EventEnvelope[Any]]]()
      val envSource: Source[EventEnvelope[Any], _] =
        TestSource()
          .mapMaterializedValue(envPublisherPromise.success)
      val envFlow: Flow[StreamIn, EventEnvelope[Any], NotUsed] =
        BidiFlow
          .fromGraph(
            new FilterStage(
              streamId,
              entityType,
              0 until persistence.numberOfSlices,
              Nil,
              testCurrentEventsByPersistenceIdQuery(allEnvelopes)))
          .join(Flow.fromSinkAndSource(Sink.ignore, envSource))
      val streamIn: Source[StreamIn, TestPublisher.Probe[StreamIn]] = TestSource()

      val (inPublisher, outProbe) = streamIn.via(envFlow).log("test stream").toMat(TestSink())(Keep.both).run()
      val envPublisher = envPublisherPromise.future.futureValue

      val filterCriteria = List(
        FilterCriteria(FilterCriteria.Message.ExcludeMatchingEntityIds(ExcludeRegexEntityIds(List(".*")))),
        FilterCriteria(
          FilterCriteria.Message.IncludeEntityIds(
            IncludeEntityIds(List(EntityIdOffset("b", 1L), EntityIdOffset("c", 1L))))))
      inPublisher.sendNext(StreamIn(StreamIn.Message.Filter(FilterReq(filterCriteria))))

      val expectedEnvelopes = allEnvelopes
        .filter { env =>
          env.persistenceId == PersistenceId(entityType, "b").id || env.persistenceId == PersistenceId(entityType, "c").id
        }

      outProbe.request(10)
      // no guarantee of order between b and c
      outProbe.expectNextN(2).map(_.event).toSet shouldBe expectedEnvelopes.map(_.event).toSet
      outProbe.expectNoMessage()

      // replay done, now from ordinary envSource
      allEnvelopes.foreach(envPublisher.sendNext)
      outProbe.expectNext(expectedEnvelopes(0))
      outProbe.expectNext(expectedEnvelopes(1))
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

  }

}
