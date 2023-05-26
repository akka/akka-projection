/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Promise

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.Persistence
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.testkit.internal.TestClock
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.testkit.scaladsl.TestSource
import org.scalatest.wordspec.AnyWordSpecLike

class EventsBySlicesFirehoseSpec extends ScalaTestWithActorTestKit("""
    akka.loglevel = DEBUG
    """) with AnyWordSpecLike with LogCapturing {
  private val entityType = "EntityA"

  private val persistence = Persistence(system)

  private val clock = new TestClock

  private def createEnvelope(
      pid: PersistenceId,
      seqNr: Long,
      evt: String,
      tags: Set[String] = Set.empty): EventEnvelope[Any] = {
    clock.tick(Duration.ofMillis(1))
    val now = clock.instant()
    EventEnvelope(
      TimestampOffset(now, Map(pid.id -> seqNr)),
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
    createEnvelope(PersistenceId(entityType, "c"), 1, "c1"),
    createEnvelope(PersistenceId(entityType, "d"), 1, "d1"))

  private class Setup {
    def allEnvelopes: Vector[EventEnvelope[Any]] = envelopes

    def envelopesFor(entityId: String): Vector[EventEnvelope[Any]] =
      allEnvelopes.filter(_.persistenceId == PersistenceId(entityType, entityId).id)

    def sliceRange = 0 to 1023

    class ConsumerSetup {
      private val catchupPublisherPromise = Promise[TestPublisher.Probe[EventEnvelope[Any]]]()
      val catchupSource: Source[EventEnvelope[Any], NotUsed] =
        TestSource[EventEnvelope[Any]]()
          .mapMaterializedValue { probe =>
            catchupPublisherPromise.success(probe)
            NotUsed
          }

      lazy val outProbe =
        eventsBySlicesFirehose
          .eventsBySlices[Any](entityType, sliceRange.min, sliceRange.max, NoOffset)
          .runWith(TestSink())

      lazy val catchupPublisher = {
        outProbe // materialize
        catchupPublisherPromise.future.futureValue
      }
    }

    private val consumerCount = new AtomicInteger
    private val consumers = Vector.fill(100)(new ConsumerSetup)

    def catchupPublisher(consumerIndex: Int): TestPublisher.Probe[EventEnvelope[Any]] =
      consumers(consumerIndex).catchupPublisher
    def catchupPublisher: TestPublisher.Probe[EventEnvelope[Any]] = catchupPublisher(0)

    def outProbe(consumerIndex: Int): TestSubscriber.Probe[EventEnvelope[Any]] = consumers(consumerIndex).outProbe
    def outProbe: TestSubscriber.Probe[EventEnvelope[Any]] = outProbe(0)

    private val firehosePublisherPromise = Promise[TestPublisher.Probe[EventEnvelope[Any]]]()
    private val firehoseSource: Source[EventEnvelope[Any], NotUsed] =
      TestSource[EventEnvelope[Any]]()
        .mapMaterializedValue { probe =>
          firehosePublisherPromise.success(probe)
          NotUsed
        }

    val eventsBySlicesFirehose = new EventsBySlicesFirehose(system) {
      override protected def eventsBySlices[Event](
          entityType: String,
          minSlice: Int,
          maxSlice: Int,
          offset: Offset,
          firehose: Boolean): Source[EventEnvelope[Event], NotUsed] = {
        if (firehose)
          firehoseSource.map(_.asInstanceOf[EventEnvelope[Event]])
        else {
          val i = consumerCount.getAndIncrement()
          consumers(i).catchupSource.map(_.asInstanceOf[EventEnvelope[Event]])
        }
      }
    }

//    outProbe
//    catchupPublisher
    lazy val firehosePublisher = {
      outProbe // materialize at least one
      firehosePublisherPromise.future.futureValue
    }
  }

  "EventsBySlicesFirehose" must {
    "emit from catchup" in new Setup {
      outProbe
      allEnvelopes.foreach(catchupPublisher.sendNext)
      outProbe.request(10)
      outProbe.expectNextN(envelopes.size) shouldBe envelopes
    }

    "emit from catchup and then switch over to firehose" in new Setup {
      catchupPublisher.sendNext(allEnvelopes(0))
      catchupPublisher.sendNext(allEnvelopes(1))
      outProbe.request(10)
      outProbe.expectNext(allEnvelopes(0))
      outProbe.expectNext(allEnvelopes(1))

      firehosePublisher.sendNext(allEnvelopes(2))
      outProbe.expectNoMessage()

      catchupPublisher.sendNext(allEnvelopes(2))
      outProbe.expectNext(allEnvelopes(2)) // still from catchup

      firehosePublisher.sendNext(allEnvelopes(3))
      outProbe.expectNext(allEnvelopes(3)) // from firehose

      catchupPublisher.sendNext(allEnvelopes(3))
      outProbe.expectNext(allEnvelopes(3)) // from catchup, emitting from both

      clock.tick(Duration.ofSeconds(60))
      val env5 = createEnvelope(PersistenceId(entityType, "a"), 2, "a2")
      catchupPublisher.sendNext(env5)
      outProbe.expectNext(env5)

      val env6 = createEnvelope(PersistenceId(entityType, "a"), 3, "a3")
      catchupPublisher.sendNext(env6)
      outProbe.expectNoMessage() // catchup closed
      firehosePublisher.sendNext(env6)
      outProbe.expectNext(env6)
    }

    "track consumer progress" in new Setup {
      // using two consumers
      outProbe(0).request(2)
      outProbe(1).request(2)
      firehosePublisher.sendNext(allEnvelopes(0))
      catchupPublisher(0).sendNext(allEnvelopes(0))
      catchupPublisher(1).sendNext(allEnvelopes(0))
      outProbe(0).expectNext(allEnvelopes(0))
      outProbe(1).expectNext(allEnvelopes(0))

      catchupPublisher(0).sendNext(allEnvelopes(1))
      catchupPublisher(1).sendNext(allEnvelopes(1))
      outProbe(0).expectNext(allEnvelopes(1))
      outProbe(1).expectNext(allEnvelopes(1))

      val firehose = eventsBySlicesFirehose.getFirehose(entityType, sliceRange)
      firehose.inCount.get shouldBe 1
      firehose.consumerTracking.size shouldBe 2
      import akka.util.ccompat.JavaConverters._
      firehose.consumerTracking.values.asScala.foreach { tracking =>
        tracking.count shouldBe 1
      }

      // only requesting for outProbe(0)
      outProbe(0).request(100)
      // less than BroadcastHub buffer size
      val moreEnvelopes = (1 to 20).map(n => createEnvelope(PersistenceId(entityType, "x"), n, s"x$n"))
      moreEnvelopes.foreach(firehosePublisher.sendNext)
      outProbe(0).expectNextN(moreEnvelopes.size) shouldBe moreEnvelopes
      firehose.inCount.get shouldBe 1 + moreEnvelopes.size
      firehose.consumerTracking.size shouldBe 2
      firehose.consumerTracking.values.asScala.exists(_.count == 1 + moreEnvelopes.size) shouldBe true
      // no demand from outProbe(1), but I think there is an internal buffer that makes it 2
      firehose.consumerTracking.values.asScala.exists(_.count <= 2) shouldBe true
    }
  }

}
