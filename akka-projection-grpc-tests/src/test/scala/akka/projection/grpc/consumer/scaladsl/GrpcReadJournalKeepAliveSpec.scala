/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.scaladsl

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.Metadata
import akka.http.scaladsl.Http
import akka.persistence.query.NoOffset
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.internal.KeepAliveTimeoutException
import akka.projection.grpc.internal.proto.CurrentEventsByPersistenceIdRequest
import akka.projection.grpc.internal.proto.EventProducerServicePowerApi
import akka.projection.grpc.internal.proto.EventProducerServicePowerApiHandler
import akka.projection.grpc.internal.proto.EventTimestampRequest
import akka.projection.grpc.internal.proto.EventTimestampResponse
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.KeepAlive
import akka.projection.grpc.internal.proto.LatestEventTimestampRequest
import akka.projection.grpc.internal.proto.LatestEventTimestampResponse
import akka.projection.grpc.internal.proto.LoadEventRequest
import akka.projection.grpc.internal.proto.LoadEventResponse
import akka.projection.grpc.internal.proto.Offset
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.SocketUtil
import com.google.protobuf.timestamp.Timestamp
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object GrpcReadJournalKeepAliveSpec {
  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  val config: Config = ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.http.server.preview.enable-http2 = on
    """)

  val KeepAliveStreamOut: StreamOut = StreamOut(StreamOut.Message.KeepAlive(KeepAlive.defaultInstance))

  def filteredEvent(pid: String, seqNr: Long): StreamOut = {
    val now = Instant.now()
    StreamOut(
      StreamOut.Message.FilteredEvent(
        FilteredEvent(
          pid,
          seqNr,
          slice = 0,
          offset = Seq(Offset(Some(Timestamp(now.getEpochSecond, now.getNano)), Seq(PersistenceIdSeqNr(pid, seqNr)))))))
  }

  // stand-in for a producer, emitting a canned stream per stream id and forwarding what the consumer sends
  class StubProducerService(
      initProbe: () => ActorRef[InitReq],
      streamInProbe: () => ActorRef[StreamIn],
      streamOutFor: String => Source[StreamOut, NotUsed])
      extends EventProducerServicePowerApi {

    override def eventsBySlices(in: Source[StreamIn, NotUsed], metadata: Metadata): Source[StreamOut, NotUsed] =
      in.prefixAndTail(1).flatMapConcat {
        case (Seq(StreamIn(StreamIn.Message.Init(init), _)), tail) =>
          initProbe() ! init
          val probe = streamInProbe()
          tail.via(Flow.fromSinkAndSource(Sink.foreach[StreamIn](probe ! _), streamOutFor(init.streamId)))
        case other =>
          throw new IllegalArgumentException(s"Unexpected first message $other")
      }

    override def eventTimestamp(in: EventTimestampRequest, metadata: Metadata): Future[EventTimestampResponse] =
      Future.failed(new UnsupportedOperationException)

    override def loadEvent(in: LoadEventRequest, metadata: Metadata): Future[LoadEventResponse] =
      Future.failed(new UnsupportedOperationException)

    override def latestEventTimestamp(
        in: LatestEventTimestampRequest,
        metadata: Metadata): Future[LatestEventTimestampResponse] =
      Future.failed(new UnsupportedOperationException)

    override def currentEventsByPersistenceId(
        in: CurrentEventsByPersistenceIdRequest,
        metadata: Metadata): Source[StreamOut, NotUsed] =
      Source.failed(new UnsupportedOperationException)
  }
}

class GrpcReadJournalKeepAliveSpec
    extends ScalaTestWithActorTestKit(GrpcReadJournalKeepAliveSpec.config)
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with LogCapturing {
  import GrpcReadJournalKeepAliveSpec._

  @volatile private var initProbe = createTestProbe[InitReq]()
  // replaced per test that looks at what the consumer sends, so that streams of earlier tests do not pollute it
  @volatile private var streamInProbe = createTestProbe[StreamIn]()

  private val streamWithKeepAlives = "stream-with-keep-alives"
  private val streamWithTwoEvents = "stream-with-two-events"
  private val keepAliveThenSilent = "keep-alive-then-silent"
  private val delayedKeepAliveStream = "delayed-keep-alive-stream"
  private val eventsOnlyStream = "events-only-stream"
  private val rejectedStream = "rejected-stream"
  private val silentStream = "silent-stream"

  private val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val service = new StubProducerService(() => initProbe.ref, () => streamInProbe.ref, {
      case `streamWithKeepAlives` =>
        // like a producer supporting keep alives, one event and then only keep alives
        Source(List(KeepAliveStreamOut, filteredEvent("pid-1", 1L)))
          .concat(Source.tick(500.millis, 500.millis, KeepAliveStreamOut).mapMaterializedValue(_ => NotUsed))
      case `streamWithTwoEvents` =>
        Source(List(KeepAliveStreamOut, filteredEvent("pid-1", 1L), filteredEvent("pid-1", 2L)))
          .concat(Source.tick(500.millis, 500.millis, KeepAliveStreamOut).mapMaterializedValue(_ => NotUsed))
      case `keepAliveThenSilent` =>
        // like a producer supporting keep alives whose connection went stale
        Source.single(KeepAliveStreamOut).concat(Source.maybe[StreamOut])
      case `delayedKeepAliveStream` =>
        // like a producer that takes a while before the stream is up
        Source.tick(3.seconds, 500.millis, KeepAliveStreamOut).mapMaterializedValue(_ => NotUsed)
      case `eventsOnlyStream` =>
        // like a producer that is busy and never idle long enough to send keep alives
        Source
          .single(KeepAliveStreamOut)
          .concat(
            Source
              .tick(500.millis, 500.millis, NotUsed)
              .zipWithIndex
              .map { case (_, seqNr) => filteredEvent("pid-1", seqNr + 1) }
              .mapMaterializedValue(_ => NotUsed))
      case `rejectedStream` =>
        // like an interceptor rejecting the request, before any keep alive
        Source.failed(new akka.grpc.GrpcServiceException(io.grpc.Status.PERMISSION_DENIED))
      case _ =>
        // like a producer not supporting keep alives
        Source.maybe[StreamOut].mapMaterializedValue(_ => NotUsed)
    })
    Http()
      .newServerAt("127.0.0.1", grpcPort)
      .bind(EventProducerServicePowerApiHandler(service))
      .futureValue
  }

  private def eventsBySlices(settings: GrpcQuerySettings) =
    GrpcReadJournal(settings, clientSettings, protobufDescriptors = Nil)
      .eventsBySlices[String](settings.streamId, 0, 1023, NoOffset)
      .runWith(TestSink[EventEnvelope[String]]())

  "GrpcReadJournal keep alive" must {

    "not request keep alives by default" in {
      val probe = eventsBySlices(GrpcQuerySettings(silentStream))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 0L
      probe.cancel()
    }

    "request keep alives and drop received keep alives" in {
      val probe = eventsBySlices(
        GrpcQuerySettings(streamWithKeepAlives).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      val envelope = probe.expectNext()
      envelope.persistenceId shouldBe "pid-1"
      envelope.filtered shouldBe true

      // keep alives arriving every 500ms are dropped and keep the stream from timing out
      probe.expectNoMessage(4.seconds)
      probe.cancel()
    }

    "fail stream when the producer stops sending keep alives" in {
      val probe = eventsBySlices(
        GrpcQuerySettings(keepAliveThenSilent).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      val error = probe.within(5.seconds)(probe.expectError())
      error shouldBe a[KeepAliveTimeoutException]
      error.getMessage should include("No keep alive or event received from producer within [2 seconds]")
    }

    "not fail stream when the producer does not support keep alives" in {
      val probe =
        eventsBySlices(GrpcQuerySettings(silentStream).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      // timeout is never armed since no keep alive arrived, like an older producer
      probe.expectNoMessage(4.seconds)
      probe.cancel()
    }

    "not fail stream while downstream is busy and not requesting" in {
      val probe = eventsBySlices(
        GrpcQuerySettings(streamWithTwoEvents).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(1)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L
      probe.expectNext().sequenceNr shouldBe 1L

      // like a slow handler with the second event pending, no demand so no timeout
      probe.expectNoMessage(5.seconds)
      probe.request(1)
      probe.expectNext().sequenceNr shouldBe 2L
      probe.cancel()
    }

    "not fail silent stream when no keep alive timeout is set" in {
      val probe = eventsBySlices(GrpcQuerySettings(silentStream).withKeepAliveInterval(1.second))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      probe.expectNoMessage(3.seconds)
      probe.cancel()
    }

    "send keep alives to the producer once it has sent its first keep alive" in {
      streamInProbe = createTestProbe[StreamIn]()
      val probe = eventsBySlices(
        GrpcQuerySettings(streamWithKeepAlives).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(10)
      val init = initProbe.receiveMessage()
      init.keepAliveIntervalMillis shouldBe 1000L
      init.keepAliveTimeoutMillis shouldBe 2000L

      streamInProbe.receiveMessage(2.seconds).message.isKeepAlive shouldBe true
      streamInProbe.receiveMessage(2.seconds).message.isKeepAlive shouldBe true
      probe.cancel()
    }

    "not send keep alives to a producer that does not support them" in {
      streamInProbe = createTestProbe[StreamIn]()
      val probe =
        eventsBySlices(GrpcQuerySettings(silentStream).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      // older producers would warn about the unknown message
      streamInProbe.expectNoMessage(3.seconds)
      probe.cancel()
    }

    "not fail stream while events flow without keep alives" in {
      val probe = eventsBySlices(
        GrpcQuerySettings(eventsOnlyStream).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(100)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      val received = probe.receiveWithin(4.seconds)
      received.size should be >= 5
      received.map(_.sequenceNr) shouldBe (1L to received.size)
      probe.cancel()
    }

    "not send keep alives when the producer rejects the stream" in {
      streamInProbe = createTestProbe[StreamIn]()
      val probe =
        eventsBySlices(
          GrpcQuerySettings(rejectedStream).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      // note: combining the within call and the matcher in one expression made expectError time out
      val error = probe.within(10.seconds)(probe.expectError())
      error should not be a[KeepAliveTimeoutException]
      streamInProbe.expectNoMessage(2.seconds)
    }

    "start a fresh keep alive handshake for each materialization" in {
      val source = GrpcReadJournal(
        GrpcQuerySettings(delayedKeepAliveStream).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds),
        clientSettings,
        protobufDescriptors = Nil).eventsBySlices[String](delayedKeepAliveStream, 0, 1023, NoOffset)

      streamInProbe = createTestProbe[StreamIn]()
      val probe1 = source.runWith(TestSink[EventEnvelope[String]]())
      probe1.request(10)
      // first producer keep alive after 3 seconds, consumer keep alive one interval later
      streamInProbe.receiveMessage(6.seconds).message.isKeepAlive shouldBe true
      probe1.cancel()

      // same source again, must wait for the first producer keep alive of the new stream
      streamInProbe = createTestProbe[StreamIn]()
      val probe2 = source.runWith(TestSink[EventEnvelope[String]]())
      probe2.request(10)
      streamInProbe.expectNoMessage(2500.millis)
      streamInProbe.receiveMessage(4.seconds).message.isKeepAlive shouldBe true
      probe2.cancel()
    }

    "keep streams independent on the same read journal" in {
      initProbe = createTestProbe[InitReq]()
      streamInProbe = createTestProbe[StreamIn]()
      val readJournal = GrpcReadJournal(
        GrpcQuerySettings(streamWithKeepAlives).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds),
        clientSettings,
        protobufDescriptors = Nil)
      def run(minSlice: Int, maxSlice: Int) =
        readJournal
          .eventsBySlices[String](streamWithKeepAlives, minSlice, maxSlice, NoOffset)
          .runWith(TestSink[EventEnvelope[String]]())

      val probe1 = run(0, 511)
      val probe2 = run(512, 1023)
      probe1.request(10)
      probe2.request(10)
      initProbe.receiveMessages(2).map(init => init.sliceMin -> init.sliceMax).toSet shouldBe Set(0 -> 511, 512 -> 1023)
      probe1.expectNext().persistenceId shouldBe "pid-1"
      probe2.expectNext().persistenceId shouldBe "pid-1"

      // both streams send keep alives
      streamInProbe.receiveMessages(2, 3.seconds).forall(_.message.isKeepAlive) shouldBe true
      probe1.cancel()

      // the other stream keeps going, with its own keep alives, and does not time out
      streamInProbe.receiveMessages(2, 4.seconds).forall(_.message.isKeepAlive) shouldBe true
      probe2.expectNoMessage(3.seconds)
      probe2.cancel()
      readJournal.close().futureValue
    }

    "reject invalid keep alive settings when the read journal is created" in {
      def create(settings: GrpcQuerySettings): GrpcReadJournal =
        GrpcReadJournal(settings, clientSettings, protobufDescriptors = Nil)

      intercept[IllegalArgumentException](create(GrpcQuerySettings(silentStream).withKeepAliveTimeout(2.seconds)))
      intercept[IllegalArgumentException](create(GrpcQuerySettings(silentStream).withKeepAliveInterval(500.millis)))
      intercept[IllegalArgumentException](create(GrpcQuerySettings(silentStream).withKeepAliveInterval(2.hours)))
      intercept[IllegalArgumentException](
        create(GrpcQuerySettings(silentStream).withKeepAliveInterval(1.second).withKeepAliveTimeout(1500.millis)))
      // order of setters does not matter
      create(GrpcQuerySettings(silentStream).withKeepAliveTimeout(2.seconds).withKeepAliveInterval(1.second))
        .close()
        .futureValue
    }
  }
}
