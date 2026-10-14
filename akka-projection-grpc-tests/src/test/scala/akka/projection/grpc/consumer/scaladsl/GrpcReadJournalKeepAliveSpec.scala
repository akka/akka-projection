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
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object GrpcReadJournalKeepAliveSpec {
  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  val config = ConfigFactory.parseString("""
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

  // stand-in for a producer, emitting a canned stream per stream id
  class StubProducerService(initProbe: ActorRef[InitReq], streamOutFor: String => Source[StreamOut, NotUsed])
      extends EventProducerServicePowerApi {

    override def eventsBySlices(in: Source[StreamIn, NotUsed], metadata: Metadata): Source[StreamOut, NotUsed] =
      in.prefixAndTail(1).flatMapConcat {
        case (Seq(StreamIn(StreamIn.Message.Init(init), _)), tail) =>
          initProbe ! init
          tail.via(Flow.fromSinkAndSource(Sink.ignore, streamOutFor(init.streamId)))
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

  private val initProbe = createTestProbe[InitReq]()

  private val streamWithKeepAlives = "stream-with-keep-alives"
  private val silentStream = "silent-stream"

  private val clientSettings = GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val service = new StubProducerService(initProbe.ref, {
      case `streamWithKeepAlives` =>
        // like a producer supporting keep alives, one event and then only keep alives
        Source
          .single(filteredEvent("pid-1", 1L))
          .concat(Source.tick(0.millis, 500.millis, KeepAliveStreamOut).mapMaterializedValue(_ => NotUsed))
      case _ =>
        // like a producer not supporting keep alives, or a stale connection
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

    "fail stream when no keep alives are received within the timeout" in {
      val probe =
        eventsBySlices(GrpcQuerySettings(silentStream).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      val error = probe.within(5.seconds)(probe.expectError())
      error.getMessage should include("No events or keep alive messages received within [2 seconds]")
    }

    "not fail silent stream when no keep alive timeout is set" in {
      val probe = eventsBySlices(GrpcQuerySettings(silentStream).withKeepAliveInterval(1.second))
      probe.request(10)
      initProbe.receiveMessage().keepAliveIntervalMillis shouldBe 1000L

      probe.expectNoMessage(3.seconds)
      probe.cancel()
    }
  }
}
