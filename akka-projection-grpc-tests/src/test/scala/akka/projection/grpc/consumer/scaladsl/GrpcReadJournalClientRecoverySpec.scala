/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.scaladsl

import java.time.Instant

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.query.NoOffset
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.internal.proto.CurrentEventsByPersistenceIdRequest
import akka.projection.grpc.internal.proto.EventProducerService
import akka.projection.grpc.internal.proto.EventProducerServiceHandler
import akka.projection.grpc.internal.proto.EventTimestampRequest
import akka.projection.grpc.internal.proto.EventTimestampResponse
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.LatestEventTimestampRequest
import akka.projection.grpc.internal.proto.LatestEventTimestampResponse
import akka.projection.grpc.internal.proto.LoadEventRequest
import akka.projection.grpc.internal.proto.LoadEventResponse
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.testkit.SocketUtil
import com.google.protobuf.timestamp.Timestamp
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object GrpcReadJournalClientRecoverySpec {
  val streamId = "test-stream"
  val timestamp: Instant = Instant.parse("2025-01-01T00:00:00Z")
  val protoTimestamp: Timestamp = Timestamp(timestamp.getEpochSecond, timestamp.getNano)

  class StubEventProducerService(implicit system: ActorSystem[_]) extends EventProducerService {
    override def eventsBySlices(in: Source[StreamIn, NotUsed]): Source[StreamOut, NotUsed] = {
      in.runWith(Sink.ignore)
      Source
        .single(
          StreamOut(
            StreamOut.Message.FilteredEvent(
              FilteredEvent(
                "pid-1",
                1L,
                slice = 0,
                offset = Seq(akka.projection.grpc.internal.proto
                  .Offset(Some(protoTimestamp), Seq(PersistenceIdSeqNr("pid-1", 1L))))))))
        .concat(Source.never)
    }

    override def eventTimestamp(in: EventTimestampRequest): Future[EventTimestampResponse] =
      Future.successful(EventTimestampResponse(Some(protoTimestamp)))

    override def loadEvent(in: LoadEventRequest): Future[LoadEventResponse] =
      Future.failed(new UnsupportedOperationException)

    override def latestEventTimestamp(in: LatestEventTimestampRequest): Future[LatestEventTimestampResponse] =
      Future.successful(LatestEventTimestampResponse(Some(protoTimestamp)))

    override def currentEventsByPersistenceId(in: CurrentEventsByPersistenceIdRequest): Source[StreamOut, NotUsed] =
      Source.empty
  }
}

class GrpcReadJournalClientRecoverySpec
    extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
      akka.http.server.enable-http2 = on
      """))
    with AnyWordSpecLike
    with LogCapturing {
  import GrpcReadJournalClientRecoverySpec._

  private def bindProducer(port: Int): Http.ServerBinding = {
    val handler = EventProducerServiceHandler(new StubEventProducerService()(system))
    Http()(system).newServerAt("127.0.0.1", port).bind(handler).futureValue
  }

  private def clientSettings(port: Int): GrpcClientSettings =
    GrpcClientSettings.connectToServiceAt("127.0.0.1", port)(system).withTls(false)

  private def journal(settings: GrpcClientSettings): GrpcReadJournal =
    GrpcReadJournal(GrpcQuerySettings(streamId), settings, protobufDescriptors = Nil)(system)

  "GrpcReadJournal" must {

    "recover when the producer was unreachable for longer than the client connection attempts" in {
      val port = SocketUtil.temporaryServerAddress("127.0.0.1").getPort
      // low attempts make the client give up quickly, unless the journal overrides it
      val readJournal = journal(clientSettings(port).withConnectionAttempts(1))

      // keep calling, connection attempts are only made when there are calls
      val deadline = 4.seconds.fromNow
      while (deadline.hasTimeLeft()) {
        // a dead client may also throw directly
        Future.delegate(readJournal.latestEventTimestamp(streamId, 0, 1023))(system.executionContext).failed.futureValue
        Thread.sleep(200)
      }

      val binding = bindProducer(port)
      try {
        eventually(timeout(20.seconds), interval(500.millis)) {
          readJournal.latestEventTimestamp(streamId, 0, 1023).futureValue shouldBe Some(timestamp)
        }
      } finally {
        binding.unbind().futureValue
        readJournal.close()
      }
    }

    "create a new client if the current one was closed" in {
      val port = SocketUtil.temporaryServerAddress("127.0.0.1").getPort
      val binding = bindProducer(port)
      val readJournal = journal(clientSettings(port))
      try {
        readJournal.latestEventTimestamp(streamId, 0, 1023).futureValue shouldBe Some(timestamp)

        val firstClient = readJournal.currentClient()
        firstClient.close().futureValue

        LoggingTestKit.warn("creating a new client").expect {
          readJournal.latestEventTimestamp(streamId, 0, 1023).futureValue shouldBe Some(timestamp)
        }
        readJournal.timestampOf("pid-1", 1L).futureValue shouldBe Some(timestamp)
        (readJournal.currentClient() should not).be(theSameInstanceAs(firstClient))

        readJournal.currentClient().close().futureValue
        // a (re)started stream should use a new client
        val env = readJournal.eventsBySlices[Any](streamId, 0, 1023, NoOffset).runWith(Sink.head).futureValue
        env.persistenceId shouldBe "pid-1"
      } finally {
        binding.unbind().futureValue
        readJournal.close()
      }
    }

    "not create a new client after close" in {
      val port = SocketUtil.temporaryServerAddress("127.0.0.1").getPort
      val binding = bindProducer(port)
      val readJournal = journal(clientSettings(port))
      try {
        readJournal.latestEventTimestamp(streamId, 0, 1023).futureValue shouldBe Some(timestamp)

        val client = readJournal.currentClient()
        readJournal.close().futureValue

        readJournal.latestEventTimestamp(streamId, 0, 1023).failed.futureValue
        (readJournal.currentClient() should be).theSameInstanceAs(client)
      } finally {
        binding.unbind().futureValue
      }
    }
  }

}
