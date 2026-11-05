/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.scaladsl

import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext

import scala.concurrent.duration._

import scala.concurrent.Future

import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.grpc.GrpcClientSettings
import akka.grpc.GrpcServiceException
import akka.http.scaladsl.ConnectionContext
import akka.http.scaladsl.Http
import akka.persistence.query.NoOffset
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.internal.ConnectionException
import akka.grpc.scaladsl.Metadata
import akka.projection.grpc.internal.proto.CurrentEventsByPersistenceIdRequest
import akka.projection.grpc.internal.proto.EventProducerServicePowerApi
import akka.projection.grpc.internal.proto.EventProducerServicePowerApiHandler
import akka.projection.grpc.internal.proto.EventTimestampRequest
import akka.projection.grpc.internal.proto.EventTimestampResponse
import akka.projection.grpc.internal.proto.LatestEventTimestampRequest
import akka.projection.grpc.internal.proto.LatestEventTimestampResponse
import akka.projection.grpc.internal.proto.LoadEventRequest
import akka.projection.grpc.internal.proto.LoadEventResponse
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.ConfigFactory
import io.grpc.Status
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object GrpcReadJournalFailureSpec {
  def selfSignedSslContext(): SSLContext = {
    val password = "changeit".toCharArray
    val keyStore = KeyStore.getInstance("PKCS12")
    keyStore.load(getClass.getResourceAsStream("/self-signed-keystore.p12"), password)
    val keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagerFactory.init(keyStore, password)
    val context = SSLContext.getInstance("TLS")
    context.init(keyManagerFactory.getKeyManagers, null, null)
    context
  }

  // producer failing every event stream
  class FailingProducerService extends EventProducerServicePowerApi {
    override def eventsBySlices(in: Source[StreamIn, NotUsed], metadata: Metadata): Source[StreamOut, NotUsed] =
      Source.failed(new GrpcServiceException(Status.INTERNAL.withDescription("producer side boom details")))

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

class GrpcReadJournalFailureSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
      akka.http.server.preview.enable-http2 = on
      """))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with LogCapturing {
  import GrpcReadJournalFailureSpec._

  private var grpcPort = 0
  private var tlsPort = 0
  private var unusedPort = 0

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val handler = EventProducerServicePowerApiHandler(new FailingProducerService)
    grpcPort = Http().newServerAt("127.0.0.1", 0).bind(handler).futureValue.localAddress.getPort
    tlsPort = Http()
      .newServerAt("127.0.0.1", 0)
      .enableHttps(ConnectionContext.httpsServer(selfSignedSslContext()))
      .bind(handler)
      .futureValue
      .localAddress
      .getPort
    // bound and unbound again, so nothing listens on it
    val unused = Http().newServerAt("127.0.0.1", 0).bind(handler).futureValue
    unusedPort = unused.localAddress.getPort
    unused.unbind().futureValue
  }

  private def eventsBySlices(port: Int, tls: Boolean = false) = {
    val settings = GrpcQuerySettings("stream-1")
    GrpcReadJournal(
      settings,
      GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(tls),
      protobufDescriptors = Nil)
      .eventsBySlices[String](settings.streamId, 0, 1023, NoOffset)
      .runWith(TestSink[EventEnvelope[String]]())
  }

  "GrpcReadJournal failures" must {

    // projection restart logs only include the exception toString until verbose logging kicks in
    "include the cause details in the failure message" in {
      val probe = eventsBySlices(grpcPort)
      probe.request(10)
      val error = probe.within(5.seconds)(probe.expectError())
      error.toString should include("producer side boom details")
    }

    "include the cause details in connection failures" in {
      val probe = eventsBySlices(unusedPort)
      probe.request(10)
      val error = probe.within(10.seconds)(probe.expectError())
      error shouldBe a[ConnectionException]
      error.getCause should not be null
      error.toString should include("UNAVAILABLE")
      error.toString should include("Connection refused")
    }

    "include the nested TLS failure reason in connection failures" in {
      val probe = eventsBySlices(tlsPort, tls = true)
      probe.request(10)
      val error = probe.within(10.seconds)(probe.expectError())
      error shouldBe a[ConnectionException]
      error.getMessage should include("SSLHandshakeException")
      error.getMessage should include("unable to find valid certification path")
      (error.getMessage should not).include("\n")
    }

    "keep connection failure messages to a bounded single line" in {
      // TLS against plaintext, netty includes a hex dump of the received bytes in the exception message
      val probe = eventsBySlices(grpcPort, tls = true)
      probe.request(10)
      val error = probe.within(10.seconds)(probe.expectError())
      error shouldBe a[ConnectionException]
      error.getMessage should include("NotSslRecordException")
      (error.getMessage should not).include("\n")
      error.getMessage.length should be < 1000
    }
  }
}
