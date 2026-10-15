/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.scaladsl

import java.security.KeyStore
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SSLContext

import scala.concurrent.duration._

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
import akka.projection.grpc.internal.proto.EventProducerServicePowerApiHandler
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.SocketUtil
import com.typesafe.config.ConfigFactory
import io.grpc.Status
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

object GrpcReadJournalFailureSpec {
  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort
  val tlsPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort
  val unusedPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

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
  import GrpcReadJournalKeepAliveSpec.StubProducerService

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val service = new StubProducerService(
      createTestProbe[InitReq]().ref,
      _ =>
        Source
          .failed[StreamOut](new GrpcServiceException(Status.INTERNAL.withDescription("producer side boom details")))
          .mapMaterializedValue(_ => NotUsed))
    val handler = EventProducerServicePowerApiHandler(service)
    Http().newServerAt("127.0.0.1", grpcPort).bind(handler).futureValue
    Http()
      .newServerAt("127.0.0.1", tlsPort)
      .enableHttps(ConnectionContext.httpsServer(selfSignedSslContext()))
      .bind(handler)
      .futureValue
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
