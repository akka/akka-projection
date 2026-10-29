/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.GrpcServiceException
import akka.http.scaladsl.Http
import akka.persistence.Persistence
import akka.persistence.query.NoOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.projection.ProjectionBehavior
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.TestEntity
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.internal.proto.EventProducerServiceClient
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.KeepAlive
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.grpc.Status
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

object KeepAliveSpec {
  val config: Config =
    ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    # low latency queries so that events show up promptly after the idle period
    akka.persistence.r2dbc.query {
      refresh-interval = 500 millis
      backtracking.behind-current-time = 3 seconds
    }
    akka.actor.testkit.typed.filter-leeway = 10s
    """)
}

class KeepAliveSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(KeepAliveSpec.config.withFallback(testContainerConf.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {

  def this() = this(new TestContainerConf)

  override def typedSystem: ActorSystem[_] = system

  private val grpcPort = testContainerConf.grpcPort
  private val entityType = nextEntityType()
  private val streamId = "stream_id_keep_alive"
  private val pid = nextPid(entityType)
  // separate stream so that events of the other tests do not show up
  private val sharedEntityType = nextEntityType()
  private val sharedStreamId = "stream_id_keep_alive_shared"

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val eventProducerSource =
      EventProducerSource(entityType, streamId, Transformation.identity, EventProducerSettings(system))
    val sharedEventProducerSource =
      EventProducerSource(sharedEntityType, sharedStreamId, Transformation.identity, EventProducerSettings(system))

    Http()
      .newServerAt("127.0.0.1", grpcPort)
      .bind(EventProducer.grpcServiceHandler(Set(eventProducerSource, sharedEventProducerSource)))
      .futureValue
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    testContainerConf.stop()
  }

  "GrpcReadJournal with keep alive" must {
    "keep the event stream alive while idle" in {
      val replyProbe = createTestProbe[Done]()
      val entity = spawn(TestEntity(pid))
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      val readJournal = GrpcReadJournal(
        GrpcQuerySettings(streamId).withKeepAliveInterval(1.second).withKeepAliveTimeout(3.seconds),
        GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false),
        protobufDescriptors = Nil)

      val probe = readJournal
        .eventsBySlices[String](streamId, 0, 1023, NoOffset)
        .filterNot(env => EnvelopeOrigin.fromBacktracking(env))
        .runWith(TestSink[EventEnvelope[String]]())
      probe.request(10)

      val envelopeA = probe.expectNext()
      envelopeA.persistenceId shouldBe pid.id
      envelopeA.event shouldBe "a"

      // idle for longer than the keep alive timeout without the stream failing
      probe.expectNoMessage(6.seconds)

      entity ! TestEntity.Persist("b")
      val envelopeB = probe.expectNext()
      envelopeB.persistenceId shouldBe pid.id
      envelopeB.event shouldBe "b"

      probe.cancel()
      readJournal.close().futureValue
    }

    "send keep alives from the producer when requested" in {
      // raw client, since the read journal drops the keep alives
      val client =
        EventProducerServiceClient(GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false))
      val initReq = InitReq(streamId, 0, 1023, offset = Seq.empty, keepAliveIntervalMillis = 1000)
      val probe = client
        .eventsBySlices(Source.single(StreamIn(StreamIn.Message.Init(initReq))).concat(Source.maybe))
        .runWith(TestSink[StreamOut]())
      probe.request(100)

      // first keep alive right away, then at the interval in between the events from the previous test
      probe.expectNext(2.seconds).message.isKeepAlive shouldBe true
      probe.receiveWithin(2500.millis).count(_.message.isKeepAlive) should be >= 1

      probe.cancel()
      client.close().futureValue
    }

    "close the stream on the producer when keep alives from the consumer stop" in {
      val client =
        EventProducerServiceClient(GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false))
      val initReq =
        InitReq(streamId, 0, 1023, offset = Seq.empty, keepAliveIntervalMillis = 1000, keepAliveTimeoutMillis = 2000)
      // like a consumer that went away after its first keep alive
      val consumerKeepAlive = StreamIn(StreamIn.Message.KeepAlive(KeepAlive.defaultInstance))
      val probe = client
        .eventsBySlices(Source(List(StreamIn(StreamIn.Message.Init(initReq)), consumerKeepAlive)).concat(Source.maybe))
        .runWith(TestSink[StreamOut]())
      probe.request(100)

      // events and producer keep alives keep coming until the producer gives up on the consumer
      def awaitError(): Throwable = probe.expectNextOrError() match {
        case Left(failure) => failure
        case Right(_)      => awaitError()
      }
      val error = probe.within(6.seconds)(awaitError())
      error shouldBe a[GrpcServiceException]
      val status = error.asInstanceOf[GrpcServiceException].status
      status.getCode shouldBe Status.Code.UNAVAILABLE
      status.getDescription should include("No keep alive from consumer within [2 seconds]")

      client.close().futureValue
    }

    "not restart a projection whose handler is slower than the keep alive timeout" in {
      // events already waiting when the projection starts, so the handler is busy with one while more are pending
      val slowPid = nextPid(entityType)
      val replyProbe = createTestProbe[Done]()
      val entity = spawn(TestEntity(slowPid))
      entity ! TestEntity.Persist("c")
      entity ! TestEntity.Persist("d")
      entity ! TestEntity.Persist("e")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      val processedProbe = createTestProbe[String]()
      val sourceProvider = EventSourcedProvider.eventsBySlices[String](
        system,
        GrpcReadJournal(
          GrpcQuerySettings(streamId).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds),
          GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false),
          protobufDescriptors = Nil),
        streamId,
        0,
        1023)
      val handler = () =>
        new Handler[EventEnvelope[String]] {
          override def process(envelope: EventEnvelope[String]): Future[Done] =
            if (envelope.persistenceId == slowPid.id && !EnvelopeOrigin.fromBacktracking(envelope)) {
              // longer than the keep alive timeout
              akka.pattern.after(2500.millis) {
                processedProbe.ref ! envelope.event
                Future.successful(Done)
              }
            } else Future.successful(Done)
        }
      val projection = spawn(
        ProjectionBehavior(
          R2dbcProjection.atLeastOnceAsync(randomProjectionId(), settings = None, sourceProvider, handler)))

      // a restart would show up as the in-flight event being processed again
      processedProbe.receiveMessages(3, 15.seconds) shouldBe List("c", "d", "e")
      processedProbe.expectNoMessage(5.seconds)

      projection ! ProjectionBehavior.Stop
    }

    "keep both directions alive with a large backlog and a handler slower than the timeouts" in {
      // enough pending payload to fill the HTTP/2 window towards the consumer while the handler is blocked,
      // the consumer keep alives still have to reach the producer on the other direction
      val backlogPid = nextPid(entityType)
      val numberOfEvents = 400
      val payload = "x" * 4000
      val replyProbe = createTestProbe[Done]()
      val entity = spawn(TestEntity(backlogPid))
      (1 to numberOfEvents).foreach(_ => entity ! TestEntity.Persist(payload))
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage(20.seconds)

      val processedProbe = createTestProbe[Long]()
      val sourceProvider = EventSourcedProvider.eventsBySlices[String](
        system,
        GrpcReadJournal(
          GrpcQuerySettings(streamId).withKeepAliveInterval(1.second).withKeepAliveTimeout(2.seconds),
          GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false),
          protobufDescriptors = Nil),
        streamId,
        0,
        1023)
      val handler = () =>
        new Handler[EventEnvelope[String]] {
          private var blockedOnce = false
          override def process(envelope: EventEnvelope[String]): Future[Done] =
            if (envelope.persistenceId == backlogPid.id && !EnvelopeOrigin.fromBacktracking(envelope)) {
              if (!blockedOnce) {
                blockedOnce = true
                // longer than both the consumer and the producer timeout
                akka.pattern.after(3.seconds) {
                  processedProbe.ref ! envelope.sequenceNr
                  Future.successful(Done)
                }
              } else {
                processedProbe.ref ! envelope.sequenceNr
                Future.successful(Done)
              }
            } else Future.successful(Done)
        }
      val projection = spawn(
        ProjectionBehavior(
          R2dbcProjection.atLeastOnceAsync(randomProjectionId(), settings = None, sourceProvider, handler)))

      processedProbe.receiveMessages(numberOfEvents, 30.seconds) shouldBe (1L to numberOfEvents)
      processedProbe.expectNoMessage(3.seconds)

      projection ! ProjectionBehavior.Stop
    }

    "not time out an idle stream sharing the connection with a stream that has a large unread backlog" in {
      def pidInSlices(minSlice: Int, maxSlice: Int) =
        Iterator
          .continually(nextPid(sharedEntityType))
          .find { p =>
            val slice = Persistence(system).sliceForPersistenceId(p.id)
            slice >= minSlice && slice <= maxSlice
          }
          .get

      val backlogPid = pidInSlices(0, 511)
      val idlePid = pidInSlices(512, 1023)
      // larger than the maximum HTTP/2 window that grpc-java grows to (8 MB)
      val numberOfEvents = 400
      val payload = "x" * 30000
      val replyProbe = createTestProbe[Done]()
      val backlogEntity = spawn(TestEntity(backlogPid))
      (1 to numberOfEvents).foreach(_ => backlogEntity ! TestEntity.Persist(payload))
      backlogEntity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage(30.seconds)

      // both slice ranges share the gRPC channel of the read journal
      val readJournal = GrpcReadJournal(
        GrpcQuerySettings(sharedStreamId).withKeepAliveInterval(1.second).withKeepAliveTimeout(3.seconds),
        GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false),
        protobufDescriptors = Nil)

      val backlogProbe = readJournal
        .eventsBySlices[String](sharedStreamId, 0, 511, NoOffset)
        .runWith(TestSink[EventEnvelope[String]]())
      backlogProbe.request(1)
      backlogProbe.expectNext(10.seconds).persistenceId shouldBe backlogPid.id

      val idleProbe = readJournal
        .eventsBySlices[String](sharedStreamId, 512, 1023, NoOffset)
        .runWith(TestSink[EventEnvelope[String]]())
      idleProbe.request(10)
      idleProbe.expectNoMessage(10.seconds)

      val idleEntity = spawn(TestEntity(idlePid))
      idleEntity ! TestEntity.Persist("f")
      idleProbe.expectNext(10.seconds).event shouldBe "f"

      backlogProbe.cancel()
      idleProbe.cancel()
      readJournal.close().futureValue
    }
  }
}
