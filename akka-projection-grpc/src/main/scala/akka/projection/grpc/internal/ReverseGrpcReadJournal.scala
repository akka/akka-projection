/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.grpc.internal.proto.ConsumerStreamIn
import akka.projection.grpc.internal.proto.ConsumerStreamOut
import akka.projection.grpc.internal.proto.ControlCommand
import akka.projection.grpc.internal.proto.ControlStreamRequest
import akka.projection.grpc.internal.proto.EventConsumerService
import akka.projection.grpc.internal.proto.InitConsumerStream
import akka.projection.internal.CanTriggerReplay
import akka.stream.BoundedSourceQueue
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Flow.Subscriber
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 *
 * Implements both the EventConsumerService gRPC interface and a read journal.
 * The producer connects over gRPC and accepts commands streamed over a control channel, control commands
 * trigger streaming events back from the producer.
 */
// FIXME where/who/what binds this
// FIXME connect to SDP running the projection somehow?
// FIXME how/where do we configure producer to connect
@InternalApi private[akka] class ReverseGrpcReadJournal()(implicit system: ActorSystem[_])
    extends EventConsumerService
    with ReadJournal
    with EventsBySliceQuery
    with EventTimestampQuery
    with LoadEventQuery
    with CanTriggerReplay {

  private val logger = LoggerFactory.getLogger(classOf[EventProducerServiceImpl])
  private implicit val ec: ExecutionContext = system.executionContext
  private lazy val persistenceExt = Persistence(system)

  private val connectionCounter = new AtomicLong()
  // FIXME could it be a broadcast hub instead?
  private val controlStreams = new ConcurrentHashMap[String, BoundedSourceQueue[ControlCommand]]()

  // request-id to stream to send events back through
  private val waitingRequests = new ConcurrentHashMap[String, Sink[ConsumerStreamIn, NotUsed]]()

  // FIXME do we need to keep a set of currently executing queries in case a new control connection appears
  // FIXME we need to query all connected?
  // FIXME manage so we only have one control connection per producer active

  override def control(in: ControlStreamRequest): Source[ControlCommand, NotUsed] = {
    logger.info("Reverse gRPC projection connection from [{}]", in.producerIdentifier)
    val connectionId = s"${in.producerIdentifier}-${connectionCounter.incrementAndGet()}"
    // FIXME where does this stream come from? an SPD on some node?
    // FIXME do we need a keepalive as well? Client can send that periodically?
    // Keep track of available control streams
    // FIXME buffer size, configurable?
    Source.queue[ControlCommand](32).watchTermination() { (queue, termination) =>
      controlStreams.put(connectionId, queue)
      termination.onComplete { _ => controlStreams.remove(connectionId) }
      NotUsed
    }
  }

  override def eventStream(in: Source[ConsumerStreamIn, NotUsed]): Source[ConsumerStreamOut, NotUsed] = {
    in.prefixAndTail(1).flatMapConcat {
      case (Seq(head), tail) =>
        head match {
          case ConsumerStreamIn(ConsumerStreamIn.Message.Init(InitConsumerStream(requestId, Some(initReq), _)), _) =>
            waitingRequests.get(requestId) match {
              case null =>
                logger.warn("Unknown request id [{}] in stream from producer")
                tail.runWith(Sink.cancelled)
              case waitingSink =>
                logger.debugN(
                  "Producer started event stream, request id [{}], stream id [{}], slices [{}-{}]",
                  requestId,
                  initReq.streamId,
                  initReq.sliceMin,
                  initReq.sliceMax)
                // FIXME this isn't good enough, will be a number of event streams from each connected (and future connecting
                //       subscriber), we need to merge those somehow
                waitingRequests.remove(requestId)
                tail.runWith(waitingSink)
            }


            tail
          case unexpected =>
            throw new IllegalArgumentException(
              s"Consumer service event stream must start with InitConsumerStream event, was ${unexpected.getClass}")
        }
      case _ =>
        throw new IllegalArgumentException("Didn't get single prefix and tail")
    }
    // FIXME actually attach a running consumer, passing these events to the one requesting
    //       the stream.
    in.via(Flow.fromSinkAndSource(Sink.foreach(in => logger.info("In {}", in)), Source.never))
  }

  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] = {
    // FIXME turn into control request
    val requestId = UUID.randomUUID().toString
    // register waiting sink/subscriber
    // pass request id to control
    ???
  }


  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    // FIXME turn into control request
    ???
  }

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Event]] = {
    // FIXME turn into control request
    ???
  }

  override private[akka] def triggerReplay(persistenceId: String, fromSeqNr: Long): Unit = {
    // FIXME turn into control request
    ???
  }

  override def sliceForPersistenceId(persistenceId: String): Int =
    persistenceExt.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): Seq[Range] =
    persistenceExt.sliceRanges(numberOfRanges)

}
