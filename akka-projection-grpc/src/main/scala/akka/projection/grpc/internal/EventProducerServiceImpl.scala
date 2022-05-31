/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import java.time.Instant

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.EventProducerService
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.Offset
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.google.protobuf.timestamp.Timestamp
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi private[akka] object EventProducerServiceImpl {
  val log: Logger =
    LoggerFactory.getLogger(classOf[EventProducerServiceImpl])

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventProducerServiceImpl(
    system: ActorSystem[_],
    eventsBySlicesQuery: EventsBySliceQuery,
    transformation: EventProducer.Transformation)
    extends EventProducerService {
  import EventProducerServiceImpl.log

  private val protoAnySerialization =
    new ProtoAnySerialization(system, protoClassMapping = Map.empty)

  override def eventsBySlices(
      in: Source[StreamIn, NotUsed]): Source[StreamOut, NotUsed] = {
    in.prefixAndTail(1).flatMapConcat {
      case (Seq(StreamIn(StreamIn.Message.Init(init), _)), tail) =>
        tail.via(runEventsBySlices(init, tail))
      case (Seq(), _) =>
        // if error during recovery in proxy the stream will be completed before init
        log.warn("Event stream closed before init.")
        Source.empty[StreamOut]
      case (Seq(StreamIn(other, _)), _) =>
        throw new IllegalArgumentException(
          "Expected init message for eventsBySlices stream, " +
          s"but received [${other.getClass.getName}]")
    }
  }

  private def runEventsBySlices(
      init: InitReq,
      nextReq: Source[StreamIn, NotUsed])
      : Flow[StreamIn, StreamOut, NotUsed] = {
    val entityType = init.entityType
    val offset = init.offset match {
      case None => NoOffset
      case Some(o) =>
        val timestamp =
          o.timestamp.map(_.asJavaInstant).getOrElse(Instant.EPOCH)
        val seen = o.seen.map { case PersistenceIdSeqNr(pid, seqNr, _) =>
          pid -> seqNr
        }.toMap
        TimestampOffset(timestamp, seen)
    }

    log.info(
      "Starting eventsBySlices stream [{}], slices [{} - {}], offset [{}]",
      entityType,
      init.sliceMin,
      init.sliceMax,
      offset)

    val events: Source[EventEnvelope[Any], NotUsed] =
      eventsBySlicesQuery
        .eventsBySlices[Any](entityType, init.sliceMin, init.sliceMax, offset)

    val eventsStreamOut: Source[StreamOut, NotUsed] =
      events
        .filterNot(
          _.eventOption.isEmpty
        ) // FIXME backtracking events not handled yet
        .mapAsync(1) { env =>
          val protoOffset = env.offset match {
            case TimestampOffset(timestamp, _, seen) =>
              val protoTimestamp = Timestamp(timestamp)
              val protoSeen = seen.iterator.map { case (pid, seqNr) =>
                PersistenceIdSeqNr(pid, seqNr)
              }.toSeq
              Offset(Some(protoTimestamp), protoSeen)
            case other =>
              throw new IllegalArgumentException(
                s"Unexpected offset type [$other]")
          }

          val f = transformation.mappers
            .getOrElse(env.event.getClass, transformation.orElse)
          import system.executionContext
          f(env.event).map {
            case Some(transformedEvent) =>
              // FIXME remove too verbose logging here
              log.debug(
                "Emitting event from [{}] with seqNr [{}], offset [{}]",
                env.persistenceId,
                env.sequenceNr,
                env.offset)

              val protoEvent = protoAnySerialization.encode(transformedEvent)

              StreamOut(
                StreamOut.Message.Event(
                  Event(
                    env.persistenceId,
                    env.sequenceNr,
                    env.slice,
                    Some(protoOffset),
                    Some(protoEvent))))

            case None =>
              // FIXME remove too verbose logging here
              log.debug(
                "Filtered event from [{}] with seqNr [{}], offset [{}]",
                env.persistenceId,
                env.sequenceNr,
                env.offset)
              StreamOut(
                StreamOut.Message.FilteredEvent(
                  FilteredEvent(
                    env.persistenceId,
                    env.sequenceNr,
                    env.slice,
                    Some(protoOffset))))
          }
        }

    // FIXME nextReq not handled yet
    Flow.fromSinkAndSource(Sink.ignore, eventsStreamOut)
  }

}
