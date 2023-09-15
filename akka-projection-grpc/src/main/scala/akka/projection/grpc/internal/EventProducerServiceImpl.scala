/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdStartingFromSnapshotQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.proto.EventProducerServicePowerApi
import akka.projection.grpc.internal.proto.EventTimestampRequest
import akka.projection.grpc.internal.proto.EventTimestampResponse
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.LoadEventRequest
import akka.projection.grpc.internal.proto.LoadEventResponse
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducerInterceptor
import akka.stream.scaladsl.BidiFlow
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import com.google.protobuf.timestamp.Timestamp
import io.grpc.Status
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import scala.concurrent.Future

import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation

/**
 * INTERNAL API
 */
@InternalApi private[akka] object EventProducerServiceImpl {
  val log: Logger =
    LoggerFactory.getLogger(classOf[EventProducerServiceImpl])
  private val futureDone = Future.successful(Done)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventProducerServiceImpl(
    system: ActorSystem[_],
    eventsBySlicesPerStreamId: Map[String, EventsBySliceQuery],
    eventsBySlicesStartingFromSnapshotsPerStreamId: Map[String, EventsBySliceStartingFromSnapshotsQuery],
    currentEventsByPersistenceIdPerStreamId: Map[String, CurrentEventsByPersistenceIdTypedQuery],
    currentEventsByPersistenceIdStartingFromSnapshotPerStreamId: Map[
      String,
      CurrentEventsByPersistenceIdStartingFromSnapshotQuery],
    sources: Set[EventProducer.EventProducerSource],
    interceptor: Option[EventProducerInterceptor])
    extends EventProducerServicePowerApi {
  import EventProducerServiceImpl._
  import akka.projection.grpc.internal.ProtobufProtocolConversions._
  import system.executionContext

  require(
    sources.nonEmpty,
    "Empty set of EventProducerSource passed to EventProducerService, must contain at least one")
  sources.foreach { s =>
    require(s.streamId.nonEmpty, s"EventProducerSource for [${s.entityType}] contains empty stream id, not allowed")
    require(
      eventsBySlicesPerStreamId.contains(s.streamId) ||
      eventsBySlicesStartingFromSnapshotsPerStreamId.contains(s.streamId),
      s"No events by slices query defined for stream id [${s.streamId}]")
    require(
      s.transformation ne Transformation.empty,
      s"Transformation is not defined for stream id [${s.streamId}]. " +
      "Use Transformation.identity to pass through each event as is.")
  }

  private val protoAnySerialization = new ProtoAnySerialization(system)

  private val streamIdToSourceMap: Map[String, EventProducer.EventProducerSource] =
    sources.map(s => s.streamId -> s).toMap

  log.info(
    s"Event producer gRPC service created with available sources [{}]",
    sources
      .map(s => s"(stream id: [${s.streamId}], entity type: [${s.entityType}])")
      .mkString(", "))

  private def intercept(streamId: String, metadata: Metadata): Future[Done] =
    interceptor match {
      case Some(interceptor) => interceptor.intercept(streamId, metadata)
      case None              => futureDone
    }

  private def eventProducerSourceFor(streamId: String): EventProducer.EventProducerSource =
    streamIdToSourceMap.getOrElse(
      streamId,
      throw new GrpcServiceException(
        Status.NOT_FOUND.withDescription(s"Stream id [${streamId}] is not available for consumption")))

  override def eventsBySlices(in: Source[StreamIn, NotUsed], metadata: Metadata): Source[StreamOut, NotUsed] = {
    in.prefixAndTail(1).flatMapConcat {
      case (Seq(StreamIn(StreamIn.Message.Init(init), _)), tail) =>
        tail.via(runEventsBySlices(init, metadata))
      case (Seq(), _) =>
        // if error during recovery in proxy the stream will be completed before init
        log.warn("Event stream closed before init.")
        Source.empty[StreamOut]
      case (Seq(StreamIn(other, _)), _) =>
        throw new IllegalArgumentException(
          "Expected init message for eventsBySlices stream, " +
          s"but received [${other.getClass.getName}]")
      case (seq, _) =>
        // silence warning: match may not be exhaustive
        throw new IllegalStateException(s"Unexpected Seq prefix with [${seq.size}] elements.")
    }
  }

  private def runEventsBySlices(init: InitReq, metadata: Metadata): Flow[StreamIn, StreamOut, NotUsed] = {
    val futureFlow = intercept(init.streamId, metadata).map { _ =>
      val producerSource = eventProducerSourceFor(init.streamId)

      val offset = protocolOffsetToOffset(init.offset)

      log.debugN(
        "Starting eventsBySlices stream [{}], [{}], slices [{} - {}], offset [{}]",
        producerSource.streamId,
        producerSource.entityType,
        init.sliceMin,
        init.sliceMax,
        offset match {
          case t: TimestampOffset => t.timestamp
          case _                  => offset
        })

      val events: Source[EventEnvelope[Any], NotUsed] =
        eventsBySlicesPerStreamId.get(init.streamId) match {
          case Some(query) =>
            query.eventsBySlices[Any](producerSource.entityType, init.sliceMin, init.sliceMax, offset)
          case None =>
            eventsBySlicesStartingFromSnapshotsPerStreamId.get(init.streamId) match {
              case Some(query) =>
                val transformSnapshot = streamIdToSourceMap(init.streamId).transformSnapshot.get
                query.eventsBySlicesStartingFromSnapshots[Any, Any](
                  producerSource.entityType,
                  init.sliceMin,
                  init.sliceMax,
                  offset,
                  transformSnapshot)
              case None =>
                Source.failed(
                  new IllegalArgumentException(s"No events by slices query defined for stream id [${init.streamId}]"))
            }
        }

      val currentEventsByPersistenceId: (String, Long) => Source[EventEnvelope[Any], NotUsed] =
        currentEventsByPersistenceIdPerStreamId.get(init.streamId) match {
          case Some(query) =>
            (pid, seqNr) => query.currentEventsByPersistenceIdTyped[Any](pid, seqNr, Long.MaxValue)
          case None =>
            currentEventsByPersistenceIdStartingFromSnapshotPerStreamId.get(init.streamId) match {
              case Some(query) =>
                val transformSnapshot = streamIdToSourceMap(init.streamId).transformSnapshot.get
                (pid, seqNr) =>
                  query.currentEventsByPersistenceIdStartingFromSnapshot[Any, Any](
                    pid,
                    seqNr,
                    Long.MaxValue,
                    transformSnapshot)
              case None =>
                (_, _) =>
                  Source.failed(
                    new IllegalArgumentException(
                      s"No currentEventsByPersistenceId query defined for stream id [${init.streamId}]"))
            }
        }

      val eventsFlow: Flow[StreamIn, EventEnvelope[Any], NotUsed] =
        BidiFlow
          .fromGraph(
            new FilterStage(
              init.streamId,
              producerSource.entityType,
              init.sliceMin to init.sliceMax,
              init.filter,
              currentEventsByPersistenceId,
              producerFilter = producerSource.producerFilter,
              topicTagPrefix = producerSource.settings.topicTagPrefix,
              replayParallelism = producerSource.settings.replayParallelism))
          .join(Flow.fromSinkAndSource(Sink.ignore, events))

      val eventsStreamOut: Flow[StreamIn, StreamOut, NotUsed] =
        eventsFlow.mapAsync(producerSource.settings.transformationParallelism) { env =>
          import system.executionContext
          transformAndEncodeEvent(producerSource.transformation, env, protoAnySerialization)
            .map {
              case Some(event) =>
                log.traceN(
                  "Emitting event from persistenceId [{}] with seqNr [{}], offset [{}], source [{}]",
                  env.persistenceId,
                  env.sequenceNr,
                  env.offset,
                  event.source)
                StreamOut(StreamOut.Message.Event(event))
              case None =>
                log.traceN(
                  "Filtered event from persistenceId [{}] with seqNr [{}], offset [{}], source [{}]",
                  env.persistenceId,
                  env.sequenceNr,
                  env.offset,
                  env.source)
                StreamOut(
                  StreamOut.Message.FilteredEvent(
                    FilteredEvent(
                      env.persistenceId,
                      env.sequenceNr,
                      env.slice,
                      ProtobufProtocolConversions.offsetToProtoOffset(env.offset))))
            }
        }

      eventsStreamOut
    }
    Flow.futureFlow(futureFlow).mapMaterializedValue(_ => NotUsed)
  }

  override def eventTimestamp(req: EventTimestampRequest, metadata: Metadata): Future[EventTimestampResponse] = {
    intercept(req.streamId, metadata).flatMap { _ =>
      val producerSource = streamIdToSourceMap(req.streamId)
      val entityTypeFromPid = PersistenceId.extractEntityType(req.persistenceId)
      if (entityTypeFromPid != producerSource.entityType) {
        throw new GrpcServiceException(Status.INVALID_ARGUMENT.withDescription(
          s"Persistence id is for a type of entity that is not available for consumption (expected type " +
          s" in persistence id for stream id [${req.streamId}] is [${producerSource.entityType}] but was [$entityTypeFromPid])"))
      }
      eventsBySlicesPerStreamId(req.streamId) match {
        case q: EventTimestampQuery =>
          import system.executionContext
          q.timestampOf(req.persistenceId, req.seqNr).map {
            case Some(instant) => EventTimestampResponse(Some(Timestamp(instant)))
            case None          => EventTimestampResponse.defaultInstance
          }
        case other =>
          Future.failed(
            new UnsupportedOperationException(s"eventTimestamp not supported by [${other.getClass.getName}]"))
      }
    }
  }

  override def loadEvent(req: LoadEventRequest, metadata: Metadata): Future[LoadEventResponse] = {
    intercept(req.streamId, metadata).flatMap { _ =>
      val producerSource = eventProducerSourceFor(req.streamId)
      val entityTypeFromPid = PersistenceId.extractEntityType(req.persistenceId)
      if (entityTypeFromPid != producerSource.entityType)
        throw new GrpcServiceException(Status.INVALID_ARGUMENT.withDescription(
          s"Persistence id is for a type of entity that is not available for consumption (expected type " +
          s" in persistence id for stream id [${req.streamId}] is [${producerSource.entityType}] but was [$entityTypeFromPid])"))
      eventsBySlicesPerStreamId(req.streamId) match {
        case q: LoadEventQuery =>
          import system.executionContext
          q.loadEnvelope[Any](req.persistenceId, req.seqNr)
            .flatMap { env =>
              transformAndEncodeEvent(producerSource.transformation, env, protoAnySerialization)
                .map {
                  case Some(event) =>
                    log.traceN(
                      "Loaded event from persistenceId [{}] with seqNr [{}], offset [{}]",
                      env.persistenceId,
                      env.sequenceNr,
                      env.offset)
                    LoadEventResponse(LoadEventResponse.Message.Event(event))
                  case None =>
                    log.traceN(
                      "Filtered loaded event from persistenceId [{}] with seqNr [{}], offset [{}]",
                      env.persistenceId,
                      env.sequenceNr,
                      env.offset)
                    LoadEventResponse(LoadEventResponse.Message.FilteredEvent(FilteredEvent(
                      persistenceId = env.persistenceId,
                      seqNr = env.sequenceNr,
                      slice = env.slice,
                      offset = ProtobufProtocolConversions.offsetToProtoOffset(env.offset),
                      source = env.source)))
                }
            }
            .recoverWith {
              case e: NoSuchElementException =>
                log.warn(e.getMessage)
                Future.failed(new GrpcServiceException(Status.NOT_FOUND.withDescription(e.getMessage)))
            }
        case other =>
          Future.failed(new UnsupportedOperationException(s"loadEvent not supported by [${other.getClass.getName}]"))
      }
    }
  }
}
