/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
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

import akka.persistence.FilteredPayload
import akka.persistence.query.typed.scaladsl.LatestEventTimestampQuery
import akka.projection.grpc.internal.proto.CurrentEventsByPersistenceIdRequest
import akka.projection.grpc.internal.proto.LatestEventTimestampRequest
import akka.projection.grpc.internal.proto.LatestEventTimestampResponse
import akka.projection.grpc.internal.proto.ReplicaInfo
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

  private val protoAnyWireSerialization = new ProtoAnySerialization(system)
  private val akkaOnlyWireSerialization = new DelegateToAkkaSerialization(system)

  private val streamIdToSourceMap: Map[String, EventProducer.EventProducerSource] =
    sources.map(s => s.streamId -> s).toMap

  log.info(
    s"Event producer gRPC service created with available sources [{}]",
    sources
      .map(s => s"(stream id: [${s.streamId}], entity type: [${s.entityType}])")
      .mkString(", "))

  private def wireSerialization(eps: EventProducer.EventProducerSource): ProjectionGrpcSerialization =
    if (eps.settings.akkaSerializationOnly) akkaOnlyWireSerialization
    else protoAnyWireSerialization

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

      log.debug(
        "Starting eventsBySlices stream [{}], [{}], slices [{} - {}], offset [{}]{}",
        producerSource.streamId,
        producerSource.entityType,
        init.sliceMin,
        init.sliceMax,
        offset match {
          case t: TimestampOffset => t.timestamp
          case _                  => offset
        },
        init.replicaInfo.fold("")(ri => s", remote replica [${ri.replicaId}]"))

      val events: Source[EventEnvelope[Any], NotUsed] =
        (eventsBySlicesPerStreamId.get(init.streamId) match {
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
        }).via(transformMetadata(producerSource))

      val currentEventsByPersistenceId: (String, Long) => Source[EventEnvelope[Any], NotUsed] = { (pid, seqNr) =>
        currentEventsByPersistenceIdSource(init.streamId, pid, seqNr, Long.MaxValue)
          .via(transformMetadata(producerSource))
      }

      val eventOriginFilter: EventEnvelope[_] => Boolean =
        producerSource.replicatedEventOriginFilter
          .flatMap(f => init.replicaInfo.map(f.createFilter))
          .getOrElse((_: EventEnvelope[_]) => true)

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
              eventOriginFilter,
              topicTagPrefix = producerSource.settings.topicTagPrefix,
              replayParallelism = producerSource.settings.replayParallelism))
          .join(Flow.fromSinkAndSource(Sink.ignore, events))

      eventsFlow.via(transformEnvelopeToStreamOut(producerSource, init.replicaInfo))
    }
    Flow.futureFlow(futureFlow).mapMaterializedValue(_ => NotUsed)
  }

  private def transformMetadata(
      producerSource: EventProducer.EventProducerSource): Flow[EventEnvelope[Any], EventEnvelope[Any], NotUsed] = {
    if (producerSource.hasReplicatedEventMetadataTransformation)
      Flow[EventEnvelope[Any]].map { env =>
        producerSource.replicatedEventMetadataTransformation(env) match {
          case None           => env
          case Some(metadata) => env.withMetadata(metadata)
        }
      }
    else
      Flow[EventEnvelope[Any]]
  }

  private def currentEventsByPersistenceIdSource(
      streamId: String,
      pid: String,
      fromSeqNr: Long,
      toSeqNr: Long): Source[EventEnvelope[Any], NotUsed] = {
    currentEventsByPersistenceIdPerStreamId.get(streamId) match {
      case Some(query) =>
        query
          .currentEventsByPersistenceIdTyped[Any](pid, fromSeqNr, toSeqNr)
      case None =>
        currentEventsByPersistenceIdStartingFromSnapshotPerStreamId.get(streamId) match {
          case Some(query) =>
            val transformSnapshot = streamIdToSourceMap(streamId).transformSnapshot.get
            query
              .currentEventsByPersistenceIdStartingFromSnapshot[Any, Any](pid, fromSeqNr, toSeqNr, transformSnapshot)
          case None =>
            Source.failed(
              new IllegalArgumentException(s"No currentEventsByPersistenceId query defined for stream id [$streamId]"))
        }
    }
  }

  private def transformEnvelopeToStreamOut(
      producerSource: EventProducer.EventProducerSource,
      replicaInfo: Option[ReplicaInfo]): Flow[EventEnvelope[Any], StreamOut, NotUsed] = {
    Flow[EventEnvelope[Any]].mapAsync(producerSource.settings.transformationParallelism) { env =>
      env.eventOption match {
        case Some(FilteredPayload) =>
          if (log.isTraceEnabled)
            log.trace(
              "Filtered event, due to origin, from persistenceId [{}] with seqNr [{}], offset [{}], source [{}]{}",
              env.persistenceId,
              env.sequenceNr,
              env.offset,
              env.source,
              replicaInfo.fold("")(ri => s", remote replica [${ri.replicaId}]"))
          Future.successful(
            StreamOut(
              StreamOut.Message.FilteredEvent(
                FilteredEvent(
                  env.persistenceId,
                  env.sequenceNr,
                  env.slice,
                  ProtobufProtocolConversions.offsetToProtoOffset(env.offset)))))
        case _ =>
          import system.executionContext
          transformAndEncodeEvent(producerSource.transformation, env, wireSerialization(producerSource))
            .map {
              case Some(event) =>
                if (log.isTraceEnabled)
                  log.trace(
                    "Emitting event from persistenceId [{}] with seqNr [{}], offset [{}], source [{}]{}",
                    env.persistenceId,
                    env.sequenceNr,
                    env.offset,
                    event.source,
                    replicaInfo.fold("")(ri => s", remote replica [${ri.replicaId}]"))
                StreamOut(StreamOut.Message.Event(event))
              case None =>
                if (log.isTraceEnabled)
                  log.trace(
                    "Filtered event from persistenceId [{}] with seqNr [{}], offset [{}], source [{}]{}",
                    env.persistenceId,
                    env.sequenceNr,
                    env.offset,
                    env.source,
                    replicaInfo.fold("")(ri => s", remote replica [${ri.replicaId}]"))
                StreamOut(
                  StreamOut.Message.FilteredEvent(
                    FilteredEvent(
                      env.persistenceId,
                      env.sequenceNr,
                      env.slice,
                      ProtobufProtocolConversions.offsetToProtoOffset(env.offset))))
            }
      }
    }
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
            .map { env =>
              producerSource.replicatedEventMetadataTransformation(env) match {
                case None           => env
                case Some(metadata) => env.withMetadata(metadata)
              }
            }
            .flatMap { env =>
              val eventOriginFilter: EventEnvelope[_] => Boolean =
                producerSource.replicatedEventOriginFilter
                  .flatMap(f => req.replicaInfo.map(f.createFilter))
                  .getOrElse((_: EventEnvelope[_]) => true)
              if (eventOriginFilter(env)) {
                transformAndEncodeEvent(producerSource.transformation, env, wireSerialization(producerSource))
                  .map {
                    case Some(event) =>
                      log.trace(
                        "Loaded event from persistenceId [{}] with seqNr [{}], offset [{}]",
                        env.persistenceId,
                        env.sequenceNr,
                        env.offset)
                      LoadEventResponse(LoadEventResponse.Message.Event(event))
                    case None =>
                      log.trace(
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
              } else {
                log.trace(
                  "Filtered loaded event, due to origin, from persistenceId [{}] with seqNr [{}], offset [{}], source [{}]",
                  env.persistenceId,
                  env.sequenceNr,
                  env.offset,
                  env.source)
                Future.successful(
                  LoadEventResponse(
                    LoadEventResponse.Message.FilteredEvent(
                      FilteredEvent(
                        env.persistenceId,
                        env.sequenceNr,
                        env.slice,
                        ProtobufProtocolConversions.offsetToProtoOffset(env.offset),
                        source = env.source))))
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

  override def latestEventTimestamp(
      req: LatestEventTimestampRequest,
      metadata: Metadata): Future[LatestEventTimestampResponse] = {
    intercept(req.streamId, metadata).flatMap { _ =>
      val producerSource = streamIdToSourceMap(req.streamId)
      eventsBySlicesPerStreamId(req.streamId) match {
        case q: LatestEventTimestampQuery =>
          import system.executionContext
          q.latestEventTimestamp(producerSource.entityType, req.sliceMin, req.sliceMax).map {
            case Some(instant) => LatestEventTimestampResponse(Some(Timestamp(instant)))
            case None          => LatestEventTimestampResponse.defaultInstance
          }
        case other =>
          Future.failed(
            new UnsupportedOperationException(s"latestEventTimestamp not supported by [${other.getClass.getName}]"))
      }
    }
  }

  override def currentEventsByPersistenceId(
      req: CurrentEventsByPersistenceIdRequest,
      metadata: Metadata): Source[StreamOut, NotUsed] = {
    val futureSource =
      intercept(req.streamId, metadata).map { _ =>
        val producerSource = eventProducerSourceFor(req.streamId)
        val entityTypeFromPid = PersistenceId.extractEntityType(req.persistenceId)
        if (entityTypeFromPid != producerSource.entityType)
          throw new GrpcServiceException(Status.INVALID_ARGUMENT.withDescription(
            s"Persistence id is for a type of entity that is not available for consumption (expected type " +
            s" in persistence id for stream id [${req.streamId}] is [${producerSource.entityType}] but was [$entityTypeFromPid])"))
        currentEventsByPersistenceIdSource(req.streamId, req.persistenceId, req.fromSeqNr, req.toSeqNr)
          .via(transformMetadata(producerSource))
          .via(transformEnvelopeToStreamOut(producerSource, replicaInfo = None))
      }

    Source.futureSource(futureSource).mapMaterializedValue(_ => NotUsed)
  }
}
