/*
 * Copyright (C) 2022-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.BytesEntry
import akka.grpc.scaladsl.SingleResponseRequestBuilder
import akka.grpc.scaladsl.StreamResponseRequestBuilder
import akka.grpc.scaladsl.StringEntry
import akka.persistence.Persistence
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.scaladsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal.withChannelBuilderOverrides
import akka.projection.grpc.internal.ProjectionGrpcSerialization
import akka.projection.grpc.internal.ConnectionException
import akka.projection.grpc.internal.ProtoAnySerialization
import akka.projection.grpc.internal.ProtobufProtocolConversions
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.EventProducerServiceClient
import akka.projection.grpc.internal.proto.EventTimestampRequest
import akka.projection.grpc.internal.proto.FilterReq
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.LoadEventRequest
import akka.projection.grpc.internal.proto.LoadEventResponse
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.ReplayReq
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.internal.CanTriggerReplay
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import akka.util.Timeout
import com.google.protobuf.Descriptors
import com.typesafe.config.Config
import io.grpc.Status
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.projection.grpc.internal.proto.ReplayPersistenceId
import akka.projection.grpc.internal.proto.ReplicaInfo
import akka.projection.grpc.replication.scaladsl.ReplicationSettings

object GrpcReadJournal {
  val Identifier = "akka.projection.grpc.consumer"

  private val log: Logger =
    LoggerFactory.getLogger(classOf[GrpcReadJournal])

  /**
   * Construct a gRPC read journal from configuration `akka.projection.grpc.consumer`. The `stream-id` must
   * be defined in the configuration.
   *
   * Configuration from `akka.projection.grpc.consumer.client` will be used to connect to the remote producer.
   *
   * Note that the `protobufDescriptors` is a list of the `javaDescriptor` for the used protobuf messages. It is
   * defined in the ScalaPB generated `Proto` companion object.
   */
  def apply(protobufDescriptors: immutable.Seq[Descriptors.FileDescriptor])(
      implicit system: ClassicActorSystemProvider): GrpcReadJournal =
    apply(GrpcQuerySettings(system), protobufDescriptors)

  /**
   * Construct a gRPC read journal for the given settings.
   *
   * Configuration from `akka.projection.grpc.consumer.client` will be used to connect to the remote producer.
   *
   * Note that the `protobufDescriptors` is a list of the `javaDescriptor` for the used protobuf messages. It is
   * defined in the ScalaPB generated `Proto` companion object.
   */
  def apply(settings: GrpcQuerySettings, protobufDescriptors: immutable.Seq[Descriptors.FileDescriptor])(
      implicit system: ClassicActorSystemProvider): GrpcReadJournal =
    apply(
      settings,
      GrpcClientSettings.fromConfig(system.classicSystem.settings.config.getConfig(Identifier + ".client")),
      protobufDescriptors)

  /**
   * Construct a gRPC read journal for the given settings and explicit `GrpcClientSettings` to control
   * how to reach the Akka Projection gRPC producer service (host, port etc).
   *
   * Note that the `protobufDescriptors` is a list of the `javaDescriptor` for the used protobuf messages. It is
   * defined in the ScalaPB generated `Proto` companion object.
   */
  def apply(
      settings: GrpcQuerySettings,
      clientSettings: GrpcClientSettings,
      protobufDescriptors: immutable.Seq[Descriptors.FileDescriptor])(
      implicit system: ClassicActorSystemProvider): GrpcReadJournal = {
    val protoAnySerialization =
      new ProtoAnySerialization(system.classicSystem.toTyped, protobufDescriptors, ProtoAnySerialization.Prefer.Scala)
    apply(settings, clientSettings, protoAnySerialization, replicationSettings = None)
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def apply(
      settings: GrpcQuerySettings,
      clientSettings: GrpcClientSettings,
      wireSerialization: ProjectionGrpcSerialization,
      replicationSettings: Option[ReplicationSettings[_]])(
      implicit system: ClassicActorSystemProvider): GrpcReadJournal = {

    // FIXME issue #702 This probably means that one GrpcReadJournal instance is created for each Projection instance,
    // and therefore one grpc client for each. Is that fine or should the client be shared for same clientSettings?

    if (settings.initialConsumerFilter.nonEmpty) {
      ConsumerFilter(system.classicSystem.toTyped).ref ! ConsumerFilter.UpdateFilter(
        settings.streamId,
        settings.initialConsumerFilter)
    }

    new scaladsl.GrpcReadJournal(
      system.classicSystem.asInstanceOf[ExtendedActorSystem],
      settings,
      withChannelBuilderOverrides(clientSettings),
      wireSerialization,
      replicationSettings)
  }

  private def withChannelBuilderOverrides(clientSettings: GrpcClientSettings): GrpcClientSettings = {
    // compose with potential user overrides to allow overriding our defaults
    clientSettings.withChannelBuilderOverrides(channelBuilderOverrides.andThen(clientSettings.channelBuilderOverrides))
  }

  private def channelBuilderOverrides: NettyChannelBuilder => NettyChannelBuilder =
    _.keepAliveWithoutCalls(true)
      .keepAliveTime(10, TimeUnit.SECONDS)
      .keepAliveTimeout(5, TimeUnit.SECONDS)

}

final class GrpcReadJournal private (
    system: ExtendedActorSystem,
    settings: GrpcQuerySettings,
    clientSettings: GrpcClientSettings,
    wireSerialization: ProjectionGrpcSerialization,
    replicationSettings: Option[ReplicationSettings[_]])
    extends ReadJournal
    with EventsBySliceQuery
    with EventTimestampQuery
    with LoadEventQuery
    with CanTriggerReplay {
  import GrpcReadJournal.log
  import ProtobufProtocolConversions._

  // When created as delegate in javadsl from `GrpcReadJournalProvider`.
  private[akka] def this(
      system: ExtendedActorSystem,
      config: Config,
      cfgPath: String,
      protoAnyPrefer: ProtoAnySerialization.Prefer) =
    this(
      system,
      GrpcQuerySettings(config),
      withChannelBuilderOverrides(GrpcClientSettings.fromConfig(config.getConfig("client"))(system)),
      new ProtoAnySerialization(system.toTyped, descriptors = Nil, protoAnyPrefer),
      replicationSettings = None)

  // When created from `GrpcReadJournalProvider`.
  def this(system: ExtendedActorSystem, config: Config, cfgPath: String) =
    this(system, config, cfgPath, ProtoAnySerialization.Prefer.Scala)

  /**
   * Correlation id to be used with [[ConsumerFilter.ReplayWithFilter]].
   * Such replay request will trigger replay in all `eventsBySlices` queries
   * with the same `streamId` running from this instance of the `GrpcReadJournal`.
   * Create separate instances of the `GrpcReadJournal` to have separation between
   * replay requests for the same `streamId`.
   */
  val replayCorrelationId: UUID = UUID.randomUUID()

  private implicit val typedSystem: akka.actor.typed.ActorSystem[_] = system.toTyped
  private val persistenceExt = Persistence(system)

  lazy val consumerFilter: ConsumerFilter = ConsumerFilter(typedSystem)

  private val client = EventProducerServiceClient(clientSettings)
  private val additionalRequestHeaders = settings.additionalRequestMetadata match {
    case Some(meta) => meta.asList
    case None       => Seq.empty
  }

  private val replicaInfo =
    replicationSettings.map(s => ReplicaInfo(s.selfReplicaId.id, s.otherReplicas.toSeq.map(_.replicaId.id)))

  @InternalApi
  private[akka] override def triggerReplay(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long): Unit = {
    consumerFilter.ref ! ConsumerFilter.ReplayWithFilter(
      streamId,
      Set(
        ConsumerFilter
          .ReplayPersistenceId(ConsumerFilter.PersistenceIdOffset(persistenceId, fromSeqNr), triggeredBySeqNr + 1)),
      replayCorrelationId)
  }

  private def addRequestHeaders[Req, Res](
      builder: StreamResponseRequestBuilder[Req, Res]): StreamResponseRequestBuilder[Req, Res] =
    additionalRequestHeaders.foldLeft(builder) {
      case (acc, (key, StringEntry(str)))  => acc.addHeader(key, str)
      case (acc, (key, BytesEntry(bytes))) => acc.addHeader(key, bytes)
    }

  private def addRequestHeaders[Req, Res](
      builder: SingleResponseRequestBuilder[Req, Res]): SingleResponseRequestBuilder[Req, Res] =
    additionalRequestHeaders.foldLeft(builder) {
      case (acc, (key, StringEntry(str)))  => acc.addHeader(key, str)
      case (acc, (key, BytesEntry(bytes))) => acc.addHeader(key, bytes)
    }

  override def sliceForPersistenceId(persistenceId: String): Int =
    persistenceExt.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    persistenceExt.sliceRanges(numberOfRanges)

  /**
   * The identifier of the stream to consume, which is exposed by the producing/publishing side.
   * It is defined in the [[GrpcQuerySettings]].
   */
  def streamId: String =
    settings.streamId

  /**
   * Query events for given slices. A slice is deterministically defined based on the persistence id. The purpose is to
   * evenly distribute all persistence ids over the slices.
   *
   * The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
   * query from a given `offset` after a crash/restart.
   *
   * The supported offset is [[TimestampOffset]] and [[Offset.noOffset]].
   *
   * The timestamp is based on the database `transaction_timestamp()` when the event was stored.
   * `transaction_timestamp()` is the time when the transaction started, not when it was committed. This means that a
   * "later" event may be visible first and when retrieving events after the previously seen timestamp we may miss some
   * events. In distributed SQL databases there can also be clock skews for the database timestamps. For that reason it
   * will perform additional backtracking queries to catch missed events. Events from backtracking will typically be
   * duplicates of previously emitted events. It's the responsibility of the consumer to filter duplicates and make sure
   * that events are processed in exact sequence number order for each persistence id. Such deduplication is provided by
   * the R2DBC Projection.
   *
   * Events emitted by the backtracking don't contain the event payload (`EventBySliceEnvelope.event` is None) and the
   * consumer can load the full `EventBySliceEnvelope` with [[GrpcReadJournal.loadEnvelope]].
   *
   * The events will be emitted in the timestamp order with the caveat of duplicate events as described above. Events
   * with the same timestamp are ordered by sequence number.
   *
   * The stream is not completed when it reaches the end of the currently stored events, but it continues to push new
   * events when new events are persisted.
   */
  override def eventsBySlices[Evt](
      // note that this is actually the producer side defined stream_id
      // not the normal entity type which is internal to the producing side
      streamId: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Evt], NotUsed] = {
    require(streamId == settings.streamId, s"Stream id mismatch, was [$streamId], expected [${settings.streamId}]")
    log.debug(
      "Starting eventsBySlices stream from [{}] [{}], slices [{} - {}], offset [{}]",
      clientSettings.serviceName,
      streamId,
      minSlice,
      maxSlice,
      offset match {
        case t: TimestampOffset => t.timestamp
        case _                  => offset
      })

    def sliceHandledByThisStream(pid: String): Boolean = {
      val slice = persistenceExt.sliceForPersistenceId(pid)
      minSlice <= slice && slice <= maxSlice
    }

    val protoOffset = offsetToProtoOffset(offset)

    def inReqSource(initCriteria: immutable.Seq[ConsumerFilter.FilterCriteria]): Source[StreamIn, NotUsed] =
      Source
        .actorRef[ConsumerFilter.SubscriberCommand](
          completionMatcher = PartialFunction.empty,
          failureMatcher = PartialFunction.empty,
          bufferSize = 1024,
          OverflowStrategy.fail)
        .collect {
          case ConsumerFilter.UpdateFilter(`streamId`, criteria) =>
            val protoCriteria = toProtoFilterCriteria(criteria)
            if (log.isDebugEnabled)
              log.debug("{}: Filter updated [{}]", streamId, criteria.mkString(", "))
            StreamIn(StreamIn.Message.Filter(FilterReq(protoCriteria)))

          case r @ ConsumerFilter.ReplayWithFilter(`streamId`, _) if r.correlationId.forall(_ == replayCorrelationId) =>
            streamInReplay(r)

          case r @ ConsumerFilter.Replay(`streamId`, _) if r.correlationId.forall(_ == replayCorrelationId) =>
            streamInReplay(r.toReplayWithFilter)
        }
        .mapMaterializedValue { ref =>
          consumerFilter.ref ! ConsumerFilter.Subscribe(streamId, initCriteria, ref)
          NotUsed
        }

    def streamInReplay(r: ConsumerFilter.ReplayWithFilter): StreamIn = {
      val protoReplayPersistenceIds = r.replayPersistenceIds.collect {
        case ConsumerFilter.ReplayPersistenceId(ConsumerFilter.PersistenceIdOffset(pid, seqNr), filterAfterSeqNr)
            if sliceHandledByThisStream(pid) =>
          ReplayPersistenceId(Some(PersistenceIdSeqNr(pid, seqNr)), filterAfterSeqNr)
      }.toVector

      // need this for compatibility with 1.6.0
      val protoPersistenceIdOffsets = protoReplayPersistenceIds.map(_.fromPersistenceIdOffset.get)

      if (log.isDebugEnabled() && protoReplayPersistenceIds.nonEmpty)
        log.debug(
          "{}: Replay triggered for [{}]",
          streamId,
          protoReplayPersistenceIds
            .map { replayPersistenceId =>
              val offset = replayPersistenceId.fromPersistenceIdOffset.get
              s"${offset.persistenceId} -> ${offset.seqNr} (${replayPersistenceId.filterAfterSeqNr})"
            }
            .mkString(", "))

      StreamIn(StreamIn.Message.Replay(ReplayReq(protoPersistenceIdOffsets, protoReplayPersistenceIds)))
    }

    val initFilter = {
      import akka.actor.typed.scaladsl.AskPattern._

      import scala.concurrent.duration._
      implicit val askTimeout: Timeout = 10.seconds
      consumerFilter.ref.ask[ConsumerFilter.CurrentFilter](ConsumerFilter.GetFilter(streamId, _))
    }

    val streamIn: Source[StreamIn, NotUsed] = {
      implicit val ec: ExecutionContext = typedSystem.executionContext
      Source
        .futureSource {
          initFilter.map { filter =>
            val protoCriteria = toProtoFilterCriteria(filter.criteria)
            val initReq = InitReq(streamId, minSlice, maxSlice, protoOffset, protoCriteria, replicaInfo)

            Source
              .single(StreamIn(StreamIn.Message.Init(initReq)))
              .concat(inReqSource(filter.criteria))
          }
        }
        .mapMaterializedValue(_ => NotUsed)
    }

    val streamOut: Source[StreamOut, NotUsed] =
      addRequestHeaders(client.eventsBySlices())
        .invoke(streamIn)
        .recover {
          case ex: akka.grpc.GrpcServiceException if ex.status.getCode == Status.Code.UNAVAILABLE =>
            // this means we couldn't connect, will be retried, relatively common, so make it less noisy
            throw new ConnectionException(
              clientSettings.serviceName,
              clientSettings.servicePortName.getOrElse(clientSettings.defaultPort.toString),
              streamId)

          case th: Throwable =>
            throw new RuntimeException(s"Failure to consume gRPC event stream for [${streamId}]", th)
        }

    streamOut.map {
      case StreamOut(StreamOut.Message.Event(event), _) =>
        if (log.isTraceEnabled)
          log.trace(
            "Received event from [{}] persistenceId [{}] with seqNr [{}], offset [{}], source [{}]",
            clientSettings.serviceName,
            event.persistenceId,
            event.seqNr,
            timestampOffset(event.offset.head).timestamp,
            event.source)

        eventToEnvelope(event, streamId)

      case StreamOut(StreamOut.Message.FilteredEvent(filteredEvent), _) =>
        if (log.isTraceEnabled)
          log.trace(
            "Received filtered event from [{}] persistenceId [{}] with seqNr [{}], offset [{}], source [{}]",
            clientSettings.serviceName,
            filteredEvent.persistenceId,
            filteredEvent.seqNr,
            timestampOffset(filteredEvent.offset.head).timestamp,
            filteredEvent.source)

        filteredEventToEnvelope(filteredEvent, streamId)

      case other =>
        throw new IllegalArgumentException(s"Unexpected StreamOut [${other.message.getClass.getName}]")
    }
  }

  private def eventToEnvelope[Evt](
      event: Event,
      // note that this is actually the producer side defined stream_id
      // not the normal entity type which is internal to the producing side
      streamId: String): EventEnvelope[Evt] = {
    require(streamId == settings.streamId, s"Stream id mismatch, was [$streamId], expected [${settings.streamId}]")
    ProtobufProtocolConversions.eventToEnvelope(event, wireSerialization)
  }

  private def filteredEventToEnvelope[Evt](filteredEvent: FilteredEvent, entityType: String): EventEnvelope[Evt] = {
    val eventOffset = timestampOffset(filteredEvent.offset.head)
    new EventEnvelope(
      eventOffset,
      filteredEvent.persistenceId,
      filteredEvent.seqNr,
      None,
      eventOffset.timestamp.toEpochMilli,
      eventMetadata = None,
      entityType,
      filteredEvent.slice,
      filtered = true,
      source = filteredEvent.source)
  }

  private def timestampOffset(protoOffset: akka.projection.grpc.internal.proto.Offset): TimestampOffset = {
    val timestamp = protoOffset.timestamp.get.asJavaInstant
    val seen = protoOffset.seen.map {
      case PersistenceIdSeqNr(pid, seqNr, _) =>
        pid -> seqNr
    }.toMap
    TimestampOffset(timestamp, seen)
  }

  // EventTimestampQuery
  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    import system.dispatcher
    addRequestHeaders(client.eventTimestamp())
      .invoke(EventTimestampRequest(settings.streamId, persistenceId, sequenceNr))
      .map(_.timestamp.map(_.asJavaInstant))
  }

  //LoadEventQuery
  override def loadEnvelope[Evt](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Evt]] = {
    log.trace(
      "Loading event from [{}] persistenceId [{}] with seqNr [{}]",
      clientSettings.serviceName,
      persistenceId,
      sequenceNr)
    import system.dispatcher
    addRequestHeaders(client.loadEvent())
      .invoke(LoadEventRequest(settings.streamId, persistenceId, sequenceNr, replicaInfo))
      .map {
        case LoadEventResponse(LoadEventResponse.Message.Event(event), _) =>
          eventToEnvelope(event, settings.streamId)

        case LoadEventResponse(LoadEventResponse.Message.FilteredEvent(filteredEvent), _) =>
          filteredEventToEnvelope(filteredEvent, settings.streamId)

        case other =>
          throw new IllegalArgumentException(s"Unexpected LoadEventResponse [${other.message.getClass.getName}]")

      }
  }

  /**
   * Close the gRPC client. It will be automatically closed when the `ActorSystem` is terminated,
   * so invoking this is only needed when there is a need to close the resource before that.
   * After closing the `GrpcReadJournal` instance cannot be used again.
   */
  def close(): Future[Done] =
    client.close()

}
