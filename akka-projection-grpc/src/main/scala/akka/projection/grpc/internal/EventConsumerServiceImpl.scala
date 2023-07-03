/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.LoggerOps
import akka.persistence.EventWriter
import akka.projection.grpc.internal.proto.ConsumeEventIn
import akka.projection.grpc.internal.proto.ConsumeEventOut
import akka.projection.grpc.internal.proto.ConsumerEventAck
import akka.projection.grpc.internal.proto.EventConsumerService
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.util.Timeout
import org.slf4j.LoggerFactory

import java.net.URLEncoder
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

/**
 * INTERNAL API
 *
 * gRPC push protocol service for the consuming side
 */
private[akka] object EventConsumerServiceImpl {

  /**
   *
   * @param journalPluginId empty to use default
   */
  def directJournalConsumer(
      journalPluginId: Option[String],
      acceptedStreamIds: Set[String],
      persistenceIdTransformer: String => String)(implicit system: ActorSystem[_]): EventConsumerServiceImpl = {
    // FIXME is this name unique, could we create multiple for the same journal? (we wouldn't be able to bind them to the same port)
    val eventWriter = system.systemActorOf(
      EventWriter(journalPluginId.getOrElse("")),
      s"EventWriter-${URLEncoder.encode(journalPluginId.getOrElse("default"), ByteString.UTF_8)}")

    new EventConsumerServiceImpl(eventWriter, acceptedStreamIds, persistenceIdTransformer)
  }

}

/**
 * INTERNAL API
 */
private[akka] final class EventConsumerServiceImpl(
    eventWriter: ActorRef[EventWriter.Command],
    acceptedStreamIds: Set[String],
    persistenceIdTransformer: String => String)(implicit system: ActorSystem[_])
    extends EventConsumerService {

  private val logger = LoggerFactory.getLogger(classOf[EventConsumerServiceImpl])

  private val protoAnySerialization = new ProtoAnySerialization(system)

  private implicit val timeout: Timeout = 5.seconds // FIXME from config or can we get rid of it

  override def consumeEvent(in: Source[ConsumeEventIn, NotUsed]): Source[ConsumeEventOut, NotUsed] = {
    in.prefixAndTail(1)
      .flatMapConcat {
        case (Seq(ConsumeEventIn(ConsumeEventIn.Message.Init(init), _)), tail) =>
          if (!acceptedStreamIds(init.streamId))
            throw new IllegalArgumentException(s"Events for stream id [${init.streamId}] not accepted by this consumer")
          logger.info("Event stream from [{}] started", init.originId)
          tail.collect {
            case ConsumeEventIn(ConsumeEventIn.Message.Event(event), _) => event
            // keepalive consumed and dropped here
          }
        case (_, _) =>
          throw new IllegalArgumentException(
            "Expected stream in starts with Init event followed by events but got something else")
      }
      // FIXME config for parallelism, and perPartition (aligned with event writer batch config)
      .mapAsyncPartitioned(1000, 20)(_.persistenceId) { (in, _) =>
        // FIXME would we want request metadata/producer ip/entire envelope in to persistence id transformer?
        val envelope = ProtobufProtocolConversions.eventToEnvelope[Any](in, protoAnySerialization)
        val persistenceId = persistenceIdTransformer(envelope.persistenceId)
        if (logger.isTraceEnabled)
          logger.traceN(
            "Saw event [{}] for pid [{}]{}",
            envelope.sequenceNr,
            envelope.persistenceId,
            if (envelope.filtered) " filtered" else "")

        eventWriter
          .askWithStatus[EventWriter.WriteAck](EventWriter.Write(
            persistenceId,
            envelope.sequenceNr,
            // FIXME how to deal with filtered - can't be null, should we have a marker filtered payload?
            envelope.eventOption.getOrElse(FilteredPayload),
            envelope.eventMetadata,
            _))
          .recover {
            case NonFatal(ex) =>
              logger.warn(s"Failing event stream because of event writer error", ex)
              throw ex;
          }(system.executionContext)
      }
      .map(ack => ConsumeEventOut(ConsumeEventOut.Message.Ack(ConsumerEventAck(ack.persistenceId, ack.sequenceNumber))))
  }

}
