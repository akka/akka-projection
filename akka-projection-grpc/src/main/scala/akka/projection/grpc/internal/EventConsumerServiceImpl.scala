/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.LoggerOps
import akka.grpc.scaladsl.Metadata
import akka.persistence.EventWriter
import akka.persistence.FilteredPayload
import akka.projection.grpc.consumer.scaladsl.EventConsumerInterceptor
import akka.projection.grpc.internal.proto.ConsumeEventIn
import akka.projection.grpc.internal.proto.ConsumeEventOut
import akka.projection.grpc.internal.proto.ConsumerEventAck
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApi
import akka.stream.scaladsl.Source
import akka.util.ByteString
import akka.util.Timeout
import org.slf4j.LoggerFactory

import java.net.URLEncoder
import scala.concurrent.ExecutionContext
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
      persistenceIdTransformer: String => String,
      interceptor: Option[EventConsumerInterceptor])(implicit system: ActorSystem[_]): EventConsumerServiceImpl = {
    // FIXME is this name unique, could we create multiple for the same journal? (we wouldn't be able to bind them to the same port)
    val eventWriter = system.systemActorOf(
      EventWriter(journalPluginId.getOrElse("")),
      s"EventWriter-${URLEncoder.encode(journalPluginId.getOrElse("default"), ByteString.UTF_8)}")

    new EventConsumerServiceImpl(eventWriter, acceptedStreamIds, persistenceIdTransformer, interceptor)
  }

}

/**
 * INTERNAL API
 */
private[akka] final class EventConsumerServiceImpl(
    eventWriter: ActorRef[EventWriter.Command],
    acceptedStreamIds: Set[String],
    persistenceIdTransformer: String => String,
    interceptor: Option[EventConsumerInterceptor])(implicit system: ActorSystem[_])
    extends EventConsumerServicePowerApi {

  private val logger = LoggerFactory.getLogger(classOf[EventConsumerServiceImpl])

  private val protoAnySerialization = new ProtoAnySerialization(system)
  private implicit val ec: ExecutionContext = system.executionContext
  private implicit val timeout: Timeout = 5.seconds // FIXME from config or can we get rid of it

  override def consumeEvent(
      in: Source[ConsumeEventIn, NotUsed],
      metadata: Metadata): Source[ConsumeEventOut, NotUsed] = {
    in.prefixAndTail(1)
      .flatMapConcat {
        case (Seq(ConsumeEventIn(ConsumeEventIn.Message.Init(init), _)), tail) =>
          if (!acceptedStreamIds(init.streamId))
            throw new IllegalArgumentException(s"Events for stream id [${init.streamId}] not accepted by this consumer")

          val eventsAndFiltered = tail.collect {
            case c if c.message.isEvent || c.message.isFilteredEvent => c
            // keepalive consumed and dropped here
          }

          // allow interceptor to block request based on metadata
          interceptor match {
            case Some(interceptor) =>
              Source.futureSource(interceptor.intercept(init.streamId, metadata).map { _ =>
                logger.info("Event stream from [{}] started", init.originId)
                eventsAndFiltered
              })
            case None =>
              logger.info("Event stream from [{}] started", init.originId)
              eventsAndFiltered
          }

        case (_, _) =>
          throw new IllegalArgumentException(
            "Expected stream in starts with Init event followed by events but got something else")
      }
      .map { consumeEventIn =>
        if (consumeEventIn.message.isEvent)
          // FIXME would we want request metadata/producer ip/entire envelope in to persistence id transformer?
          ProtobufProtocolConversions.eventToEnvelope[Any](consumeEventIn.getEvent, protoAnySerialization)
        else if (consumeEventIn.message.isFilteredEvent) {
          ProtobufProtocolConversions.filteredEventToEnvelope[Any](consumeEventIn.getFilteredEvent)
        } else {
          throw new IllegalArgumentException(s"Unexpected type of ConsumeEventIn: ${consumeEventIn.message.getClass}")
        }
      }
      // FIXME config for parallelism, and perPartition (aligned with event writer batch config)
      .mapAsyncPartitioned(1000, 20)(_.persistenceId) { (envelope, _) =>
        val persistenceId = persistenceIdTransformer(envelope.persistenceId)
        if (logger.isTraceEnabled)
          logger.traceN(
            "Saw event [{}] for pid [{}]{}",
            envelope.sequenceNr,
            envelope.persistenceId,
            if (envelope.filtered) " filtered" else "")

        eventWriter
          .askWithStatus[EventWriter.WriteAck](
            EventWriter.Write(
              persistenceId,
              envelope.sequenceNr,
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
