/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.projection.grpc.internal.proto.ConsumerStreamIn
import akka.projection.grpc.internal.proto.ConsumerStreamOut
import akka.projection.grpc.internal.proto.ControlCommand
import akka.projection.grpc.internal.proto.ControlStreamRequest
import akka.projection.grpc.internal.proto.EventConsumerService
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 *
 * Optionally runs on the consuming side, accepting connections from a downstream gRPC projection event producer.
 */
// FIXME where/who/what binds this
// FIXME connect to SDP running the projection somehow?
// FIXME how/where do we configure producer to connect
@InternalApi private[akka] class EventConsumerServiceImpl extends EventConsumerService {

  private val logger = LoggerFactory.getLogger(classOf[EventProducerServiceImpl])

  override def control(in: ControlStreamRequest): Source[ControlCommand, NotUsed] = {
    logger.info("Reverse gRPC projection connection from [{}]", in.producerIdentifier)
    // FIXME where does this stream come from? an SPD on some node?
    Source.never[ControlCommand]
  }

  override def eventStream(in: Source[ConsumerStreamIn, NotUsed]): Source[ConsumerStreamOut, NotUsed] = {
    // FIXME actually attach a running consumer, passing these events to the one requesting
    //       the stream.
    in.via(Flow.fromSinkAndSource(Sink.foreach(in => logger.info("In {}", in)), Source.never))
  }
}
