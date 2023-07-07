/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.consumer.javadsl;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.SpawnProtocol;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.persistence.query.Offset;

import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import akka.projection.ProjectionId;
import akka.projection.ProjectionBehavior;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.producer.javadsl.EventProducerPush;
import akka.projection.grpc.producer.javadsl.EventProducerSource;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class EventProducerPushDestinationCompileTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventProducerPushDestinationCompileTest.class);

  public static void initConsumer(ActorSystem<SpawnProtocol.Command> system) {

    // projection consuming is not really part of API but for completeness
    SourceProvider<Offset, EventEnvelope<String>> consumerProjectionProvider =
        EventSourcedProvider.eventsBySlices(system, R2dbcReadJournal.Identifier(), "enitityTypeName", 0, 1023);

    ProjectionId consumerProjectionId = ProjectionId.of("fruit-consumer", "0-1023");
    final Handler<EventEnvelope<String>> handler = new Handler<EventEnvelope<String>>() {
      @Override
      public CompletionStage<Done> process(EventEnvelope<String> envelope) throws Exception {
        LOGGER.info(
            "Saw projected event: {}-{}: {}",
            envelope.persistenceId(),
            envelope.sequenceNr(),
            envelope.getOptionalEvent().orElse("filtered"));
        return CompletableFuture.completedFuture(Done.done());
      }
    };
    system.tell(new SpawnProtocol.Spawn<ProjectionBehavior.Command>(ProjectionBehavior.create(
        R2dbcProjection.atLeastOnceAsync(
            consumerProjectionId,
            Optional.empty(),
            consumerProjectionProvider,
            () -> handler,
            system)
        ),
        "projection-consumer",
        Props.empty(),
        system.ignoreRef().narrow()
    ));

    // this is the API, consumer runs gRPC server accepting pushed events from producers
    EventProducerPushDestination destination = EventProducerPushDestination.create("stream-id", system);
    CompletionStage<ServerBinding> bound = Http.get(system)
        .newServerAt("127.0.0.1", 8080)
        .bind(EventProducerPushDestination.grpcServiceHandler(destination, system));
    bound.thenAccept(binding ->
        LOGGER.info("Consumer listening at: {}:{}", binding.localAddress().getHostString(), binding.localAddress().getPort()));
  }
}
