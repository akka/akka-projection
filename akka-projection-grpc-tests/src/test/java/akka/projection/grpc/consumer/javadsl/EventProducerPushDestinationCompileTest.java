/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.javadsl;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;
import akka.actor.typed.SpawnProtocol;
import akka.grpc.javadsl.Metadata;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal;
import akka.projection.ProjectionBehavior;
import akka.projection.ProjectionId;
import akka.projection.eventsourced.javadsl.EventSourcedProvider;
import akka.projection.grpc.consumer.ConsumerFilter;
import akka.projection.javadsl.Handler;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class EventProducerPushDestinationCompileTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventProducerPushDestinationCompileTest.class);

  // just an empty dummy list for documentation
  private static List<Descriptors.FileDescriptor> protoDescriptors = Collections.emptyList();

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
    // #consumerSetup
    EventProducerPushDestination destination = EventProducerPushDestination.create("stream-id", protoDescriptors, system);
    CompletionStage<ServerBinding> bound = Http.get(system)
        .newServerAt("127.0.0.1", 8080)
        .bind(EventProducerPushDestination.grpcServiceHandler(destination, system));
    bound.thenAccept(binding ->
        LOGGER.info("Consumer listening at: {}:{}", binding.localAddress().getHostString(), binding.localAddress().getPort()));
    // #consumerSetup
  }

  public static void withFilters(ActorSystem<?> system) {
    // #consumerFilters
    EventProducerPushDestination destination =
        EventProducerPushDestination.create("stream-id", protoDescriptors, system)
            .withConsumerFilters(
                Collections.singletonList(new ConsumerFilter.IncludeTopics(Collections.singleton("myhome/groundfloor/+/temperature")))
            );
      // #consumerFilters
  }

  public static void withTransformations(ActorSystem<?> system) {
    // #consumerTransformation
    EventProducerPushDestination destination =
      EventProducerPushDestination.create("stream-id", protoDescriptors, system)
        .withTransformationForOrigin((String originId, Metadata metadata) ->
            Transformation.empty()
              .registerPersistenceIdMapper(system, envelope ->
                  envelope.persistenceId().replace("originalPrefix", "newPrefix"))
              .registerTagMapper(String.class, envelope -> {
                  Set<String> newTags = new HashSet<>();
                  newTags.addAll(envelope.getTags());
                  newTags.add("origin-" + originId);
                  return newTags;
            }));
    // #consumerTransformation
  }
}
