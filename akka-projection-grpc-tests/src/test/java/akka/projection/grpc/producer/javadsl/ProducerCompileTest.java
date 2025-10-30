/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.producer.javadsl;

import akka.actor.typed.ActorSystem;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.persistence.query.typed.EventEnvelope;
import akka.projection.grpc.producer.EventProducerSettings;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ProducerCompileTest {
  public static void start(ActorSystem<?> system) {
    Transformation asyncTransformation =
        Transformation.empty()
            .registerAsyncMapper(
                Integer.class,
                event ->
                    CompletableFuture.completedFuture(
                        Optional.of(Integer.valueOf(event * 2).toString())))
            .registerAsyncOrElseMapper(
                event -> CompletableFuture.completedFuture(Optional.of(event.toString())));
    Transformation transformation =
        Transformation.empty()
            .registerMapper(
                Integer.class, event -> Optional.of(Integer.valueOf(event * 2).toString()))
            .registerEnvelopeMapper(Long.class, envelope -> Optional.of(envelope.event() + 1L))
            .registerOrElseMapper(event -> Optional.of(event.toString()));
    Transformation lowLevel =
        Transformation.empty()
            .registerAsyncEnvelopeMapper(
                Integer.class,
                envelope -> CompletableFuture.completedFuture(envelope.getOptionalEvent()))
            .registerAsyncEnvelopeOrElseMapper(
                envelope -> CompletableFuture.completedFuture(Optional.empty()));

    EventProducerSource source =
        new EventProducerSource(
                "ShoppingCart", "cart", transformation, EventProducerSettings.create(system))
            .withProducerFilter((EventEnvelope<Integer> env) -> env.event().doubleValue() > 0.0);

    EventProducerSource sourceStartingFromSnapshots =
        new EventProducerSource(
                "ShoppingCart", "cart-snap", transformation, EventProducerSettings.create(system))
            .withProducerFilter((EventEnvelope<Integer> env) -> env.event().doubleValue() > 0.0)
            .withStartingFromSnapshots((String snap) -> Integer.valueOf(snap));

    Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerService =
        EventProducer.grpcServiceHandler(system, source);
    Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerServiceWithMultiple =
        EventProducer.grpcServiceHandler(
            system, new HashSet<>(Arrays.asList(source, sourceStartingFromSnapshots)));

    @SuppressWarnings("unchecked")
    Function<HttpRequest, CompletionStage<HttpResponse>> service =
        ServiceHandler.concatOrNotFound(eventProducerService);

    CompletionStage<ServerBinding> bound =
        Http.get(system).newServerAt("127.0.0.1", 8080).bind(service);
  }
}
