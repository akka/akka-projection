/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.producer.javadsl;

import akka.actor.typed.ActorSystem;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ProducerCompileTest {
  public static void start(ActorSystem<?> system) {
    Transformation asyncTransformation =
        Transformation.empty()
            .registerAsyncMapper(Integer.class, event -> CompletableFuture.completedFuture(Optional.of(Integer.valueOf(event * 2).toString())))
            .registerAsyncOrElseMapper(event -> CompletableFuture.completedFuture(Optional.of(event.toString())));
    Transformation transformation =
        Transformation.empty()
            .registerMapper(Integer.class, event -> Optional.of(Integer.valueOf(event * 2).toString()))
            .registerOrElseMapper(event -> Optional.of(event.toString()));

    Function<HttpRequest, CompletionStage<HttpResponse>> eventProducerService = EventProducer.grpcServiceHandler(system, transformation);

    Function<HttpRequest, CompletionStage<HttpResponse>> service =
        ServiceHandler.concatOrNotFound(eventProducerService);

    CompletionStage<ServerBinding> bound =
        Http.get(system).newServerAt("127.0.0.1", 8080).bind(service);

  }
}
