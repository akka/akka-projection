/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javdsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.grpc.GrpcClientSettings;
import akka.grpc.javadsl.ServiceHandler;
import akka.http.javadsl.Http;
import akka.http.javadsl.ServerBinding;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.japi.function.Function;
import akka.persistence.query.Offset;
import akka.persistence.query.typed.EventEnvelope;
import akka.persistence.typed.ReplicaId;
import akka.projection.ProjectionContext;
import akka.projection.ProjectionId;
import akka.projection.grpc.producer.EventProducerSettings;
import akka.projection.grpc.producer.javadsl.EventProducer;
import akka.projection.grpc.producer.javadsl.EventProducerSource;
import akka.projection.grpc.replication.javadsl.Replica;
import akka.projection.grpc.replication.javadsl.ReplicatedBehaviors;
import akka.projection.grpc.replication.javadsl.Replication;
import akka.projection.grpc.replication.javadsl.ReplicationProjectionProvider;
import akka.projection.grpc.replication.javadsl.ReplicationSettings;
import akka.projection.javadsl.SourceProvider;
import akka.projection.r2dbc.R2dbcProjectionSettings;
import akka.projection.r2dbc.javadsl.R2dbcProjection;
import akka.projection.javadsl.AtLeastOnceFlowProjection;
import akka.stream.javadsl.FlowWithContext;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;

public class ReplicationCompileTest {
  interface MyCommand {
  }

  static class MyReplicatedBehavior {


    static Behavior<MyCommand> create(
        ReplicatedBehaviors<MyCommand, Void, Void> replicatedBehaviors) {
      return replicatedBehaviors.setup(
          replicationContext -> {
            throw new UnsupportedOperationException("just a dummy factory method");
          });
    }
  }

  public static void start(ActorSystem<?> system) {
   Set<Replica> otherReplicas = new HashSet<>();
   otherReplicas.add(Replica.create(
       new ReplicaId("DCB"),
       2,
       GrpcClientSettings.connectToServiceAt("b.example.com", 443, system).withTls(true)
       ));
   otherReplicas.add(Replica.create(
       new ReplicaId("DCC"),
       2,
       GrpcClientSettings.connectToServiceAt("c.example.com", 443, system).withTls(true)
   ));

   ReplicationProjectionProvider projectionProvider = new ReplicationProjectionProvider() {

    @Override
    public AtLeastOnceFlowProjection<Offset, EventEnvelope<Object>> create(ProjectionId projectionId, SourceProvider<Offset, EventEnvelope<Object>> sourceProvider, FlowWithContext<EventEnvelope<Object>, ProjectionContext, Done, ProjectionContext, NotUsed> replicationFlow, ActorSystem<?> system) {
     return  R2dbcProjection.atLeastOnceFlow(projectionId, Optional.<R2dbcProjectionSettings>empty(), sourceProvider, replicationFlow, system);
    }
   };

   // SAM so this is possible
   ReplicationProjectionProvider projectionProvider2 = (projectionId, sourceProvider, flow, s) ->
           R2dbcProjection.atLeastOnceFlow(projectionId, Optional.empty(), sourceProvider, flow, s);

   ReplicationSettings<MyCommand> settings = ReplicationSettings.<MyCommand>create(
       MyCommand.class,
       "my-entity",
       ReplicaId.apply("DCA"),
       EventProducerSettings.apply(system),
       otherReplicas,
       Duration.ofSeconds(10),
       // parallel updates
       8,
       projectionProvider).configureEntity(entity -> entity.withRole("entities"));

   Replication<MyCommand> replication = Replication.grpcReplication(settings, MyReplicatedBehavior::create, system);

   // bind a single handler endpoint
   Function<HttpRequest, CompletionStage<HttpResponse>> handler = replication.createSingleServiceHandler();

   @SuppressWarnings("unchecked")
   Function<HttpRequest, CompletionStage<HttpResponse>> service =
       ServiceHandler.concatOrNotFound(handler);

   CompletionStage<ServerBinding> bound =
       Http.get(system).newServerAt("127.0.0.1", 8080).bind(service);

  }

  static class ShoppingCart {
    static Replication<MyCommand> init(ActorSystem<?> system) {
      throw new UnsupportedOperationException("Just a sample");
    }
  }

  public static void multiEventProducers(ActorSystem<?> system, ReplicationSettings<MyCommand> settings, String host, int port) {

    Replication<Void> otherReplication = null;

    // #multi-service
    Set<EventProducerSource> allSources = new HashSet<>();

    Replication<MyCommand> replication = ShoppingCart.init(system);
    allSources.add(replication.eventProducerService());

    // add additional EventProducerSource from other entities or
    // Akka Projection gRPC
    allSources.add(otherReplication.eventProducerService());

    Function<HttpRequest, CompletionStage<HttpResponse>> route =
        EventProducer.grpcServiceHandler(system, allSources);

    @SuppressWarnings("unchecked")
    Function<HttpRequest, CompletionStage<HttpResponse>> handler =
        ServiceHandler.concatOrNotFound(route);
    // #multi-service

    CompletionStage<ServerBinding> bound =
        Http.get(system).newServerAt(host, port).bind(handler);

  }
}
